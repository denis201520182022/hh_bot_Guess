# worker_scheduler.py
import asyncio
import logging
import datetime
import signal
from sqlalchemy import select, and_
from app.db.models import Dialogue, AnalyticsEvent
from zoneinfo import ZoneInfo
from app.connectors.avito.avito_search import avito_search_service
from app.connectors.hh.hh_search import hh_search_service
from app.core.config import settings
from app.core.rabbitmq import mq
from app.db.session import AsyncSessionLocal, engine
from app.db.models import JobContext
from app.db.models import Dialogue, InterviewReminder
from app.services.knowledge_base import kb_service
from sqlalchemy.orm import selectinload
from app.services.google_sync_search_avito import google_sync_search_avito_service
from app.services.google_sync_search_hh import google_sync_search_hh_service
from app.utils.logger import logger, set_log_context, log_context
from app.utils.analytics import log_event
from app.db.models import Account, JobContext, Candidate, Dialogue, AppSettings

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

class Scheduler:
    def __init__(self):
        self.is_running = True

    async def start(self):
        logger.info("🚀 [worker_scheduler] запущен")
        await mq.connect()

        # Запускаем параллельные циклы
        await asyncio.gather(
            self._loop_silence_reminders(),      # Напоминания молчунам
            self._loop_interview_reminders(),    # Напоминания перед собесом
            self._loop_kb_refresh(),             # Обновление промпта (раз в 3 мин)
            self._loop_outbound_search(),        # Активный поиск кандидатов (Avito + HH)
            self._loop_google_search_sync_avito(),  # Синхронизация поиска Avito
            self._loop_google_search_sync_hh()      # Синхронизация поиска HH
        )

    async def stop(self):
        logger.info("🛑 Остановка планировщика...")
        self.is_running = False

    
    # --- 1. ЛОГИКА МОЛЧУНОВ ---
    async def _loop_silence_reminders(self):
        """Проверка кандидатов, которые замолчали (с учетом часовых поясов и тихого часа)"""
        
        while self.is_running:
            try:
                if not settings.reminders.silence.enabled:
                    await asyncio.sleep(60)
                    continue

                qt_cfg = settings.reminders.silence.quiet_time
                max_levels = len(settings.reminders.silence.levels)

                async with AsyncSessionLocal() as db:
                    now_utc = datetime.datetime.now(datetime.timezone.utc)
                
                    # 1. МЕНЯЕМ ЗАПРОС: добавляем .vacancy в selectinload
                    stmt = (
                        select(Dialogue)
                        .join(JobContext)
                        .options(
                            selectinload(Dialogue.candidate),
                            selectinload(Dialogue.vacancy)  # <--- ДОБАВИТЬ ЭТУ СТРОКУ
                        )
                        .where(
                            and_(
                                Dialogue.status == 'in_progress',
                                Dialogue.reminder_level <= max_levels,
                                JobContext.is_active == True
                            )
                        )
                    )
                    result = await db.execute(stmt)
                    dialogues = result.scalars().all()

                    for dialogue in dialogues:
                        log_context.set({})
                        set_log_context(dialogue_id=dialogue.id, task="silence_reminder")
                        
                        # --- А. ЧАСОВОЙ ПОЯС И ТИХИЙ ЧАС ---
                        profile = dialogue.candidate.profile_data or {}
                        tz_name = profile.get("timezone", qt_cfg.default_timezone)
                        try:
                            candidate_tz = ZoneInfo(tz_name)
                        except Exception:
                            candidate_tz = ZoneInfo(qt_cfg.default_timezone)

                        if qt_cfg.enabled:
                            now_candidate = datetime.datetime.now(candidate_tz).time()
                            start_q = datetime.datetime.strptime(qt_cfg.start, "%H:%M").time()
                            end_q = datetime.datetime.strptime(qt_cfg.end, "%H:%M").time()
                            is_quiet = False
                            if start_q > end_q:
                                if now_candidate >= start_q or now_candidate <= end_q: is_quiet = True
                            else:
                                if start_q <= now_candidate <= end_q: is_quiet = True
                            
                            if is_quiet: continue

                        # --- Б. ПРОВЕРКА ВРЕМЕНИ МОЛЧАНИЯ ---
                        if not dialogue.history or dialogue.history[-1].get("role") != "assistant":
                            continue
                        
                        last_ts = dialogue.last_message_at.replace(tzinfo=datetime.timezone.utc)
                        silence_minutes = (now_utc - last_ts).total_seconds() / 60
                        
                        current_level = dialogue.reminder_level

                        # --- В. ЛОГИКА ПЕРЕКЛЮЧЕНИЯ ---
                        
                        # ВАРИАНТ 1: Еще есть напоминания в запасе
                        if current_level < max_levels:
                            reminder_cfg = settings.reminders.silence.levels[current_level]
                            
                            if silence_minutes >= reminder_cfg.delay_minutes:
                                new_level = current_level + 1
                                logger.info(f"⏰ Напоминание ур.{new_level} для диалога {dialogue.id}")
                                
                                await mq.publish("engine_tasks", {
                                    "dialogue_id": dialogue.id,
                                    "trigger": "reminder",
                                    "reminder_text": reminder_cfg.text,
                                    "new_level": new_level,
                                    "stop_bot": False # Мы не стопаем тут, стоп будет позже
                                })
                                dialogue.reminder_level = new_level

                        # ВАРИАНТ 2: Все напоминания отправлены, ждем финальные 30 минут
                        elif current_level == max_levels:
                            if silence_minutes >= 30: # <--- Тот самый порог 30 минут
                                logger.info(f"💤 Диалог {dialogue.id} официально признан молчуном (30 мин после финала)")
                                
                                dialogue.status = 'timed_out'
                                # Ставим уровень выше максимума, чтобы запрос его больше не цеплял
                                dialogue.reminder_level = max_levels + 1 
                                
                                # Пишем в аналитику
                                await log_event(
                                    db=db,
                                    dialogue=dialogue,
                                    event_type='timed_out',
                                    event_data={"reason": "final_timeout_30min", "total_reminders": max_levels}
                                )

                    await db.commit()

            except Exception as e:
                error_msg = f"❌ Ошибка в цикле молчунов [worker_scheduler]:\n{str(e)}"
                logger.error(error_msg, exc_info=True)
                logger.exception("❌ Ошибка в цикле молчунов")
                try:
                    await mq.publish("tg_alerts", {"type": "system", "text": error_msg, "alert_type": "admin_only"})
                except: pass
            
            
            await asyncio.sleep(30)

    # --- 2. НАПОМИНАНИЯ ПЕРЕД СОБЕСЕДОВАНИЕМ ---
    async def _loop_interview_reminders(self):
        """Проверка таблицы InterviewReminder и отправка напоминаний"""
        from sqlalchemy.orm import selectinload # Убедись, что этот импорт есть в начале файла
        
        while self.is_running:
            try:
                # 1. Проверяем, включены ли напоминания в конфиге
                if not settings.reminders.interview.enabled:
                    await asyncio.sleep(30)
                    continue

                async with AsyncSessionLocal() as db:
                    now_utc = datetime.datetime.now(datetime.timezone.utc)
                    
                    # Загружаем напоминания, которые пора отправить
                    stmt = (
                        select(InterviewReminder)
                        .join(Dialogue)    # Соединяем с диалогами
                        .join(JobContext)  # Соединяем с вакансиями
                        .options(
                            selectinload(InterviewReminder.dialogue)
                            .selectinload(Dialogue.vacancy)
                        )
                        .where(
                            and_(
                                InterviewReminder.status == 'pending',
                                InterviewReminder.scheduled_at <= now_utc,
                                JobContext.is_active == True # <--- ДОБАВЛЕНО: если вакансия закрыта, напоминание не шлем
                            )
                        )
                    )
                    result = await db.execute(stmt)
                    reminders = result.scalars().all()

                    for rem in reminders:
                        log_context.set({})
                        set_log_context(
                            dialogue_id=rem.dialogue_id,
                            reminder_type=rem.reminder_type,
                            task="interview_reminder"
                        )
                        # 2. Ищем конфиг по ID (теперь через поиск в списке items)
                        reminder_cfg = next(
                            (item for item in settings.reminders.interview.items if item.id == rem.reminder_type), 
                            None
                        )
                        
                        if not reminder_cfg:
                            logger.warning(f"⚠️ Конфигурация для напоминания '{rem.reminder_type}' не найдена в config.yaml")
                            rem.status = 'error' # Помечаем ошибкой, чтобы не крутилось вечно
                            continue

                        if rem.dialogue:
                            dialogue = rem.dialogue
                            vacancy_title = dialogue.vacancy.title if dialogue.vacancy else "Вакансия"
                            
                            # 3. Достаем данные из метаданных (которые сохранил Engine)
                            meta = dialogue.metadata_json or {}
                            i_date_raw = meta.get("interview_date", "не указана")
                            i_time = meta.get("interview_time", "не указано")

                            # Красивое форматирование даты (из 2026-02-15 в 15.02.2026)
                            display_date = i_date_raw
                            try:
                                if i_date_raw != "не указана":
                                    dt_obj = datetime.datetime.strptime(i_date_raw, "%Y-%m-%d")
                                    display_date = dt_obj.strftime("%d.%m.%Y")
                            except:
                                pass

                            # 4. Форматируем текст
                            try:
                                formatted_text = reminder_cfg.text.format(
                                    interview_date=display_date,
                                    interview_time=i_time,
                                    vacancy_title=vacancy_title
                                )
                                
                                # 5. Публикуем в Engine для отправки
                                await mq.publish("engine_tasks", {
                                    "dialogue_id": rem.dialogue_id,
                                    "trigger": "reminder",
                                    "reminder_text": formatted_text
                                })
                                
                                rem.status = 'sent'
                                logger.info(f"✅ Отправлено напоминание '{rem.reminder_type}' для диалога {dialogue.id}")
                            except Exception as format_e:
                                logger.error(f"❌ Ошибка форматирования текста напоминания: {format_e}")
                                rem.status = 'error'
                        
                        rem.processed_at = now_utc
                    
                    await db.commit()
            except Exception as e:
                error_msg = f"❌ Ошибка в цикле собеседований [worker_scheduler]:\n{str(e)}"
                logger.error(error_msg, exc_info=True)
                logger.exception("❌ Ошибка в цикле собесов")
                try:
                    await mq.publish("tg_alerts", {
                        "type": "system", "text": error_msg, "alert_type": "admin_only"
                    })
                except: pass
            
            await asyncio.sleep(30) # Проверка каждые 30 секунд

    # --- 4. АКТИВНЫЙ ПОИСК КАНДИДАТОВ (UNIVERSAL) ---
    async def _loop_outbound_search(self):
        """Периодический поиск новых резюме по активным платформам (Avito + HH)"""
        while self.is_running:
            try:
                logger.info("🔍 Запуск цикла активного поиска кандидатов...")

                # === ПРОВЕРКА AVITO ===
                if settings.platforms.avito.enabled and settings.platforms.avito.outbound_search.enabled:
                    logger.info("🔍 Avito outbound поиск включен")
                    await avito_search_service.discover_and_propose()
                    logger.info("✅ Avito outbound поиск завершен")
                else:
                    logger.debug("🔍 Avito outbound поиск отключен в конфиге")

                # === ПРОВЕРКА HH ===
                if settings.platforms.hh.enabled and settings.platforms.hh.outbound_search.enabled:
                    logger.info("🔍 HH outbound поиск включен")
                    await hh_search_service.discover_and_propose()
                    logger.info("✅ HH outbound поиск завершен")
                else:
                    logger.debug("🔍 HH outbound поиск отключен в конфиге")

            except Exception as e:
                error_msg = f"❌ Ошибка в цикле активного поиска [worker_scheduler]:\n{str(e)}"
                logger.exception(error_msg)
                try:
                    await mq.publish("tg_alerts", {"type": "system", "text": error_msg, "alert_type": "admin_only"})
                except: pass

            await asyncio.sleep(900)  # 15 минут между циклами

    # --- 3. ОБНОВЛЕНИЕ БАЗЫ ЗНАНИЙ ---
    async def _loop_kb_refresh(self):
        """Обновление промптов каждые 3 минуты"""
        while self.is_running:
            try:
                logger.debug("🔄 Обновление библиотеки промптов из Google Docs...")
                await kb_service.refresh_cache()
                logger.debug("✅ База знаний успешно обновлена")
            except Exception as e:
                error_msg = f"❌ Ошибка обновления базы знаний (Google Docs):\n{str(e)}"
                logger.exception("❌ Ошибка обновления базы знаний")
                # Отправка алерта
                try:
                    await mq.publish("tg_alerts", {
                        "type": "system",
                        "text": error_msg,
                        "alert_type": "admin_only"
                    })
                except: pass
            await asyncio.sleep(180)

    async def _loop_google_search_sync_avito(self):
        """Синхронизация параметров поиска из Google Таблицы для Avito каждые 5 минут"""
        while self.is_running:
            try:
                if not settings.platforms.avito.enabled or not settings.platforms.avito.outbound_search.enabled:
                    await asyncio.sleep(300)
                    continue

                logger.info("🔄 Синхронизация параметров поиска Avito из Google Таблицы...")
                await google_sync_search_avito_service.sync_all()
                logger.info("✅ Синхронизация Avito завершена")
            except Exception as e:
                error_msg = f"❌ Ошибка в цикле синхронизации поиска Avito: {e}"
                logger.exception(error_msg)
                try:
                    await mq.publish("tg_alerts", {"type": "system", "text": error_msg, "alert_type": "admin_only"})
                except: pass
            await asyncio.sleep(300)  # 5 минут

    async def _loop_google_search_sync_hh(self):
        """Синхронизация параметров поиска из Google Таблицы для HH каждые 5 минут"""
        while self.is_running:
            try:
                if not settings.platforms.hh.enabled or not settings.platforms.hh.outbound_search.enabled:
                    await asyncio.sleep(300)
                    continue

                logger.info("🔄 Синхронизация параметров поиска HH из Google Таблицы...")
                await google_sync_search_hh_service.sync_all()
                logger.info("✅ Синхронизация HH завершена")
            except Exception as e:
                error_msg = f"❌ Ошибка в цикле синхронизации поиска HH: {e}"
                logger.exception(error_msg)
                try:
                    await mq.publish("tg_alerts", {"type": "system", "text": error_msg, "alert_type": "admin_only"})
                except: pass
            await asyncio.sleep(300)  # 5 минут

async def main():
    scheduler = Scheduler()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(scheduler.stop()))
    try:
        await scheduler.start()
    finally:
        await mq.close()
        await engine.dispose()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass