# app/connectors/hh/service.py
import asyncio
import logging
import datetime
import time # Добавили для замеров времени из старого кода
from typing import Optional, Any, Dict, List
from decimal import Decimal
from zoneinfo import ZoneInfo # Для работы с таймзонами

from sqlalchemy import select, func, and_ # Добавили func и and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
import re
from app.utils.redis_lock import get_redis_client
from app.db.session import AsyncSessionLocal
from app.db.models import Account, JobContext, Candidate, Dialogue, AppSettings, AnalyticsEvent # Добавили AnalyticsEvent
from app.core.rabbitmq import mq
from app.core.schemas import IncomingEventDTO, CandidateDTO, JobContextDTO
from app.connectors.base import BaseConnector
from app.utils.logger import logger, set_log_context, log_context
from app.utils.analytics import log_event
from app.core.config import settings
from sqlalchemy.orm.attributes import flag_modified
from .client import hh

# --- КОНСТАНТЫ И НАСТРОЙКИ ТАЙМЗОНЫ ---
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
VACANCY_CACHE_DURATION_MINUTES = 2 # Из старого кода

class HHConnectorService(BaseConnector):
    def __init__(self):
        self.is_running = False
        self._poll_task: Optional[asyncio.Task] = None
        self.poll_interval = settings.platforms.hh.poll_interval 

    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ (ИЗ СТАРОГО КОДА) ===

    def _format_timestamp_to_msk(self, timestamp_str: str) -> str:
        """
        Преобразует строку времени из формата ISO в читаемую строку по МСК.
        Используется для формирования истории сообщений.
        """
        try:
            dt_object = datetime.datetime.fromisoformat(timestamp_str)
            msk_dt = dt_object.astimezone(MOSCOW_TZ)
            return msk_dt.strftime('%Y-%m-%d %H:%M:%S MSK')
        except (ValueError, TypeError):
            return "время не определено"
    def _clean_html(self, raw_html: str) -> str:
        """Удаляет HTML-теги для корректной работы LLM."""
        if not raw_html:
            return ""
        # Заменяем закрывающие теги списков на переносы строк
        text = re.sub(r'</li>', '\n', raw_html)
        # Заменяем параграфы и брейки на переносы
        text = re.sub(r'</p>|</div>|<br\s*/?>', '\n', text)
        # Удаляем все остальные теги
        text = re.sub(r'<.*?>', '', text)
        # Схлопываем лишние пустые строки
        return re.sub(r'\n\s*\n', '\n', text).strip()

    def _build_full_address(self, raw_data: dict) -> str:
        """Собирает полный адрес из структуры HH."""
        addr = raw_data.get('address')
        if not addr:
            return raw_data.get('area', {}).get('name', 'Адрес не указан')

        parts = []
        city = addr.get('city')
        street = addr.get('street')
        building = addr.get('building')
        
        if city: parts.append(city)
        if street: parts.append(street)
        if building: parts.append(f"д. {building}")
        
        desc = addr.get('description')
        if desc: parts.append(f"({desc})")
        
        metro = addr.get('metro_stations', [])
        if metro:
            m_parts = [f"м. {m.get('station_name')} ({m.get('line_name')})" for m in metro if m.get('station_name')]
            if m_parts: parts.append(", ".join(m_parts))
                
        return ", ".join(parts).strip()

    async def _accumulate_and_dispatch(self, dialogue: Dialogue, source: str):
        """
        Логика дебоунса (накопления). Ждем 5 секунд, чтобы собрать пачку сообщений,
        прежде чем будить ИИ-движок.
        """
        redis = get_redis_client()
        lock_key = f"debounce_lock:hh:{dialogue.external_chat_id}"
        
        # Если лок уже стоит — значит, таймер запущен, просто выходим
        if await redis.get(lock_key):
            logger.info(f"⏳ Сообщения для HH чата {dialogue.external_chat_id} накапливаются...")
            return

        # Ставим лог на 6 секунд (чуть больше времени ожидания)
        await redis.set(lock_key, "1", ex=6)

        async def wait_and_push():
            try:
                # Ждем 5 секунд
                await asyncio.sleep(5)
                
                # Формируем задачу для Engine
                engine_task = {
                    "dialogue_id": dialogue.id,
                    "account_id": dialogue.account_id,
                    "candidate_id": dialogue.candidate_id,
                    "platform": "hh",
                    "trigger": source
                }
                
                await mq.publish("engine_tasks", engine_task)
                logger.info(f"🚀 [Debounce HH] Пачка сообщений диалога {dialogue.id} отправлена в Engine")
                
            except Exception as e:
                logger.error(f"💥 Ошибка в дебоунсе HH: {e}", exc_info=True)
            finally:
                await redis.delete(lock_key)

        # Запускаем ожидание в фоне
        asyncio.create_task(wait_and_push())
    # === РЕАЛИЗАЦИЯ ИНТЕРФЕЙСА BaseConnector ===

    async def start(self):
        """Запуск фонового поллинга HH"""
        if self.is_running:
            return
        self.is_running = True
        logger.info("🚀 Запуск HH Connector Service (Polling mode)...")
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self):
        """Остановка фонового поллинга"""
        logger.info("🛑 Остановка HH Connector Service...")
        self.is_running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        await hh.close()
        logger.info("✅ HH Connector Service остановлен.")

    async def parse_event(self, payload: dict, account_id: int) -> IncomingEventDTO:
        """
        Переводит сырой JSON из RabbitMQ в унифицированный DTO.
        """
        item = payload.get("payload", {})
        # Для HH external_chat_id — это ID переговоров (negotiation id)
        return IncomingEventDTO(
            platform="hh",
            external_chat_id=str(item.get("id")),
            text=None, # Текст будет вытянут из истории позже
            user_id=str(item.get("resume", {}).get("id")),
            item_id=payload.get("vacancy_id_external"),
            raw_payload=payload
        )

    async def get_candidate_details(self, account: Account, db: AsyncSession, **kwargs) -> CandidateDTO:
        """
        Запрашивает расширенную информацию о кандидате через API HH.
        """
        resume_id = kwargs.get("resume_id")
        if not resume_id:
            return CandidateDTO(platform_user_id="unknown")

        # В будущем здесь можно дергать hh.get_resume_details
        # Пока возвращаем базовый DTO
        return CandidateDTO(
            platform_user_id=resume_id,
            raw_payload={"info": "Full resume sync not implemented yet"}
        )

    async def get_job_details(self, account: Account, db: AsyncSession, job_id: str) -> JobContextDTO:
        """
        Превращает данные из нашей БД в DTO вакансии.
        """
        stmt = select(JobContext).filter_by(external_id=job_id)
        job = (await db.execute(stmt)).scalar_one_or_none()
        
        return JobContextDTO(
            external_id=job_id,
            title=job.title if job else "Вакансия HH",
            description=job.description_data.get("description_text", "") if job else ""
        )

    async def send_message(self, account: Account, db: AsyncSession, chat_id: str, text: str, user_id: str = "me"):
        """Отправка сообщения через API HH"""
        return await hh.send_message(account, db, chat_id, text)

    # === ЛОГИКА ПОЛЛИНГА (POLLER) ===

    async def _poll_loop(self):
        """
        Бесконечный цикл обхода аккаунтов.
        Взято из main() старого проекта с адаптацией под RabbitMQ.
        """
        # Лимит одновременных аккаунтов в обработке
        # Берем из конфига (concurrency_limit)
        semaphore = asyncio.Semaphore(settings.platforms.hh.concurrency_limit)

        while self.is_running:
            try:
                async with AsyncSessionLocal() as db:
                    # 1. Получаем ID всех активных HH аккаунтов
                    stmt = select(Account.id).filter_by(platform="hh", is_active=True)
                    account_ids = (await db.execute(stmt)).scalars().all()

                if not account_ids:
                    logger.debug("Нет активных HH аккаунтов для сканирования. Жду...")
                    await asyncio.sleep(self.poll_interval)
                    continue

                # 2. Запускаем опрос каждого аккаунта через семафор
                async def run_with_sem(acc_id: int):
                    async with semaphore:
                        # На каждый аккаунт создаем свою сессию
                        async with AsyncSessionLocal() as db:
                            await self._poll_single_account(acc_id, db)

                await asyncio.gather(*[run_with_sem(aid) for aid in account_ids])

            except Exception as e:
                logger.error(f"💥 Критическая ошибка в главном цикле поллинга HH: {e}", exc_info=True)
            
            # Пауза между циклами (из конфига poll_interval)
            await asyncio.sleep(self.poll_interval)

    async def _poll_single_account(self, account_id: int, db: AsyncSession):
        """
        Опрос конкретного аккаунта. 
        Использует логику handle_scanner_recruiter (блокировка строки).
        """
        try:
            # Пытаемся заблокировать строку аккаунта (skip_locked=True пропустит, если уже занято)
            stmt = select(Account).filter_by(id=account_id).with_for_update(skip_locked=True)
            account = (await db.execute(stmt)).scalar_one_or_none()

            if not account:
                # Аккаунт уже обрабатывается другим процессом/воркером
                return

            set_log_context(account_id=account.id, account_name=account.name, platform="hh")

            # 1. Синхронизируем вакансии (метод, который мы написали ранее)
            # Это аналог get_all_active_vacancies_for_recruiter
            vacancy_ids = await self._sync_vacancies_for_account(account, db)
            
            if not vacancy_ids:
                logger.debug(f"У аккаунта {account.name} нет активных вакансий.")
                return

            # 2. Собираем события из всех папок (аналог Этапа 1 и Этапа 2)
            # Этот метод пушит сообщения в RabbitMQ hh_inbound
            await self._collect_hh_events(account, db, vacancy_ids)
            
            # Фиксируем изменения в Account (например, время синхронизации)
            await db.commit()

        except Exception as e:
            logger.error(f"❌ Ошибка сканирования аккаунта ID {account_id}: {e}", exc_info=True)
            await db.rollback()


    async def _sync_vacancies_for_account(self, account: Account, db: AsyncSession) -> List[str]:
        """
        Синхронизация списка и описаний вакансий (JobContext).
        Аналог get_all_active_vacancies_for_recruiter из старого проекта.
        """
        function_start_time = time.monotonic()
        
        # Настройка логгера (как в старом коде)
        rec_logger = logging.LoggerAdapter(logger, {
            "account_id": account.id, 
            "account_name": account.name,
            "worker": "scanner"
        })

        try:
            now = datetime.datetime.now(datetime.timezone.utc)
            cache_expiry_time = datetime.timedelta(minutes=VACANCY_CACHE_DURATION_MINUTES)
            details_sync_interval = datetime.timedelta(hours=1)

            # 1. ПРОВЕРКА КЭША (из settings JSONB)
            hh_sync_meta = dict(account.settings.get('hh_sync_meta', {}))
            last_synced_str = hh_sync_meta.get('vacancies_last_synced_at')
            
            if last_synced_str:
                last_synced_dt = datetime.datetime.fromisoformat(last_synced_str)
                if now - last_synced_dt < cache_expiry_time:
                    rec_logger.info(f"Используем кэш списка вакансий (синхронизация была {(now - last_synced_dt).total_seconds() / 60:.1f} мин. назад)")
                    stmt = select(JobContext).filter_by(account_id=account.id, is_active=True)
                    cached_jobs = (await db.execute(stmt)).scalars().all()
                    return [v.external_id for v in cached_jobs]

            # 2. ЗАПРОС К API HH ЗА СПИСКОМ ВАКАНСИЙ (через наш новый client)
            all_vacancies_from_api = await hh.get_active_vacancies(account, db)
            if not all_vacancies_from_api:
                rec_logger.warning("API HH не вернуло активных вакансий.")
                return []

            active_hh_ids = {str(v["id"]) for v in all_vacancies_from_api}
            rec_logger.info(f"Начинаю проверку {len(all_vacancies_from_api)} вакансий из API...")

            # 3. ОБРАБОТКА КАЖДОЙ ВАКАНСИИ
            for vacancy_data in all_vacancies_from_api:
                hh_id = str(vacancy_data.get("id"))
                address_data = vacancy_data.get("address") or {}
                actual_city = address_data.get("city") or vacancy_data.get("area", {}).get("name")

                # Ищем вакансию в нашей БД
                stmt = select(JobContext).filter_by(external_id=hh_id)
                job = (await db.execute(stmt)).scalar_one_or_none()

                if not job:
                    rec_logger.info(f"Вакансия {hh_id}: новая, создаю в БД")
                    job = JobContext(
                        external_id=hh_id,
                        account_id=account.id,
                        title=vacancy_data.get("name", "Без названия"),
                        city=actual_city,
                        is_active=True
                    )
                    db.add(job)
                    await db.flush()
                else:
                    # Обновляем базовые поля
                    job.title = vacancy_data.get("name", "Без названия")
                    job.city = actual_city
                    job.is_active = True # Если она в списке активных от API

                # --- ЛОГИКА ОБНОВЛЕНИЯ ДЕТАЛЕЙ (ОПИСАНИЯ) ---
                desc_data = dict(job.description_data or {})
                need_details_sync = False
                reason = ""

                last_details_sync_str = desc_data.get('details_last_synced_at')

                if not desc_data.get('full_raw_data'):
                    need_details_sync = True
                    reason = "отсутствуют сырые данные"
                elif not last_details_sync_str:
                    need_details_sync = True
                    reason = "дата синхронизации пуста"
                else:
                    last_details_dt = datetime.datetime.fromisoformat(last_details_sync_str)
                    if now - last_details_dt > details_sync_interval:
                        need_details_sync = True
                        reason = f"данные устарели (прошло {(now - last_details_dt).total_seconds() / 3600:.1f} ч.)"

                if need_details_sync:
                    rec_logger.info(f"Вакансия {hh_id}: ТРЕБУЕТСЯ обновление деталей (Причина: {reason})")
                    try:
                        full_data = await hh.get_vacancy_details(account, db, hh_id)
                        if full_data:
                            # --- ТВОЯ ЛОГИКА СБОРКИ ОПИСАНИЯ ---
                            full_addr = self._build_full_address(full_data)
                            
                            # Оформление
                            contracts = [c.get('name') for c in full_data.get('civil_law_contracts', [])]
                            if full_data.get('accept_labor_contract'):
                                contracts.insert(0, "ТК РФ")
                            contracts_str = ", ".join(contracts) if contracts else "не указано"
                            
                            # Форматы работы
                            work_formats = ", ".join([f.get('name') for f in full_data.get('work_format', [])])
                            
                            instruction = (
                                "СИСТЕМНАЯ УСТАНОВКА: Информация в блоке 'Описание вакансии' является наиболее актуальной "
                                "и приоритетной. Если данные в кратких полях (адрес, оформление) противоречат тексту "
                                "в описании, следует использовать информацию из описания.\n\n"
                            )

                            unified_text = (
                                f"{instruction}"
                                f"Оформление: {contracts_str}\n"
                                f"Формат работы: {work_formats}\n"
                                f"Адрес: {full_addr}\n\n"
                                f"Описание вакансии:\n{self._clean_html(full_data.get('description', ''))}"
                            )

                            # Сохраняем всё в JobContext
                            desc_data['full_raw_data'] = full_data
                            desc_data['details_last_synced_at'] = now.isoformat()
                            desc_data['description_text'] = unified_text # Очищенный и склеенный текст
                            desc_data['full_address'] = full_addr

                            job.description_data = desc_data
                            rec_logger.info(f"Вакансия {hh_id}: детали успешно очищены и обновлены.")
                        else:
                            rec_logger.warning(f"Вакансия {hh_id}: API вернуло пустой ответ.")
                    except Exception as e:
                        rec_logger.error(f"Вакансия {hh_id}: ошибка при скачивании деталей: {e}")

            # 4. ОБРАБОТКА НЕАКТИВНЫХ ВАКАНСИЙ
            # Все вакансии этого аккаунта, которых нет в свежем списке от API
            stale_stmt = (
                select(JobContext)
                .filter(
                    and_(
                        JobContext.account_id == account.id,
                        JobContext.external_id.notin_(active_hh_ids),
                        JobContext.is_active == True
                    )
                )
            )
            stale_jobs = (await db.execute(stale_stmt)).scalars().all()
            for s_job in stale_jobs:
                rec_logger.info(f"Вакансия {s_job.external_id} больше не активна в HH. Деактивируем.")
                s_job.is_active = False

            # Сохраняем время синхронизации в настройки аккаунта
            hh_sync_meta['vacancies_last_synced_at'] = now.isoformat()
            new_settings = dict(account.settings)
            new_settings['hh_sync_meta'] = hh_sync_meta
            account.settings = new_settings
            flag_modified(account, "settings") # Явное указание на изменение JSONB
            
            await db.commit()
            return list(active_hh_ids)

        except Exception as e:
            rec_logger.error(f"Ошибка при синхронизации вакансий: {e}", exc_info=True)
            await db.rollback()
            raise 
        finally:
            rec_logger.debug(f"Синхронизация вакансий завершена за {time.monotonic() - function_start_time:.2f} сек.")

    async def _collect_hh_events(self, account: Account, db: AsyncSession, vacancy_ids: List[str]):
        """Сбор новых откликов и сообщений и быстрый пуш в RabbitMQ"""
        # Твое требование: дата отсечки — дата добавления аккаунта
        cutoff_date = account.created_at 
        
        # Опрашиваем три ключевые папки (аналог Этапа 1 и Этапа 2 старого кода)
        folders = ['response', 'consider', 'interview']
        
        for folder in folders:
            try:
                # Для папок кроме 'response' используем флаг check_for_updates, 
                # чтобы API HH возвращало только то, где есть новые сообщения
                is_update_folder = folder != 'response'
                
                raw_responses = await hh.get_responses_from_folder(
                    account, db, folder, vacancy_ids, 
                    since_datetime=cutoff_date, 
                    check_for_updates=is_update_folder
                )
                
                for item, vid in raw_responses:
                    # Быстро пушим в очередь. Вся логика БД будет в Унификаторе.
                    await mq.publish("hh_inbound", {
                        "source": "hh_poller",
                        "account_id": account.id,
                        "folder": folder,
                        "vacancy_id_external": vid,
                        "payload": item
                    })
                    
            except Exception as e:
                logger.error(f"Ошибка сбора откликов HH из папки {folder}: {e}")

    

    # === ЛОГИКА УНИФИКАТОРА (Обработка из RabbitMQ в Worker) ===

    async def process_hh_event(self, raw_data: dict):
        """
        Главный метод обработки события HH из очереди RabbitMQ.
        Аналог логики process_new_responses и process_ongoing_responses.
        """
        account_id = raw_data.get("account_id")
        folder = raw_data.get("folder")
        item = raw_data.get("payload", {})
        ext_vacancy_id = raw_data.get("vacancy_id_external")
        
        # ID отклика в HH (Negotiation ID)
        hh_response_id = str(item.get('id'))
        
        # Устанавливаем базовый контекст логов
        set_log_context(account_id=account_id, chat_id=hh_response_id, folder=folder)

        async with AsyncSessionLocal() as db:
            # 1. ЗАГРУЖАЕМ АККАУНТ
            account = await db.get(Account, account_id)
            if not account:
                logger.error(f"❌ Аккаунт ID {account_id} не найден в БД.")
                return

            # 2. ИЩЕМ СУЩЕСТВУЮЩИЙ ДИАЛОГ
            stmt = select(Dialogue).filter_by(external_chat_id=hh_response_id).with_for_update()
            dialogue = (await db.execute(stmt)).scalar_one_or_none()

            if not dialogue:
                # --- ЛОГИКА ДЛЯ НОВОГО ОТКЛИКА ---
                
                # Если событие не из папки 'response', значит мы пропустили момент создания.
                # В новой архитектуре мы игнорируем обновления для несуществующих в БД диалогов,
                # если они не являются новыми откликами.
                if folder != 'response':
                    logger.debug(f"Найдено обновление для чата {hh_response_id} в папке {folder}, но диалога нет в БД. Пропуск.")
                    return

                # Извлекаем данные кандидата из отклика
                resume_info = item.get('resume', {})
                hh_resume_id = resume_info.get('id')
                if not hh_resume_id:
                    logger.warning(f"Отклик {hh_response_id} пришел без resume_id. Пропуск.")
                    return

                candidate_first_name = resume_info.get('first_name', 'Неизвестно')
                candidate_last_name = resume_info.get('last_name', '')
                candidate_full_name = f"{candidate_first_name} {candidate_last_name}".strip()

                # Находим вакансию в БД (она должна быть синхронизирована поллером ранее)
                stmt = select(JobContext).filter_by(external_id=ext_vacancy_id)
                job = (await db.execute(stmt)).scalar_one_or_none()
                
                if not job:
                    logger.error(f"Вакансия HH {ext_vacancy_id} не найдена в БД. Невозможно привязать отклик.")
                    return

                # Ищем или создаем кандидата
                # Используем твой составной ключ: ID резюме + ID вакансии
                unique_candidate_key = f"{hh_resume_id}_{ext_vacancy_id}"
                candidate = await db.scalar(select(Candidate).filter_by(platform_user_id=unique_candidate_key))
                
                if not candidate:
                    try:
                        # Используем вложенную транзакцию (savepoint) на случай race condition
                        async with db.begin_nested():
                            candidate = Candidate(
                                platform_user_id=unique_candidate_key,
                                full_name=candidate_full_name,
                                profile_data={"hh_resume_id": hh_resume_id}
                            )
                            db.add(candidate)
                            await db.flush()
                    except Exception:
                        await db.rollback()
                        candidate = await db.scalar(select(Candidate).filter_by(platform_user_id=unique_candidate_key))

                # БИЛЛИНГ И ИНИЦИАЛИЗАЦИЯ ДИАЛОГА
                try:
                    dialogue = await self._sync_dialogue_and_billing(
                        account=account,
                        candidate=candidate,
                        job=job,
                        hh_response_id=hh_response_id,
                        db=db,
                        trigger_source="hh_new_response"
                    )
                except Exception as e:
                    logger.warning(f"💰 Не удалось создать диалог для {hh_response_id} (вероятно, баланс): {e}")
                    return

                # КРИТИЧЕСКИЙ МОМЕНТ: ПЕРЕМЕЩЕНИЕ В HH
                # Сразу после записи в БД переносим в 'consider' (Подумать), 
                # чтобы HH видел, что мы взяли отклик в работу.
                try:
                    await hh.move_response_to_folder(account, db, hh_response_id, 'consider')
                except Exception as move_err:
                    logger.error(f"❌ Ошибка перемещения отклика {hh_response_id} в consider: {move_err}")

                # Получаем первые сообщения (если были в отклике) и сохраняем всё
                await self._update_history_only(dialogue, account, db, item.get('messages_url'))
                await db.commit()
                logger.info(f"✅ Новый диалог HH {hh_response_id} успешно создан и инициализирован.")

            else:
                # --- ЛОГИКА ДЛЯ СУЩЕСТВУЮЩЕГО ДИАЛОГА (Обновления) ---
                set_log_context(dialogue_id=dialogue.id)
                
                # Если кандидат обнаружен в папке interview — переводим состояние диалога
                # (Твоя логика с проверками исключений)
                if folder == 'interview':
                    if dialogue.current_state not in ['post_qualification_chat', 'forwarded_to_researcher'] and dialogue.status != 'follow_up':
                        logger.info(f"📍 Чат {hh_response_id} в папке 'interview'. Принудительный перевод в post_qualification_chat.")
                        dialogue.current_state = 'post_qualification_chat'

                # Синхронизируем новые сообщения из API
                messages_url = item.get('messages_url')
                history_changed = await self._update_history_only(dialogue, account, db, messages_url)

                # if history_changed:
                #     # Если пришло сообщение от кандидата - сбрасываем счетчик напоминаний (молчуна)
                #     if dialogue.reminder_level > 0:
                #         logger.info(f"🔄 Сброс reminder_level для диалога {dialogue.id}")
                #         dialogue.reminder_level = 0
                    
                #     # Логируем событие ответа для аналитики
                #     # Если режим 'followup' (дожим) -> пишем 'followup_reply', иначе 'user_reply'
                #     event_type = 'followup_reply' if dialogue.status == 'follow_up' else 'user_reply'
                #     await log_event(db, dialogue, event_type, {"folder": folder})

                await db.commit()

            # 3. ОТПРАВКА В ENGINE (через дебоунс)
            if dialogue and dialogue.status not in ['rejected', 'closed', 'archive']:
                await self._accumulate_and_dispatch(dialogue, "hh_poller")

    async def _sync_dialogue_and_billing(self, account: Account, candidate: Candidate, job: JobContext, hh_response_id: str, db: AsyncSession, trigger_source: str):
        """
        Проверка баланса, списание средств, отправка алертов и инициализация Dialogue.
        Полный аналог логики Авито.
        """
        # 1. Получаем настройки баланса с блокировкой строки
        settings_stmt = select(AppSettings).filter_by(id=1).with_for_update()
        settings_obj = await db.scalar(settings_stmt)
        if not settings_obj:
            settings_obj = AppSettings(id=1, balance=Decimal("0.00"))
            db.add(settings_obj)
            await db.flush()

        costs = settings_obj.costs or {}
        cost_per_dialogue = Decimal(str(costs.get("dialogue", 19.00)))
        current_balance = settings_obj.balance

        # 2. ПРОВЕРКА: Хватает ли денег вообще?
        if current_balance < cost_per_dialogue:
            logger.error(f"💰 НЕДОСТАТОЧНО СРЕДСТВ для аккаунта HH {account.name}!")
            if not settings_obj.low_limit_notified:
                await mq.publish("tg_alerts", {
                    "type": "system",
                    "text": f"🚨 **БОТ ОСТАНОВЛЕН (HH)!** Недостаточно средств для аккаунта **{account.name}**. Баланс: {current_balance} руб.",
                    "alert_type": "all"
                })
                settings_obj.low_limit_notified = True
                await db.commit()
            raise Exception(f"Insufficient funds for HH account {account.id}")

        # 3. СПИСАНИЕ
        settings_obj.balance -= cost_per_dialogue
        
        # Обновляем статистику трат в AppSettings
        stats = dict(settings_obj.stats or {})
        stats["total_spent"] = float(Decimal(str(stats.get("total_spent", 0))) + cost_per_dialogue)
        stats["spent_on_dialogues"] = float(Decimal(str(stats.get("spent_on_dialogues", 0))) + cost_per_dialogue)
        settings_obj.stats = stats

        # 4. АЛЕРТ: Порог низкого баланса
        if settings_obj.balance < settings_obj.low_balance_threshold and not settings_obj.low_limit_notified:
            await mq.publish("tg_alerts", {
                "type": "system",
                "text": f"📉 **Внимание (HH)!** Баланс аккаунта **{account.name}** близок к нулю: {settings_obj.balance} руб.",
                "alert_type": "balance"
            })
            settings_obj.low_limit_notified = True
        elif settings_obj.balance >= settings_obj.low_balance_threshold:
            settings_obj.low_limit_notified = False

        # 5. ПОДГОТОВКА СИСТЕМНОЙ КОМАНДЫ
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        initial_history = [{
            'message_id': f'hh_init_{hh_response_id}',
            'role': 'user',
            'content': "[SYSTEM COMMAND] Кандидат откликнулся на вакансию. Поздоровайся и предложи задать вопросы",
            'timestamp_utc': now_utc.isoformat()
        }]

        # 6. СОЗДАНИЕ ДИАЛОГА
        dialogue = Dialogue(
            external_chat_id=hh_response_id,
            account_id=account.id,
            candidate_id=candidate.id,
            vacancy_id=job.id,
            history=initial_history,
            current_state="initial",
            status="new",
            last_message_at=now_utc
        )
        db.add(dialogue)
        await db.flush()

        # 7. АНАЛИТИКА: Событие создания лида
        await log_event(
            db=db,
            dialogue=dialogue,
            event_type='lead_created',
            event_data={"cost": float(cost_per_dialogue), "platform": "hh", "trigger": trigger_source}
        )
        
        return dialogue

    async def _update_history_only(self, dialogue: Dialogue, account: Account, db: AsyncSession, messages_url: str) -> bool:
        """
        Синхронизация истории сообщений. 
        Возвращает True, если были добавлены новые сообщения от кандидата.
        """
        if not messages_url: return False

        all_api_msgs = await hh.get_messages(account, db, messages_url)
        
        # Собираем ID уже известных сообщений (из истории и из временного буфера, если он есть)
        existing_ids = {str(m.get("message_id")) for m in (dialogue.history or [])}
        
        new_entries = []
        has_new_user_msg = False

        for msg in all_api_msgs:
            m_id = str(msg.get('id'))
            text = msg.get('text')
            
            if m_id not in existing_ids and text:
                # В HH автор applicant — это наш 'user'
                is_applicant = msg.get('author', {}).get('participant_type') == 'applicant'
                role = "user" if is_applicant else "assistant"
                
                if is_applicant:
                    has_new_user_msg = True
                
                new_entries.append({
                    "role": role,
                    "content": text,
                    "message_id": m_id,
                    "timestamp_utc": msg.get('created_at'),
                    "timestamp_msk": self._format_timestamp_to_msk(msg.get('created_at'))
                })

        if new_entries:
            history = list(dialogue.history or [])
            history.extend(new_entries)
            # Сортируем историю по времени
            history.sort(key=lambda x: x.get("timestamp_utc", ""))
            
            dialogue.history = history
            dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
            
            logger.info(f"📩 Добавлено {len(new_entries)} новых сообщений в историю диалога {dialogue.id}")
            return has_new_user_msg
            
        return False
# Синглтон сервиса для экспорта
hh_connector = HHConnectorService()