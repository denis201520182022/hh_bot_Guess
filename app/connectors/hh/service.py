# app/connectors/hh/service.py
import asyncio
import json
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
from app.db.models import Account, JobContext, Candidate, Dialogue, AppSettings, AnalyticsEvent, Director # Добавили AnalyticsEvent, Director
from app.core.rabbitmq import mq
from app.core.schemas import IncomingEventDTO, CandidateDTO, JobContextDTO
from app.connectors.base import BaseConnector
from app.services.director_mapping import resolve_director_name
from app.utils.logger import logger, set_log_context, log_context
from app.utils.analytics import log_event
from app.core.config import settings
from app.output_chanels.talantix.talantix_crm import talantix_crm_service
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

    async def _assign_director_to_vacancy(self, db: AsyncSession, job: JobContext, logger, vacancy_id: str):
        """
        Определяет директора по названию вакансии (ищет текст в скобках) и привязывает его.
        Пример: "Продавец-консультант GUESS (ТЦ Екатеринбург АУТЛЕТ)" → "ТЦ Екатеринбург АУТЛЕТ" → директор
        """
        job_title = job.title or ""

        if not job_title:
            logger.debug(f"Вакансия {vacancy_id}: название вакансии пустое")
            return

        director_name = resolve_director_name(job_title, job.city)

        if not director_name:
            bracket_content = (job_title.split("(")[-1].split(")")[0].strip() if "(" in job_title else "нет скобок")
            logger.debug(f"Вакансия {vacancy_id}: директор не найден по ТЦ '{bracket_content}' из названия '{job_title}'")
            return

        # Ищем директора по имени в БД
        stmt = select(Director).where(
            Director.name == director_name,
            Director.is_active == True
        )
        result = await db.execute(stmt)
        director = result.scalar_one_or_none()

        if not director:
            logger.warning(f"Вакансия {vacancy_id}: директор '{director_name}' найден в маппинге, но отсутствует в БД")
            return

        # Привязываем директора к вакансии
        if job.director_id != director.id:
            job.director_id = director.id
            logger.info(f"✅ Вакансия {vacancy_id}: назначен директор '{director.name}' (TG chat: {director.tg_chat_id}, Sheet: {director.google_sheet_name}) по ТЦ '{bracket_content}'")
        else:
            logger.debug(f"Вакансия {vacancy_id}: директор '{director.name}' уже привязан")

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
                    logger.info("🔎 [HH Poller] Активных HH аккаунтов не найдено. Жду...")
                    await asyncio.sleep(self.poll_interval)
                    continue

                logger.debug(f"🔎 [HH Poller] Начинаю цикл опроса для {len(account_ids)} аккаунтов...")

                # 2. Запускаем опрос каждого аккаунта через семафор
                async def run_with_sem(acc_id: int):
                    async with semaphore:
                        # На каждый аккаунт создаем свою сессию
                        async with AsyncSessionLocal() as db:
                            await self._poll_single_account(acc_id, db)

                await asyncio.gather(*[run_with_sem(aid) for aid in account_ids])
                logger.debug("✅ [HH Poller] Цикл опроса завершен.")

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
                logger.info(f"⏭️ Аккаунт ID {account_id} пропущен (заблокирован другим воркером).")
                return

            logger.debug(f"⚙️ [HH Poller] Обработка аккаунта: {account.name} (ID: {account_id})")
            set_log_context(account_id=account.id, account_name=account.name, platform="hh")

            # 1. Синхронизируем вакансии
            logger.debug(f"🔄 [{account.name}] Синхронизация списка вакансий...")
            vacancy_ids = await self._sync_vacancies_for_account(account, db)
            logger.debug(f"Количество вакансий: {len(vacancy_ids)}")
            
            if not vacancy_ids:
                logger.info(f"ℹ️ [{account.name}] Нет активных вакансий в работе.")
                return

            # 2. Собираем события из всех папок
            logger.debug(f"📥 [{account.name}] Сбор новых событий из папок HH...")
            await self._collect_hh_events(account, db, vacancy_ids)
            
            # Фиксируем изменения в Account
            logger.debug(f"💾 [{account.name}] Фиксация изменений в БД...")
            await db.commit()
            logger.debug(f"✅ [{account.name}] Обработка завершена.")

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
                    rec_logger.debug(f"Используем кэш списка вакансий (синхронизация была {(now - last_synced_dt).total_seconds() / 60:.1f} мин. назад)")
                    stmt = select(JobContext).filter_by(account_id=account.id, is_active=True)
                    cached_jobs = (await db.execute(stmt)).scalars().all()
                    return [v.external_id for v in cached_jobs]

            # 2. ЗАПРОС К API HH ЗА СПИСКОМ ВАКАНСИЙ (через наш новый client)
            all_vacancies_from_api = await hh.get_active_vacancies(account, db)
            if not all_vacancies_from_api:
                rec_logger.warning("API HH не вернуло активных вакансий.")
                return []

            rec_logger.debug(f"📥 HH POLLING DATA (VACANCIES): {json.dumps(all_vacancies_from_api, ensure_ascii=False)}")

            active_hh_ids = {str(v["id"]) for v in all_vacancies_from_api}
            rec_logger.debug(f"Начинаю проверку {len(all_vacancies_from_api)} вакансий из API...")

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
                    rec_logger.debug(f"Вакансия {hh_id}: ТРЕБУЕТСЯ обновление деталей (Причина: {reason})")
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
                            
                            unified_text = (
                                # f"Оформление: {contracts_str}\n"
                                # f"Формат работы: {work_formats}\n"
                                f"Адрес: {full_addr}\n\n"
                                f"Описание вакансии:\n{self._clean_html(full_data.get('description', ''))}"
                            )

                            # Сохраняем всё в JobContext
                            desc_data['full_raw_data'] = full_data
                            desc_data['details_last_synced_at'] = now.isoformat()
                            desc_data['description_text'] = unified_text # Очищенный и склеенный текст
                            desc_data['full_address'] = full_addr

                            job.description_data = desc_data
                            rec_logger.debug(f"Вакансия {hh_id}: детали успешно очищены и обновлены.")

                            # === ПРИВЯЗКА ДИРЕКТОРА ПО НАЗВАНИЮ ТЦ (из скобок) ===
                            await self._assign_director_to_vacancy(db, job, rec_logger, hh_id)
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
                
                if raw_responses:
                    logger.debug(f"📥 HH POLLING DATA (RESPONSES from {folder}): {json.dumps([r[0] for r in raw_responses], ensure_ascii=False)}")
                
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
            stmt = (
                select(Dialogue)
                .options(selectinload(Dialogue.candidate))
                .filter_by(external_chat_id=hh_response_id)
                .with_for_update()
            )
            dialogue = (await db.execute(stmt)).scalar_one_or_none()

            # --- ТЕСТОВЫЙ РЕЖИМ (ФИЛЬТР ПО ИМЕНИ) ---
            if settings.system.test_mode.enabled:
                candidate_name = None
                if dialogue and dialogue.candidate:
                    candidate_name = dialogue.candidate.full_name
                elif folder == 'response':
                    resume_info = item.get('resume', {})
                    candidate_first_name = resume_info.get('first_name', 'Неизвестно')
                    candidate_last_name = resume_info.get('last_name', '')
                    candidate_name = f"{candidate_first_name} {candidate_last_name}".strip()
                
                if candidate_name not in settings.system.test_mode.allowed_candidate_names:
                    logger.info(f"🧪 [TestMode] Игнорируем кандидата '{candidate_name}' (не в списке разрешенных)")
                    return

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

                # Извлекаем пол кандидата из резюме
                gender_info = resume_info.get('gender')
                gender_id = gender_info.get('id') if gender_info else None

                # Получаем номер телефона из полного резюме через HH API
                phone_number = None
                try:
                    full_resume = await hh.get_resume_details(account, db, hh_resume_id, with_creds=True)
                    if full_resume:
                        contacts = full_resume.get('contact', [])
                        # Ищем первый контакт с kind="phone"
                        for contact in contacts:
                            if contact.get('kind') == 'phone':
                                phone_number = contact.get('contact_value')
                                logger.info(f"📱 Получен номер телефона для кандидата {hh_resume_id}: {phone_number}")
                                break
                        if not phone_number:
                            logger.debug(f"📱 Номер телефона не найден в резюме {hh_resume_id}")
                    else:
                        logger.warning(f"⚠️ Не удалось получить полное резюме {hh_resume_id}")
                except Exception as e:
                    logger.error(f"❌ Ошибка при получении резюме {hh_resume_id}: {e}", exc_info=True)

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
                                phone_number=phone_number,
                                profile_data={"hh_resume_id": hh_resume_id, "gender": gender_id}
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

                # СИНХРОНИЗАЦИЯ С TALANTIX
                # Получаем номер телефона и ищем кандидата в Talantix
                if phone_number and settings.services.talantix.enabled:
                    try:
                        await self._sync_with_talantix(
                            dialogue=dialogue,
                            phone_number=phone_number,
                            hh_resume_id=hh_resume_id,
                            hh_vacancy_id=ext_vacancy_id,
                            db=db
                        )
                    except Exception as e:
                        logger.error(f"❌ Ошибка синхронизации с Talantix для диалога {dialogue.id}: {e}", exc_info=True)

                # Получаем первые сообщения (если были в отклике) и сохраняем всё
                await self._update_history_only(dialogue, account, db, item.get('messages_url'))
                await db.commit()
                logger.info(f"✅ Новый диалог HH {hh_response_id} успешно создан и инициализирован.")

            else:
                # --- ЛОГИКА ДЛЯ СУЩЕСТВУЮЩЕГО ДИАЛОГА (Обновления) ---
                set_log_context(dialogue_id=dialogue.id)
                
                # Если кандидат обнаружен в папке interview — переводим состояние диалога
                # (Твоя логика с проверками исключений)
                # if folder == 'interview':
                #     if dialogue.current_state not in ['post_qualification_chat', 'forwarded_to_researcher'] and dialogue.status != 'follow_up':
                #         logger.info(f"📍 Чат {hh_response_id} в папке 'interview'. Принудительный перевод в post_qualification_chat.")
                #         dialogue.current_state = 'post_qualification_chat'

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
        
        if all_api_msgs:
            logger.info(f"📥 HH POLLING DATA (MESSAGES for {dialogue.id}): {json.dumps(all_api_msgs, ensure_ascii=False)}")
        
        # Собираем ID и отпечатки (роль + текст) для дедупликации
        history_list = list(dialogue.history or [])
        existing_ids = {str(m.get("message_id")) for m in history_list}
        
        # Отпечаток: (роль, текст). Помогает, когда ID временный (bot_...) не совпадает с реальным.
        existing_fingerprints = {
            (m.get("role"), m.get("content", "").strip()) 
            for m in history_list 
            if m.get("content")
        }
        
        new_entries = []
        has_new_user_msg = False

        for msg in all_api_msgs:
            m_id = str(msg.get('id'))
            text = msg.get('text')
            
            is_applicant = msg.get('author', {}).get('participant_type') == 'applicant'
            role = "user" if is_applicant else "assistant"
            fingerprint = (role, text.strip()) if text else None

            # Дедупликация: по ID или (для бота) по тексту
            if m_id in existing_ids:
                continue
            
            if role == "assistant" and fingerprint in existing_fingerprints:
                # Если это копия сообщения бота, которую мы уже сохранили в Engine (с временным ID)
                continue

            if text:
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
            
            # Универсальный парсер для корректной сортировки по времени (обработка разных форматов таймзон)
            def get_sort_key(m):
                ts = m.get("timestamp_utc", "")
                if not ts: 
                    return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
                try:
                    # HH иногда шлет Z, иногда +0300. fromisoformat может капризничать в старых Python.
                    t = ts.replace("Z", "+00:00")
                    # Если есть смещение без двоеточия (+0300), добавляем его для совместимости
                    if "+" in t and ":" not in t.split("+")[-1]:
                        off = t.split("+")[-1]
                        if len(off) == 4:
                            t = t[:-4] + off[:2] + ":" + off[2:]
                    return datetime.datetime.fromisoformat(t).astimezone(datetime.timezone.utc)
                except Exception:
                    return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

            # Сортируем историю по реальным объектам datetime
            history.sort(key=get_sort_key)
            
            dialogue.history = history
            dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
            
            logger.info(f"📩 Добавлено {len(new_entries)} новых сообщений в историю диалога {dialogue.id}")
            return has_new_user_msg

        return False

    async def _sync_with_talantix(
        self, 
        dialogue: Dialogue, 
        phone_number: str, 
        hh_resume_id: str,
        hh_vacancy_id: str,
        db: AsyncSession
    ):
        """
        Синхронизация с Talantix CRM по новому алгоритму (api.txt):
        1. Поиск кандидатов по номеру телефона
        2. Проверка каждого кандидата по hh_resume_id (externalId в резюме)
        3. Получение откликов найденного кандидата
        4. Проверка каждой вакансии в отклике: привязана ли к ней наша hh_vacancy_id
        5. Сохранение данных в metadata_json
        """
        if not settings.services.talantix.enabled:
            logger.debug(f"Talantix integration is disabled. Skipping sync for dialogue {dialogue.id}.")
            return

        from sqlalchemy import select
        
        logger.info(f"🔄 [Talantix Sync] Начинаю для диалога {dialogue.id} (телефон: {phone_number}, HH Resume: {hh_resume_id})")
        
        # Получаем аккаунт Talantix API
        talantix_account = await db.scalar(
            select(Account).where(
                Account.platform == "talantix_api",
                Account.is_active == True
            )
        )
        
        if not talantix_account:
            logger.warning("⚠️ Аккаунт talantix_api не найден или не активен. Пропуск синхронизации.")
            return

        # 0. Загружаем маппинг менеджеров (чтобы знать их внутренние ID)
        vacancies_managers_map = await talantix_crm_service.get_all_vacancies_with_managers(
            account=talantix_account,
            db=db
        )
        
        # 1. Поиск всех кандидатов по номеру телефона
        persons = await talantix_crm_service.find_persons_by_phone(
            phone=phone_number,
            account=talantix_account,
            db=db
        )
        
        if not persons:
            logger.info(f"ℹ️ Кандидаты с номером {phone_number} не найдены в Talantix")
            return
        
        matched_person_id = None
        
        # 2. Итерируемся по каждому и ищем совпадение по HH Resume ID
        for p in persons:
            p_id = p.get('id')
            logger.debug(f"🧐 Проверка кандидата Talantix {p_id} ({p.get('firstName')} {p.get('lastName')})...")
            
            talantix_resume_ids = await talantix_crm_service.get_person_resume_ids(p_id, talantix_account, db)
            if hh_resume_id in talantix_resume_ids:
                logger.info(f"✅ Найден нужный кандидат в Talantix: person_id={p_id}")
                matched_person_id = p_id
                break
        
        if not matched_person_id:
            logger.info(f"❌ Среди найденных по телефону кандидатов нет того, у кого резюме {hh_resume_id}")
            return

        # 3. Находим конкретный отклик и вакансию
        person_data = await talantix_crm_service.get_person_responses(
            person_id=matched_person_id,
            account=talantix_account,
            db=db
        )
        
        if not person_data:
            logger.warning(f"⚠️ Не удалось получить отклики для person_id: {matched_person_id}")
            return

        responses = ((person_data.get('responses') or {}).get('items') or [])
        matched_talantix_data = None
        
        # 4. Проверяем каждый отклик
        for response in responses:
            workflow_status = response.get('workflowStatus') or {}
            vacancy = workflow_status.get('vacancy') or {}
            talantix_vacancy_id = vacancy.get('id')
            talantix_vacancy_title = vacancy.get('title')
            
            if not talantix_vacancy_id: continue

            logger.debug(f"🔍 Проверка привязки вакансии HH {hh_vacancy_id} к вакансии Talantix {talantix_vacancy_id}...")
            
            hh_external_ids = await talantix_crm_service.get_vacancy_external_ids(
                vacancy_id=talantix_vacancy_id, 
                account=talantix_account, 
                db=db
            )
            
            if hh_vacancy_id in hh_external_ids:
                logger.info(f"🎯 Найдено совпадение! Вакансия Talantix {talantix_vacancy_id} ('{talantix_vacancy_title}') привязана к HH {hh_vacancy_id}")
                
                # Собираем данные менеджеров (из маппинга, чтобы были ID)
                managers_data = []
                global_managers = vacancies_managers_map.get(talantix_vacancy_id, [])
                
                for manager_item in global_managers:
                    managers_data.append({
                        'manager_id': manager_item.get('manager_id'),
                        'vacancyRole': manager_item.get('vacancyRole'),
                        'firstName': manager_item.get('firstName'),
                        'lastName': manager_item.get('lastName'),
                        'middleName': manager_item.get('middleName')
                    })
                
                matched_talantix_data = {
                    'person_id': matched_person_id,
                    'vacancy_id': talantix_vacancy_id,
                    'vacancy_title': talantix_vacancy_title,
                    'managers': managers_data
                }
                break
        
        # 5. Сохраняем в metadata_json диалога
        if matched_talantix_data:
            metadata = dict(dialogue.metadata_json or {})
            metadata['talantix'] = matched_talantix_data
            dialogue.metadata_json = metadata
            flag_modified(dialogue, "metadata_json")
            
            logger.info(
                f"✅ Данные Talantix успешно привязаны к диалогу {dialogue.id}: "
                f"person_id={matched_talantix_data['person_id']}, "
                f"vacancy_id={matched_talantix_data['vacancy_id']}"
            )
        else:
            logger.info(f"ℹ️ Совпадений по вакансии HH {hh_vacancy_id} в Talantix не найдено для диалога {dialogue.id}")

# Синглтон сервиса для экспорта
hh_connector = HHConnectorService()
