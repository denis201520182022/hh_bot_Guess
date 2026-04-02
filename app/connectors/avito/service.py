# app/connectors/avito/service.py
import asyncio
import logging
import os
import datetime
from typing import Optional, Any, Dict
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.db.session import AsyncSessionLocal
from app.db.models import Account, JobContext, Candidate, Dialogue, AppSettings, AnalyticsEvent
from app.core.rabbitmq import mq
from app.utils.redis_lock import get_redis_client
from app.db.models import Account, JobContext, Candidate, Dialogue, AppSettings
from app.utils.analytics import log_event 
from .client import avito

from app.utils.logger import logger, set_log_context, log_context

class AvitoConnectorService:
    def __init__(self):
        self.is_running = False
        self._poll_task: Optional[asyncio.Task] = None
        self.poll_interval = 5

    async def start(self):
        if self.is_running:
            return
        self.is_running = True
        logger.info("🚀 Запуск Avito Connector Service...")
        await self._setup_all_webhooks()
        #self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self):
        logger.info("🛑 Остановка Avito Connector Service...")
        self.is_running = False
        if self._poll_task:
            self._poll_task.cancel()
            try: await self._poll_task
            except asyncio.CancelledError: pass
        await avito.close()
        logger.info("✅ Avito Connector Service полностью остановлен.")

    async def _setup_all_webhooks(self):
        webhook_base = os.getenv("WEBHOOK_BASE_URL")
        if not webhook_base:
            error_msg = "❌ WEBHOOK_BASE_URL не задан! Бот не будет получать сообщения из чатов."
            logger.error(error_msg)
            await mq.publish("tg_alerts", {"type": "system", "text": error_msg})
            return

        target_url = webhook_base.rstrip('/') + "/webhooks/avito"
        async with AsyncSessionLocal() as db:
            try:
                stmt = select(Account).filter_by(platform="avito", is_active=True)
                accounts = (await db.execute(stmt)).scalars().all()
                for acc in accounts:
                    await avito.check_and_register_webhooks(acc, db, target_url)
            except Exception as e:
                error_msg = f"❌ Ошибка инициализации вебхуков Avito: {e}"
                logger.error(error_msg, exc_info=True)
                await mq.publish("tg_alerts", {"type": "system", "text": error_msg})

    async def _poll_loop(self):
        while self.is_running:
            try:
                async with AsyncSessionLocal() as db:
                    stmt = select(Account).filter_by(platform="avito", is_active=True)
                    accounts = (await db.execute(stmt)).scalars().all()
                    tasks = [self._poll_single_account(acc, db) for acc in accounts]
                    await asyncio.gather(*tasks)
            except Exception as e:
                error_msg = f"💥 Критическая ошибка в цикле поллинга откликов: {e}"
                logger.error(error_msg, exc_info=True)
                await mq.publish("tg_alerts", {"type": "system", "text": error_msg})
            await asyncio.sleep(self.poll_interval)

    async def _poll_single_account(self, account: Account, db: AsyncSession):
        set_log_context(account_id=account.id, account_name=account.name, source="avito_poller")
        try:
            new_apps = await avito.get_new_applications(account, db)
            for app_data in new_apps:
                await self.process_avito_event({
                    "source": "avito_poller",
                    "account_id": account.id,
                    "payload": app_data
                })
        except Exception as e:
            error_msg = f"⚠️ Ошибка поллинга аккаунта {account.name} (ID: {account.id}): {e}"
            logger.error(error_msg, exc_info=True)
            await mq.publish("tg_alerts", {"type": "system", "text": error_msg})

    # --- ЛОГИКА УНИФИКАТОРА ---

    def _parse_message_content(self, content_data: dict) -> str:
        """Единая логика извлечения текста из сложной структуры Авито"""
        text_content = content_data.get("text")
        
        if not text_content:
            if content_data.get("image"):
                text_content = "[Вложение: Изображение]"
            elif content_data.get("item"):
                item_title = content_data.get("item", {}).get("title", "Товар")
                text_content = f"[Вложение: Карточка товара - {item_title}]"
            elif content_data.get("link"):
                # Исправили кавычки тут
                url = content_data.get("link", {}).get("url", "нет ссылки")
                text_content = f"[Вложение: Ссылка - {url}]"
            elif content_data.get("call"):
                status = content_data.get("call", {}).get("status", "")
                text_content = f"[Звонок: {status}]"
            else:
                text_content = "[Неподдерживаемый тип сообщения]"
        return text_content

    def _inject_webhook_message(self, dialogue: Dialogue, payload: dict, account: Account):
        """
        Ручное добавление сообщения из вебхука в историю перед синхронизацией.
        """
        try:
            # Путь к данным в вебхуке Messenger V3: payload -> value
            msg_data = payload.get("payload", {}).get("value", {})
            if not msg_data:
                return

            msg_id = str(msg_data.get("id"))
            
            # Проверка на дубликаты (вдруг уже есть)
            existing_ids = {str(m.get("message_id")) for m in (dialogue.history or [])}
            if msg_id in existing_ids:
                return

            # Определение роли
            author_id = str(msg_data.get("author_id"))
            # Наш ID (бота). Берем из базы.
            my_user_id = str(account.auth_data.get("user_id"))
            
            # Если автор - это мы, то роль assistant, иначе user
            role = "assistant" if author_id == my_user_id else "user"

            # Время: в вебхуке оно в Unix timestamp (created)
            created_ts = msg_data.get("created")
            timestamp_utc = datetime.datetime.fromtimestamp(created_ts, datetime.timezone.utc).isoformat()

            # Контент: используем наш общий парсер
            content_text = self._parse_message_content(msg_data.get("content", {}))
            # --- ДОБАВИТЬ ЭТУ ПРОВЕРКУ ---
            if content_text.strip().startswith("[Системное сообщение]"):
                logger.info(f"🚫 Игнорируем системное сообщение из вебхука в чате {dialogue.external_chat_id}")
                return # Просто выходим, не добавляя в историю
            
            new_entry = {
                "role": role,
                "content": content_text,
                "message_id": msg_id,
                "timestamp_utc": timestamp_utc
            }
            
            # Если это исходящее от нас, добавляем контекст
            if role == "assistant":
                new_entry["state"] = dialogue.current_state
                new_entry["extracted_data"] = {}

            # Добавляем в историю
            history = list(dialogue.history or [])
            history.append(new_entry)
            
            # Сортируем на всякий случай, чтобы порядок был верным
            history.sort(key=lambda x: x.get("timestamp_utc") or "0000-01-01T00:00:00+00:00")
            
            dialogue.history = history
            dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
            
            logger.info(f"⚡ Сообщение {msg_id} добавлено из вебхука мгновенно.")

        except Exception as e:
            logger.error(f"⚠️ Ошибка при ручном добавлении вебхука в историю: {e}")
            # Не падаем, так как следом пойдет _update_history_only и починит всё

    async def _accumulate_and_dispatch(self, dialogue: Dialogue, job: JobContext, source: str):
        redis = get_redis_client()
        lock_key = f"debounce_lock:{dialogue.external_chat_id}"
        
        if await redis.get(lock_key):
            logger.info(f"⏳ Сообщение для чата {dialogue.external_chat_id} добавлено в очередь ожидания.")
            return

        await redis.set(lock_key, "1", ex=6)

        async def wait_and_push():
            try:
                await asyncio.sleep(5)
                
                engine_task = {
                    "dialogue_id": dialogue.id,
                    "account_id": dialogue.account_id,
                    "candidate_id": dialogue.candidate_id,
                    "vacancy_id": job.id if job else None,
                    "platform": "avito",
                    "trigger": source
                }
                
                await mq.publish("engine_tasks", engine_task)
                # ЛОГ ПЕРЕНЕСЕН СЮДА:
                logger.info(f"🚀 [Debounce] Пачка сообщений для диалога {dialogue.id} отправлена в Engine")
                
            except Exception as e:
                error_msg = f"💥 Ошибка в фоновом накопителе Debounce: {e}"
                logger.error(error_msg, exc_info=True)
                await mq.publish("tg_alerts", {"type": "system", "text": error_msg})
                raise e
            finally:
                await redis.delete(lock_key)

        asyncio.create_task(wait_and_push())

    async def process_avito_event(self, raw_data: dict):
        source = raw_data.get("source")
        payload = raw_data.get("payload", {})
        
        avito_user_id = raw_data.get("avito_user_id") 
        account_id = raw_data.get("account_id")      
        set_log_context(
            source=source,
            avito_user_id=avito_user_id,
            account_id=account_id
        )

        external_chat_id = None
        resume_id = None
        item_id = None
        avito_author_id = None 
        is_system_msg = False

        # 1. Извлекаем данные в зависимости от источника
        if source == "avito_webhook":
            msg_val = payload.get("payload", {}).get("value", {})
            external_chat_id = msg_val.get("chat_id")
            item_id = msg_val.get("item_id")
            avito_author_id = str(msg_val.get("author_id")) if msg_val.get("author_id") else None
            
            # Проверка на системное сообщение
            text = msg_val.get("content", {}).get("text", "")
            if text.strip().startswith("[Системное сообщение]"):
                is_system_msg = True

            if external_chat_id:
                set_log_context(chat_id=external_chat_id)

            # Игнорируем эхо (сообщения бота)
            if avito_author_id and str(avito_author_id) == str(avito_user_id):
                logger.info(f"🚫 Игнорируем эхо-сообщение от бота в чате {external_chat_id}")
                return 

        elif source == "avito_poller":
            contacts = payload.get("contacts", {})
            external_chat_id = contacts.get("chat", {}).get("value")
            resume_id = str(payload.get("applicant", {}).get("resume_id"))
            item_id = payload.get("vacancy_id")
            avito_author_id = str(payload.get("applicant", {}).get("user_id"))
            if external_chat_id:
                set_log_context(chat_id=external_chat_id)

        elif source == "avito_search_found":
            external_chat_id = raw_data.get("chat_id")
            resume_id = raw_data.get("resume_id")
            item_id = raw_data.get("vacancy_id")
            # Для поиска автор - это ID кандидата, переданный извне
            avito_author_id = str(raw_data.get("avito_user_id_candidate")) 
            if external_chat_id:
                set_log_context(chat_id=external_chat_id)

        async with AsyncSessionLocal() as db:
            # 2. Находим аккаунт владельца
            if source == "avito_webhook":
                account = await db.scalar(select(Account).filter(Account.auth_data['user_id'].astext == str(avito_user_id)))
            else:
                account = await db.get(Account, account_id)

            if not account:
                logger.error(f"❌ Аккаунт не найден (ID: {avito_user_id})")
                return

            # 3. Ищем существующий диалог
            stmt = select(Dialogue).options(selectinload(Dialogue.vacancy)).filter_by(external_chat_id=external_chat_id)
            dialogue = (await db.execute(stmt)).scalar_one_or_none()
            # 5. Синхронизируем вакансию
            job_context = None
            if item_id:
                try:
                    job_context = await self._sync_vacancy(account, db, item_id)
                    
                    # --- ГЛАВНЫЙ ФИЛЬТР: ПРЕРЫВАЕМ ОБРАБОТКУ, ЕСЛИ НЕ АКТИВНА ---
                    if not job_context or not job_context.is_active:
                        status_label = "НЕ АКТИВНА" if job_context else "НЕ НАЙДЕНА"
                        logger.info(f"⛔ ИГНОР: Вакансия {item_id} {status_label}. Обработка прервана.")
                        
                        # Если вакансия не активна, мы просто выходим из функции.
                        # БД зафиксирует is_active=False (внутри _sync_vacancy), но диалог не создастся/не обновится.
                        await db.commit() # Сохраняем статус вакансии
                        return 
                except Exception as e:
                    logger.error(f"⚠️ Ошибка при подтягивании контекста объявления {item_id}: {e}", exc_info=True)
                    logger.info(f"ℹ️ Контекст объявления {item_id} не подтянут из-за ошибки, продолжаем обработку сообщения.")

            if dialogue:
                # --- ЛОГИКА ДЛЯ СУЩЕСТВУЮЩЕГО ДИАЛОГА ---
                if is_system_msg:
                    logger.info(f"🚫 Игнорируем системное сообщение в СУЩЕСТВУЮЩЕМ чате {external_chat_id}")
                    return 
                # --- ДОБАВЬ ЭТО: "Обновление временного ID на реальный" ---
                

                # Обновляем историю нормальным сообщением
                if source == "avito_webhook":
                    self._inject_webhook_message(dialogue, payload, account)
                await self._update_history_only(dialogue, account, external_chat_id, db)

            else:
                # --- ЛОГИКА ДЛЯ НОВОГО ДИАЛОГА ---
                if is_system_msg:
                    logger.info(f"🆕 Системное сообщение в НОВОМ чате {external_chat_id}. Инициализируем диалог.")

                # Определяем идентификатор кандидата (resume_id в приоритете)
                # 1. Сначала пытаемся найти resume_id (для вакансий)
                if not resume_id:
                    try:
                        resume_id = await self._fetch_resume_id_by_chat_id(account, db, external_chat_id)
                    except Exception:
                        resume_id = None

                # 2. Формируем БАЗОВЫЙ ID (либо резюме, либо автор, либо заглушка)
                base_id = resume_id or (avito_author_id if avito_author_id and avito_author_id != "1" else f"temp_{external_chat_id[-8:]}")
                
                # 3. ФОРМИРУЕМ УНИКАЛЬНЫЙ СОСТАВНОЙ КЛЮЧ: ЮЗЕР + ВАКАНСИЯ
                # Теперь platform_user_id будет выглядеть так: "12345_67890"
                unique_candidate_key = f"{base_id}_{item_id}"

                # 4. Ищем или создаем кандидата по этому составному ключу
                candidate = await db.scalar(select(Candidate).filter_by(platform_user_id=unique_candidate_key))
                if not candidate:
                    try:
                        async with db.begin_nested():
                            candidate = Candidate(
                                platform_user_id=unique_candidate_key, 
                                profile_data={"note": f"Unique candidate for context {item_id}"}
                            )
                            db.add(candidate)
                            await db.flush()
                    except Exception:
                        await db.rollback()
                        candidate = await db.scalar(select(Candidate).filter_by(platform_user_id=unique_candidate_key))

                
                # 6. Биллинг и создание диалога
                dialogue = await self._sync_dialogue_and_billing(
                    account, candidate, job_context, external_chat_id, db, 
                    payload if source == "avito_poller" else {},
                    trigger_source=source
                )

            # 7. Отправка в Engine (если чат не закрыт)
            if dialogue:
                if dialogue.status == 'rejected':
                    logger.info(f"🤐 Чат {external_chat_id} отклонен. Молчим.")
                else:
                    # Даже если это было системное сообщение, Engine проверит историю и отправит приветствие
                    await self._accumulate_and_dispatch(dialogue, dialogue.vacancy, source)
            
            await db.commit()

    def _enrich_from_resume(self, candidate: Candidate, resume: dict):
        """
        Парсит данные из Resume API и записывает их в profile_data кандидата.
        """
        profile = dict(candidate.profile_data or {})
        params = resume.get("params", {})
        addr = resume.get("address_details", {})

        # 1. Город проживания
        if not profile.get("city"):
            profile["city"] = addr.get("location") or params.get("address")

        # 2. Возраст
        if not profile.get("age"):
            profile["age"] = params.get("age")

        # 3. Гражданство
        if not profile.get("citizenship"):
            profile["citizenship"] = params.get("nationality")

        # 4. Наличие патента (Разрешение на работу в РФ)
        if "has_patent" not in profile:
            val = params.get("razreshenie_na_rabotu_v_rossii")
            if val == "Да":
                profile["has_patent"] = "да"
            elif val == "Нет":
                profile["has_patent"] = "нет"

        candidate.profile_data = profile



    async def _fetch_resume_id_by_chat_id(self, account: Account, db: AsyncSession, chat_id: str) -> str:
        """
        Метод-мост: находит resume_id через Job API, используя фильтр chatId.
        Теперь с обязательным параметром updatedAtFrom.
        """
        # Определяем дату, начиная с которой искать (например, за последние 30 дней)
        # Этого достаточно, чтобы найти активный отклик.
        date_from = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

        params = {
            "chatId": chat_id,
            "updatedAtFrom": date_from  # <--- ТЕПЕРЬ ОБЯЗАТЕЛЬНО
        }

        # 1. Получаем ID отклика по chatId
        logger.info(f"🔍 Запрос в Job API для поиска отклика по chatId: {chat_id}")
        resp_ids = await avito._request(
            "GET", 
            "/job/v1/applications/get_ids", 
            account, 
            db, 
            params=params
        )
        
        apps = resp_ids.get("applications", [])
        
        if not apps:
            raise ValueError(f"Отклик для чата {chat_id} не найден в Job API (искали с {date_from})")

        app_id = apps[0]["id"]
        logger.info(f"✅ Найден ID отклика: {app_id}, запрашиваем детали...")
        
        # 2. Получаем детали отклика, чтобы вытащить resume_id
        details = await avito._request(
            "POST", 
            "/job/v1/applications/get_by_ids", 
            account, 
            db, 
            json={"ids": [app_id]}
        )
        
        app_details = details.get("applications", [])
        
        if not app_details:
            raise ValueError(f"Не удалось получить детали отклика {app_id}")

        resume_id = str(app_details[0].get("applicant", {}).get("resume_id"))
        logger.info(f"✅ Получен resume_id: {resume_id}")
        
        return resume_id

    async def _sync_vacancy(self, account: Account, db: AsyncSession, item_id: Any) -> Optional[JobContext]:
        if not item_id:
            return None
        
        item_id_str = str(item_id)
        
        # 1. Получаем или создаем запись в БД
        job = await db.scalar(select(JobContext).filter_by(external_id=item_id_str))
        if not job:
            job = JobContext(external_id=item_id_str, account_id=account.id, is_active=True)
            db.add(job)
            await db.flush()

        vac_details = None
        try:
            # Пытаемся получить детали как вакансию
            try:
                vac_details = await avito.get_job_details(item_id_str, account, db)
                logger.debug(f"✅ Получены детали вакансии {vac_details}")
            except Exception as e:
                # Если не вакансия — идем в Core API
                logger.debug(f"ℹ️ {item_id_str} проверяем через Core API...")
                vac_details = await avito.get_item_details(item_id_str, account, db)
                logger.debug(f"✅ Получены детали вакансии {vac_details}")

            # Если API что-то вернуло
            if vac_details:
                api_status = getattr(vac_details, 'status', 'unknown')
                job.is_active = (api_status == 'active') # ЖЕСТКАЯ ПРОВЕРКА
                logger.info(f"✅ Статус API для {item_id_str}: {api_status} (is_active={job.is_active})")
                job.title = vac_details.title
                job.city = vac_details.city
                job.description_data = {"text": vac_details.description, "status": api_status}
                logger.info(f"✅ Статус API для {item_id_str}: {api_status} (is_active={job.is_active})")
            else:
                # Если vac_details == None (объявление не найдено совсем)
                logger.warning(f"🔴 Объявление {item_id_str} отсутствует в API. Деактивируем.")
                job.is_active = False

        except Exception as e:
            logger.error(f"💥 Ошибка при синхронизации {item_id_str}: {e}")
            job.is_active = False # Безопасный режим: если ошибка связи, считаем неактивным

        await db.flush()
        return job
        
    
        
    def _enrich_candidate_from_avito_payload(self, candidate: Candidate, payload: dict):
        """
        Универсальный парсинг: работает и для откликов (poller), и для поиска (search)
        """
        # 1. Попытка взять данные из структуры отклика (poller)
        applicant = payload.get("applicant", {})
        data = applicant.get("data", {})
        contacts = payload.get("contacts", {})

        # 2. Попытка взять данные из нашей структуры поиска (search)
        search_name = payload.get("search_full_name")
        search_phone = payload.get("search_phone")

        # --- ЗАПОЛНЕНИЕ ФИО ---
        if not candidate.full_name:
            # Приоритет: 1. Поиск, 2. Прямое поле name отклика, 3. Объект full_name отклика
            candidate.full_name = search_name or data.get("name") or data.get("full_name", {}).get("name")
            
        # --- ЗАПОЛНЕНИЕ ТЕЛЕФОНА ---
        if not candidate.phone_number:
            phone_val = None
            if search_phone:
                phone_val = search_phone
            else:
                phones = contacts.get("phones", [])
                if phones:
                    phone_val = phones[0].get("value")
            
            if phone_val:
                candidate.phone_number = str(phone_val)

        # --- ЗАПОЛНЕНИЕ ОСТАЛЬНОГО (только для поллера) ---
        # Для поиска эти поля заполняются в _enrich_from_resume
        profile = dict(candidate.profile_data or {})
        if "citizenship" not in profile:
            profile["citizenship"] = data.get("citizenship")
        if "birthday" not in profile:
            profile["birthday"] = data.get("birthday")
        if "city" not in profile:
            profile["city"] = data.get("city") or applicant.get("city")
            
        candidate.profile_data = profile

    async def _sync_dialogue_and_billing(self, account: Account, candidate: Candidate, job: JobContext, chat_id: str, db: AsyncSession, payload: dict, trigger_source: str = None):
        if not chat_id: return None

        dialogue = await db.scalar(select(Dialogue).filter_by(external_chat_id=chat_id))
        
        if dialogue:
            await self._update_history_only(dialogue, account, chat_id, db)
            return dialogue

        # === НОВЫЙ ЛИД: ПЕРВИЧНОЕ ЗАПОЛНЕНИЕ ДАННЫХ ИЗ АВИТО ===
        # self._enrich_candidate_from_avito_payload(candidate, payload)

        # === БИЛЛИНГ: СПИСАНИЕ СРЕДСТВ ===
        settings_stmt = select(AppSettings).filter_by(id=1).with_for_update()
        settings_obj = await db.scalar(settings_stmt)
        if not settings_obj:
            settings_obj = AppSettings(id=1, balance=Decimal("0.00"))
            db.add(settings_obj)
            await db.flush() 

        costs = settings_obj.costs or {}
        cost_per_dialogue = Decimal(str(costs.get("dialogue", 19.00)))
        current_balance = settings_obj.balance

        if current_balance < cost_per_dialogue:
            logger.error(
                "💰 НЕДОСТАТОЧНО СРЕДСТВ!", 
                extra={
                    "balance": float(current_balance), 
                    "cost": float(cost_per_dialogue),
                    "account_name": account.name
                }
            )
            if not settings_obj.low_limit_notified:
                await mq.publish("tg_alerts", {
                    "type": "system",
                    "text": f"🚨 **БОТ ОСТАНОВЛЕН!** Недостаточно средств для аккаунта **{account.name}**. Баланс: {current_balance} руб.",
                    "alert_type": "all"
                })
                settings_obj.low_limit_notified = True
                await db.commit()
            raise Exception(f"Insufficient funds for account {account.id}")

        settings_obj.balance -= cost_per_dialogue
        stats = dict(settings_obj.stats or {})
        stats["total_spent"] = float(Decimal(str(stats.get("total_spent", 0))) + cost_per_dialogue)
        stats["spent_on_dialogues"] = float(Decimal(str(stats.get("spent_on_dialogues", 0))) + cost_per_dialogue)
        settings_obj.stats = stats

        if settings_obj.balance < settings_obj.low_balance_threshold and not settings_obj.low_limit_notified:
            await mq.publish("tg_alerts", {
                "type": "system",
                "text": f"📉 **Внимание!** Баланс аккаунта **{account.name}** близок к нулю: {settings_obj.balance} руб.",
                "alert_type": "balance"
            })
            settings_obj.low_limit_notified = True
        elif settings_obj.balance >= settings_obj.low_balance_threshold:
            settings_obj.low_limit_notified = False

        # --- ПОДГОТОВКА СИСТЕМНОЙ КОМАНДЫ (UTC) ---
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        
        if trigger_source == "avito_search_found":
            cmd_content = "[SYSTEM COMMAND] Ты нашел кандидата на вакансию Поздоровайся и предложи задать вопросы"
        else:
            cmd_content = "[SYSTEM COMMAND] Кандидат откликнулся на вакансию. Поздоровайся и предложи задать вопросы"

        initial_history = [{
            'message_id': f'no_msg_{int(now_utc.timestamp())}_{chat_id[-5:]}',
            'role': 'user',
            'content': cmd_content,
            'timestamp_utc': now_utc.isoformat()
        }]

        # СОЗДАНИЕ ДИАЛОГА С НАЧАЛЬНОЙ ИСТОРИЕЙ
        dialogue = Dialogue(
            external_chat_id=chat_id, account_id=account.id, candidate_id=candidate.id,
            vacancy_id=job.id if job else None, history=initial_history,
            current_state="initial", status="new",
            last_message_at=now_utc
        )
        db.add(dialogue)
        
        try:
            await db.flush() 
        except Exception as e:
            logger.warning(f"Race condition при создании диалога: {e}. Откат.")
            await db.rollback()
            raise e
        
        await log_event(
            db=db,
            dialogue=dialogue,
            event_type='lead_created',
            event_data={"cost": float(cost_per_dialogue), "trigger": trigger_source}
        )

        await self._update_history_only(dialogue, account, chat_id, db)
        return dialogue

    async def _update_history_only(self, dialogue: Dialogue, account: Account, chat_id: str, db: AsyncSession):
        # ВРЕМЕННО ОТКЛЮЧЕНО: по просьбе оставляем только вебхуки
        logger.info(f"ВРЕМЕННО ОТКЛЮЧЕНО: Синхронизация истории через API для чата {chat_id}")
        return

        try:
            user_id = account.auth_data.get("user_id", "me")
            api_messages = await avito.get_chat_messages(user_id, chat_id, account, db)
            one_hour_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
            existing_ids = {str(m.get("message_id")) for m in (dialogue.history or [])}
            new_history = list(dialogue.history or [])
            changed = False
            
            for msg in api_messages:
                m_id = str(msg.get("id"))
                msg_ts = datetime.datetime.fromtimestamp(msg.get("created"), datetime.timezone.utc)
                if msg_ts < one_hour_ago:
                    continue # Пропускаем сообщение, если оно старше часа
                
                if m_id not in existing_ids:
                    # Определяем роль
                    direction = msg.get("direction")
                    role = "user" if direction == "in" else "assistant"
                    
                    # ИСПОЛЬЗУЕМ ОБЩИЙ ПАРСЕР (который мы исправили в шаге 1)
                    text_content = self._parse_message_content(msg.get("content", {}))
                    if text_content.strip().startswith("[Системное сообщение]"):
                        continue # Пропускаем это сообщение и идем к следующему
                    # -----------------------------
                    entry = {
                        "role": role,
                        "content": text_content,
                        "message_id": m_id,
                        "timestamp_utc": datetime.datetime.fromtimestamp(
                            msg.get("created"), datetime.timezone.utc
                        ).isoformat()
                    }
                    
                    if role == "assistant":
                        entry["state"] = dialogue.current_state
                        entry["extracted_data"] = {}

                    new_history.append(entry)
                    changed = True
            
            if changed:
                new_history.sort(key=lambda x: x.get("timestamp_utc") or "0000-01-01T00:00:00+00:00")
                dialogue.history = new_history
                dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
                
        except Exception as e:
            error_msg = f"💥 Ошибка синхронизации истории для чата {chat_id}: {e}"
            logger.exception("💥 Ошибка синхронизации истории")
            await mq.publish("tg_alerts", {"type": "system", "text": error_msg})
            raise e

# Синглтон сервиса
avito_connector = AvitoConnectorService()
