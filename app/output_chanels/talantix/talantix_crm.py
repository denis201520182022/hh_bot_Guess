import asyncio
import datetime
import logging
import os
import re
from typing import Any

import httpx
from pydantic import BaseModel, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.core.rabbitmq import mq
from app.db.models import Account
from app.db.session import AsyncSessionLocal
from app.utils.redis_lock import DistributedSemaphore, acquire_lock, release_lock

TALANTIX_API_ENDPOINT = os.getenv("TALANTIX_API_ENDPOINT", "https://api.talantix.ru")
TALANTIX_CONCURRENCY_LIMIT = 5
MSK_TZ = datetime.timezone(datetime.timedelta(hours=3))

class GraphQLResponse(BaseModel):
    data: dict | None
    errors: list | None

class TalantixApiCreds(BaseModel):
    name: str
    access_token: str
    expires_in: int
    refresh_token: str
    refresh_token_expires_in: int
    token_type: str
    created_at: int

class TalantixClient:
    def __init__(self):
        self.base_url = TALANTIX_API_ENDPOINT
        self.logger = logging.getLogger("talantix.client")
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def _send_alert(self, text: str):
        try:
            await mq.publish("tg_alerts", {"type": "system", "text": text})
        except Exception:
            self.logger.error("Не удалось отправить алерт")

    async def get_token(self, account: Account, db: AsyncSession) -> str:
        """
        Получение/обновление access_token.
        """
        try:
            auth_data = TalantixApiCreds.model_validate(account.auth_data)
        except ValidationError:
            self.logger.error(f"Ошибка: Токен у аккаунта {account.id} отсутсвует")
            raise

        now_ts = datetime.datetime.now(datetime.UTC).timestamp()

        # Проверяем валидность текущего токена (с запасом 300 секунд)
        if auth_data.expires_in + auth_data.created_at < now_ts + 300:
            return auth_data.access_token

        lock_key = f"talantix_token_lock:{account.id}"
        if not await acquire_lock(lock_key, timeout=20):
            self.logger.info(f"⏳ Обновление токена для {account.id} уже в процессе. Ожидание...")
            await asyncio.sleep(3)
            await db.refresh(account)
            return await self.get_token(account, db)

        try:
            self.logger.info(f"🔑 Обновление токена для аккаунта {account.name} (ID: {account.id})")

            auth_url = self.base_url + "/oauth/token"
            payload = {"grant_type": "refresh_token", "refresh_token": auth_data.refresh_token}

            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "HRBot/1.0 (hr-bot@za-bota.com)",
            }

            resp = await self.http_client.post(auth_url, data=payload, headers=headers)
            resp.raise_for_status()

            data = resp.json()
            data["created_at"] = now_ts
            token_data = TalantixApiCreds.model_validate(data)

            account.auth_data = token_data.model_dump()
            await db.commit()

            self.logger.info(f"✅ Токен успешно получен для аккаунта {account.id}")
            return token_data.access_token

        except httpx.HTTPStatusError as e:
            error_msg = f"❌ OAuth API Error {e.response.status_code}: {e.response.text}"
            self.logger.error(error_msg, exc_info=True)
            await self._send_alert(error_msg)
            raise
        except Exception as e:
            error_msg = (
                f"❌ КРИТИЧЕСКАЯ ОШИБКА АВТОРИЗАЦИИ Talantix для аккаунта {account.name}: {e}"
            )
            self.logger.error(error_msg, exc_info=True)
            await self._send_alert(error_msg)
            raise
        finally:
            await release_lock(lock_key)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10),
        retry=retry_if_exception_type(httpx.HTTPError),
        reraise=True,
    )
    async def _send_graphql_request(
        self, query: str, variables: dict | None, account: Account, db: AsyncSession, **kwargs
    ) -> GraphQLResponse:
        url = self.base_url + "/graphql"
        token = await self.get_token(account, db)

        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        headers["User-Agent"] = "HRBot/1.0 (hr-bot@za-bota.com)"

        try:
            data = {"query": query, "variables": variables or {}}
            async with DistributedSemaphore(
                name="talantix_api_global", limit=TALANTIX_CONCURRENCY_LIMIT
            ):
                resp = await self.http_client.request(
                    "POST", url, data=data, headers=headers, **kwargs
                )

            resp.raise_for_status()
            return GraphQLResponse.model_validate(resp.json())

        except httpx.HTTPStatusError as e:
            error_msg = f"❌ API Error {e.response.status_code} на {url}: {e.response.text}"
            self.logger.error(error_msg)
            raise

    async def get_me(self, account: Account, db: AsyncSession):
        query = """
                query {
                  me {
                    id
                    firstName
                    lastName
                  }
                }
                """

        return await self._send_graphql_request(query, None, account, db)


class TalantixService:
    """
    Универсальный сервис для работы с Talantix CRM.
    """

    def __init__(self):
        self.graphql_client = TalantixClient()
        self.logger = logging.getLogger("talantix.service")

    async def _get_account(
        self, db: AsyncSession, platform: str
    ) -> Account | None:
        """Получение аккаунта из БД по платформе."""
        result = await db.execute(
            select(Account).where(Account.platform == platform, Account.is_active == True)
        )
        return result.scalar_one_or_none()

    # --- МЕТОДЫ ДЛЯ КАНДИДАТОВ ---

    async def upsert_candidate(self, candidate_data: dict) -> int | None:
        """
        Создание или обновление кандидата в Talantix.
        """
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_api")
            if not account:
                self.logger.error("Аккаунт talantix_api не найден")
                return None

            full_name = (candidate_data.get("full_name") or "").strip()
            first_name, last_name, middle_name = self._split_full_name(full_name)
            raw_phone = (candidate_data.get("phone") or "").strip()
            normalized_phone = self._normalize_phone(raw_phone)
            vacancy_id = candidate_data.get("vacancy_id")

            if not first_name:
                self.logger.error("upsert_candidate: пустое имя кандидата")
                return None

            person_id = await self._find_person_id_by_phone(normalized_phone, account, db)

            person_input = {
                "firstName": first_name,
                "lastName": last_name,
                "middleName": middle_name,
                "contacts": (
                    [{"type": "cell", "value": raw_phone}]
                    if raw_phone
                    else []
                ),
            }
            if vacancy_id is not None:
                person_input["vacancyIds"] = [int(vacancy_id)]

            if person_id:
                mutation = """
                mutation EditPerson($personEdit: PersonEditInput!) {
                  editPerson(personEdit: $personEdit) {
                    __typename
                    ... on PersonItem { id }
                    ... on PersonSaveError { errorType message }
                  }
                }
                """
                variables = {
                    "personEdit": {
                        "id": person_id,
                        **person_input,
                    }
                }
            else:
                mutation = """
                mutation CreatePerson($personCreate: PersonCreateInput!) {
                  createPerson(personCreate: $personCreate) {
                    __typename
                    ... on PersonItem { id }
                    ... on PersonSaveError { errorType message }
                  }
                }
                """
                variables = {"personCreate": person_input}

            response = await self.graphql_client._send_graphql_request(
                mutation, variables, account, db
            )
            payload_key = "editPerson" if person_id else "createPerson"
            payload = (response.data or {}).get(payload_key, {})

            if payload.get("__typename") == "PersonItem":
                saved_person_id = payload.get("id")
                self.logger.info(
                    "✅ Talantix upsert кандидата выполнен (person_id=%s)", saved_person_id
                )
                return int(saved_person_id) if saved_person_id is not None else None

            self.logger.error("upsert_candidate: ошибка Talantix: %s", payload)
            return None

    async def transfer_candidate_to_stage(self, person_id: int, workflow_status_id: int) -> bool:
        """Перевод кандидата на этап вакансии в Talantix."""
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_api")
            if not account:
                self.logger.error("Аккаунт talantix_api не найден")
                return False

            mutation = """
            mutation TransferPersonsToWorkflowStatus($personIds: [Int!]!, $workflowStatusId: Int!) {
              transferPersonsToWorkflowStatus(personIds: $personIds, workflowStatusId: $workflowStatusId) {
                __typename
                ... on PersonsToWorkflowStatusTransferredSuccess { message }
                ... on PersonsToWorkflowStatusTransferError { errorType message }
              }
            }
            """
            variables = {
                "personIds": [int(person_id)],
                "workflowStatusId": int(workflow_status_id),
            }

            response = await self.graphql_client._send_graphql_request(
                mutation, variables, account, db
            )
            payload = (response.data or {}).get("transferPersonsToWorkflowStatus", {})
            if payload.get("__typename") == "PersonsToWorkflowStatusTransferredSuccess":
                self.logger.info(
                    "✅ Talantix этап обновлен (person_id=%s, workflow_status_id=%s)",
                    person_id,
                    workflow_status_id,
                )
                return True

            self.logger.error(
                "transfer_candidate_to_stage: ошибка Talantix: %s", payload
            )
            return False

    async def ensure_candidate_response(self, person_id: int, vacancy_id: int) -> bool:
        """Прикрепление кандидата к вакансии в Talantix, если отклик отсутствует."""
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_api")
            if not account:
                self.logger.error("Аккаунт talantix_api не найден")
                return False

            mutation = """
            mutation CreateResponses($vacancyIds: [Int!]!, $personIds: [Int!]!) {
              createResponses(vacancyIds: $vacancyIds, personIds: $personIds) {
                __typename
                ... on ResponsesCreatedSuccess { message }
                ... on ResponsesCreatedError { errorType message }
              }
            }
            """
            variables = {
                "vacancyIds": [int(vacancy_id)],
                "personIds": [int(person_id)],
            }

            response = await self.graphql_client._send_graphql_request(
                mutation, variables, account, db
            )
            payload = (response.data or {}).get("createResponses", {})
            if payload.get("__typename") == "ResponsesCreatedSuccess":
                self.logger.info(
                    "✅ Talantix отклик создан (person_id=%s, vacancy_id=%s)",
                    person_id,
                    vacancy_id,
                )
                return True

            self.logger.warning("ensure_candidate_response: ответ Talantix: %s", payload)
            return False

    @staticmethod
    def _split_full_name(full_name: str) -> tuple[str, str | None, str | None]:
        parts = [p for p in full_name.split() if p]
        if not parts:
            return "", None, None
        first_name = parts[0]
        last_name = parts[1] if len(parts) > 1 else None
        middle_name = parts[2] if len(parts) > 2 else None
        return first_name, last_name, middle_name

    @staticmethod
    def _normalize_phone(phone: str | None) -> str:
        if not phone:
            return ""
        return re.sub(r"\D+", "", phone)

    async def _find_person_id_by_phone(
        self, normalized_phone: str, account: Account, db: AsyncSession
    ) -> int | None:
        if not normalized_phone:
            return None

        query = """
        query PersonsByPhone($filter: PersonFilterInput, $first: Int) {
          persons(filter: $filter, first: $first) {
            items {
              id
              contacts {
                items {
                  type
                  value
                }
              }
            }
          }
        }
        """
        variables = {
            "filter": {
                "search": normalized_phone,
                "searchFrom": ["CONTACTS"],
            },
            "first": 25,
        }
        response = await self.graphql_client._send_graphql_request(query, variables, account, db)
        items = ((response.data or {}).get("persons") or {}).get("items") or []

        for person in items:
            contacts = ((person.get("contacts") or {}).get("items")) or []
            for contact in contacts:
                contact_type = (contact.get("type") or "").lower()
                value = self._normalize_phone(contact.get("value"))
                if contact_type in {"cell", "home"} and value.endswith(normalized_phone):
                    return int(person["id"])

        return None

    async def find_persons_by_phone(
        self, phone: str, account: Account, db: AsyncSession
    ) -> list[dict]:
        """
        Поиск всех кандидатов по номеру телефона.
        Возвращает список кандидатов с базовой информацией.
        
        Args:
            phone: Номер телефона (в любом формате)
            account: Аккаунт Talantix
            db: Сессия БД
            
        Returns:
            Список словарей с полями: id, firstName, lastName, area, source
        """
        normalized_phone = self._normalize_phone(phone)
        if not normalized_phone:
            self.logger.warning("find_persons_by_phone: пустой номер телефона")
            return []

        self.logger.info(f"🔍 Поиск кандидатов в Talantix по номеру: {phone}")

        query = """
        query JustCandidate($phone: String!) {
          persons(filter: {search: $phone, searchFrom: CONTACTS}) {
            items {
              id
              firstName
              lastName
              area {
                name
              }
              source {
                name
              }
            }
          }
        }
        """
        variables = {"phone": phone}
        
        response = await self.graphql_client._send_graphql_request(
            query, variables, account, db
        )
        
        if response.errors:
            self.logger.error(f"❌ Ошибка поиска в Talantix: {response.errors}")
            return []
        
        persons_data = ((response.data or {}).get("persons") or {}).get("items") or []
        self.logger.info(f"✅ Найдено {len(persons_data)} кандидатов в Talantix")
        
        return persons_data

    async def get_person_responses(
        self, person_id: int, account: Account, db: AsyncSession
    ) -> dict | None:
        """
        Получение информации о кандидате и его откликах.
        
        Args:
            person_id: ID кандидата в Talantix
            account: Аккаунт Talantix
            db: Сессия БД
            
        Returns:
            Словарь с данными кандидата и его откликами или None при ошибке
        """
        self.logger.info(f"🔍 Получение данных кандидата Talantix (person_id={person_id})")

        query = """
        query GetFinalData($personId: Int!) {
          person(id: $personId) {
            ... on PersonItem {
              id
              firstName
              lastName
              responses {
                items {
                  ... on ResponseItem {
                    workflowStatus {
                      ... on WorkflowStatusItem {
                        vacancy {
                          ... on VacancyItem {
                            id
                            title
                            vacancyManagers {
                              items {
                                vacancyRole
                                manager {
                                  firstName
                                  lastName
                                  email
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        variables = {"personId": person_id}
        
        response = await self.graphql_client._send_graphql_request(
            query, variables, account, db
        )
        
        if response.errors:
            self.logger.error(f"❌ Ошибка получения данных кандидата {person_id}: {response.errors}")
            return None
        
        person_data = (response.data or {}).get("person")
        if not person_data:
            self.logger.warning(f"⚠️ Кандидат {person_id} не найден")
            return None
        
        self.logger.info(f"✅ Получены данные кандидата {person_id}")
        return person_data

    async def get_all_vacancies_with_managers(
        self, account: Account, db: AsyncSession
    ) -> dict[int, list[dict]]:
        """
        Получение списка всех вакансий с их менеджерами.
        
        Args:
            account: Аккаунт Talantix
            db: Сессия БД
            
        Returns:
            Словарь {vacancy_id: [managers]} где managers — список с полями:
            manager_id, firstName, lastName, middleName, vacancyRole
        """
        self.logger.info("🔍 Получение списка всех вакансий с менеджерами из Talantix")

        query = """
        query VacanciesManagerList {
          vacancies {
            items {
              id
              vacancyManagers {
                items {
                  manager {
                    id
                    lastName
                    firstName
                    middleName
                  }
                  vacancyRole
                }
              }
            }
          }
        }
        """
        
        response = await self.graphql_client._send_graphql_request(
            query, None, account, db
        )
        
        if response.errors:
            self.logger.error(f"❌ Ошибка получения вакансий: {response.errors}")
            return {}
        
        vacancies_data = ((response.data or {}).get("vacancies") or {}).get("items") or []
        
        # Преобразуем в удобный словарь {vacancy_id: [managers]}
        result = {}
        for vacancy in vacancies_data:
            vacancy_id = vacancy.get('id')
            if not vacancy_id:
                continue
            
            managers_list = []
            vacancy_managers = ((vacancy.get('vacancyManagers') or {}).get('items') or [])
            
            for manager_item in vacancy_managers:
                manager = manager_item.get('manager') or {}
                managers_list.append({
                    'manager_id': manager.get('id'),
                    'firstName': manager.get('firstName'),
                    'lastName': manager.get('lastName'),
                    'middleName': manager.get('middleName'),
                    'vacancyRole': manager_item.get('vacancyRole')
                })
            
            result[vacancy_id] = managers_list
        
        self.logger.info(f"✅ Получено {len(result)} вакансий с менеджерами")
        return result

    async def create_person_comment(
        self,
        person_id: int,
        text: str,
        account: Account,
        db: AsyncSession,
        visible_for_all: bool = True
    ) -> int | None:
        """
        Создание комментария к кандидату в Talantix.
        
        Args:
            person_id: ID кандидата в Talantix
            text: Текст комментария
            account: Аккаунт Talantix
            db: Сессия БД
            visible_for_all: Видимость комментария (True = виден всем)
            
        Returns:
            ID созданного комментария или None при ошибке
        """
        self.logger.info(f"💬 Создание комментария для кандидата {person_id} в Talantix")

        mutation = """
        mutation CreatePersonComment($commentCreate: PersonCommentCreateInput!) {
          createPersonComment(commentCreate: $commentCreate) {
            __typename
            ... on Comment {
              id
            }
            ... on PersonCommentCreateError {
              errorType
              errors {
                field
                violatedConstraints
              }
            }
          }
        }
        """
        
        variables = {
            "commentCreate": {
                "personId": person_id,
                "text": text,
                "commentVisibility": {
                    "visibleForAll": visible_for_all
                }
            }
        }
        
        response = await self.graphql_client._send_graphql_request(
            mutation, variables, account, db
        )
        
        payload = (response.data or {}).get("createPersonComment")
        
        if not payload:
            self.logger.error(f"❌ Пустой ответ при создании комментария для {person_id}")
            return None
        
        typename = payload.get("__typename")
        
        if typename == "Comment":
            comment_id = payload.get("id")
            self.logger.info(f"✅ Комментарий создан для кандидата {person_id} (comment_id={comment_id})")
            return int(comment_id) if comment_id else None
        
        elif typename == "PersonCommentCreateError":
            errors = payload.get("errors", [])
            self.logger.error(
                f"❌ Ошибка создания комментария для {person_id}: {payload.get('errorType')}, errors={errors}"
            )
            return None
        
        else:
            self.logger.error(f"❌ Неизвестный тип ответа: {typename}")
            return None

    @staticmethod
    def format_comment_text(
        event_type: str,
        candidate_name: str,
        vacancy_title: str,
        profile_data: dict = None,
        interview_date: str = None,
        interview_time: str = None,
        old_date: str = None,
        old_time: str = None,
        reason: str = None,
        platform: str = None
    ) -> str:
        """
        Формирует текст комментария для Talantix в зависимости от типа события.
        Данные как в TG-карточке, но без ФИО и телефона.
        
        Args:
            event_type: 'qualified', 'rescheduled', 'cancelled', 'rejected', 'silence'
            candidate_name: Имя кандидата
            vacancy_title: Название вакансии
            profile_data: Профиль кандидата (age, citizenship, employment_type и т.д.)
            interview_date/time: Дата и время собеседования
            old_date/time: Старая дата/время (для переноса)
            reason: Причина (для отмены/отказа)
            platform: Платформа (HH, Avito)
        """
        platform_str = f" ({platform.upper()})" if platform else ""
        profile = profile_data or {}
        
        # Перевод значений как в TG-карточках
        emp_type = profile.get('employment_type')
        emp_label = "Полная" if emp_type == "full" else "Частичная" if emp_type == "part" else "—"
        
        hours = profile.get('ready_20_40_hours')
        hours_label = "✅ Да" if hours == "yes" else "❌ Нет" if hours == "no" else "—"
        
        shift = profile.get('shift_preference')
        shift_map = {"morning": "🌅 Утро", "evening": "🌆 Вечер", "any": "🔄 Любая"}
        shift_label = shift_map.get(shift, "—")
        
        contract = profile.get('employment_contract_ready')
        contract_label = "✅ Да" if contract == "yes" else "❌ Нет" if contract == "no" else "—"
        
        military = profile.get('has_military_document')
        military_label = "✅ Да" if military == "yes" else "❌ Нет" if military == "no" else "—"
        
        # Формируем блок анкеты
        profile_lines = [
            f"🎂 Возраст: {profile.get('age', '—')}",
            f"🌍 Гражданство: {profile.get('citizenship', '—')}",
            f"⏳ Занятость: {emp_label}",
        ]
        
        if emp_type == "part":
            profile_lines.append(f"⏱ Готов 20-40ч: {hours_label}")
        elif emp_type == "full":
            profile_lines.append(f"🕒 Смена: {shift_label}")
        
        profile_lines.append(f"📋 Оформление ТК: {contract_label}")
        profile_lines.append(f"🎖 Военный билет: {military_label}")
        
        profile_text = "\n".join(profile_lines)
        
        # Заголовки в зависимости от типа события
        if event_type == 'qualified':
            header = f"🚀 Запись на собеседование{platform_str}"
            body = (
                f"{header}\n\n"
                f"📌 Вакансия: {vacancy_title}\n\n"
                f"{profile_text}\n\n"
                f"📅 Собеседование: {interview_date} в {interview_time}\n"
                f"Рекрутер и Директор оповещены."
            )
        
        elif event_type == 'rescheduled':
            header = f"🔄 Перенос собеседования{platform_str}"
            body = (
                f"{header}\n\n"
                f"📌 Вакансия: {vacancy_title}\n\n"
                f"{profile_text}\n\n"
                f"📅 Было: {old_date} в {old_time}\n"
                f"📅 Стало: {interview_date} в {interview_time}"
            )
        
        elif event_type == 'cancelled':
            header = f"❌ Отмена собеседования{platform_str}"
            reason_text = f"\n📝 Причина: {reason}" if reason else ""
            body = (
                f"{header}\n\n"
                f"📌 Вакансия: {vacancy_title}\n\n"
                f"🗓 Отменено: {interview_date} в {interview_time}"
                f"{reason_text}"
            )
        
        elif event_type == 'rejected':
            header = f"⛔ Отказ кандидату{platform_str}"
            reason_text = f"\n📝 Причина: {reason}" if reason else ""
            body = (
                f"{header}\n\n"
                f"📌 Вакансия: {vacancy_title}\n\n"
                f"{profile_text}"
                f"{reason_text}"
            )
        
        elif event_type == 'silence':
            header = f"⏰ Напоминание отправлено{platform_str}"
            body = (
                f"{header}\n\n"
                f"📌 Вакансия: {vacancy_title}\n\n"
                f"Кандидат не отвечает на сообщения"
            )
        
        else:
            body = f"Событие: {event_type}\nВакансия: {vacancy_title}"
        
        return body

    async def notify_talantix_comment(
        self,
        dialogue: 'Dialogue',
        event_type: str,
        db: AsyncSession,
        reason: str = None
    ):
        """
        Универсальный метод для создания комментария в Talantix.
        Находит person_id в metadata_json диалога и создаёт комментарий.
        
        Args:
            dialogue: Объект диалога из БД
            event_type: Тип события ('qualified', 'rescheduled', 'cancelled', 'rejected', 'silence')
            db: Сессия БД
            reason: Причина (для отмены/отказа)
        """
        # Получаем person_id из metadata_json
        metadata = dialogue.metadata_json or {}
        talantix_data = metadata.get('talantix') or {}
        person_id = talantix_data.get('person_id')
        
        if not person_id:
            self.logger.debug(f"💬 Пропуск комментария Talantix: person_id не найден в диалоге {dialogue.id}")
            return None
        
        # Собираем данные для комментария
        candidate = dialogue.candidate
        vacancy = dialogue.vacancy
        profile = candidate.profile_data if candidate else {}
        meta = dialogue.metadata_json or {}
        
        comment_text = self.format_comment_text(
            event_type=event_type,
            candidate_name=candidate.full_name if candidate else 'Неизвестно',
            vacancy_title=vacancy.title if vacancy else 'Не указана',
            profile_data=profile,
            interview_date=meta.get('interview_date'),
            interview_time=meta.get('interview_time'),
            old_date=meta.get('old_interview_date'),
            old_time=meta.get('old_interview_time'),
            reason=reason,
            platform=dialogue.account.platform if dialogue.account else None
        )
        
        # Получаем аккаунт Talantix
        talantix_account = await self._get_account(db, "talantix_api")
        if not talantix_account:
            self.logger.warning("⚠️ Аккаунт talantix_api не найден. Пропуск комментария.")
            return None
        
        # Создаём комментарий
        return await self.create_person_comment(
            person_id=person_id,
            text=comment_text,
            account=talantix_account,
            db=db
        )

# Глобальный экземпляр сервиса
talantix_crm_service = TalantixService()