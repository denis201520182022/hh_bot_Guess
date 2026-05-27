# app\output_chanels\talantix\talantix_crm.py
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

    async def get_token(self, account: Account, db: AsyncSession, force_refresh: bool = False) -> str:
        """
        Получение/обновление access_token.
        """
        try:
            auth_data = TalantixApiCreds.model_validate(account.auth_data)
        except ValidationError:
            self.logger.error(f"Ошибка: Токен у аккаунта {account.id} отсутсвует")
            raise

        now_ts = int(datetime.datetime.now(datetime.UTC).timestamp())

        # Проверяем валидность текущего токена (с запасом 300 секунд)
        if not force_refresh and auth_data.expires_in + auth_data.created_at > now_ts + 300:
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
            
            await self._send_alert(
                f"✅ **Talantix OAuth Refresh Success**\n"
                f"Аккаунт: {account.name} (ID: {account.id})\n"
                f"Статус: Новый access_token получен и сохранен."
            )
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
        
        # Первая попытка
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
                    "POST", url, json=data, headers=headers, **kwargs
                )

            # Обработка 401 Unauthorized (истекший токен)
            if resp.status_code == 401:
                try:
                    error_data = resp.json()
                    if error_data.get("type") == "invalid_token" and error_data.get("detail") == "token_expired":
                        self.logger.warning(f"🔄 Токен Talantix для {account.name} протух (401 token_expired). Принудительное обновление...")
                        
                        # Принудительно обновляем токен
                        token = await self.get_token(account, db, force_refresh=True)
                        headers["Authorization"] = f"Bearer {token}"
                        
                        # Повторная попытка
                        async with DistributedSemaphore(
                            name="talantix_api_global", limit=TALANTIX_CONCURRENCY_LIMIT
                        ):
                            resp = await self.http_client.request(
                                "POST", url, json=data, headers=headers, **kwargs
                            )
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке 401 Talantix: {e}")

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
        self, db: AsyncSession, platform: str, name: str | None = None
    ) -> Account | None:
        """Получение аккаунта из БД по платформе и имени."""
        # Если имя не передано, используем дефолт для обратной совместимости
        
        # Если искали по дефолту, пробуем найти по имени, если нет - по дефолту
        stmt = select(Account).where(Account.platform == platform, Account.is_active == True)
        if name:
            stmt = stmt.where(Account.name == name)
        else:
            stmt = stmt.where(Account.name == "Talantix API Integration")
            
        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    # --- МЕТОДЫ ДЛЯ КАНДИДАТОВ ---

    async def upsert_candidate(self, candidate_data: dict, account_name: str | None = None) -> int | None:
        """
        Создание или обновление кандидата в Talantix.
        """
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_api", account_name)
            if not account:
                self.logger.error(f"Аккаунт talantix_api (name={account_name}) не найден")
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

    async def transfer_candidate_to_stage(self, person_id: int, workflow_status_id: int, account_name: str | None = None) -> bool:
        """Перевод кандидата на этап вакансии в Talantix."""
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_api", account_name)
            if not account:
                self.logger.error(f"Аккаунт talantix_api (name={account_name}) не найден")
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

    async def ensure_candidate_response(self, person_id: int, vacancy_id: int, db: AsyncSession, account: Account | None = None, account_name: str | None = None) -> bool:
        """Прикрепление кандидата к вакансии в Talantix, если отклик отсутствует."""
        if not account:
            account = await self._get_account(db, "talantix_api", account_name)
        if not account:
            self.logger.error(f"ensure_candidate_response: Аккаунт talantix_api (name={account_name}) не найден")
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
        query {
          persons(filter: {search: "%s", searchFrom: CONTACTS}) {
            items {
              id
            }
          }
        }
        """ % normalized_phone
        
        response = await self.graphql_client._send_graphql_request(query, None, account, db)
        items = ((response.data or {}).get("persons") or {}).get("items") or []

        if items:
            return int(items[0]["id"])

        return None

    async def find_persons_by_phone(
        self, phone: str, db: AsyncSession, account: Account | None = None, account_name: str | None = None
    ) -> list[dict]:
        """
        Поиск всех кандидатов по номеру телефона.
        """
        if not account:
            account = await self._get_account(db, "talantix_api", account_name)
        
        if not account:
            self.logger.error(f"❌ Аккаунт talantix_api (name={account_name}) не найден. Пропуск поиска.")
            return []

        normalized_phone = self._normalize_phone(phone)
        if not normalized_phone:
            self.logger.warning("find_persons_by_phone: пустой номер телефона")
            return []

        self.logger.info(f"🔍 [Account: {account.name}] Поиск кандидатов в Talantix по номеру: {phone}")

        query = """
        query {
          persons(filter: {search: "%s", searchFrom: CONTACTS}) {
            items {
              id
              firstName
              lastName
            }
          }
        }
        """ % normalized_phone
        
        response = await self.graphql_client._send_graphql_request(
            query, None, account, db
        )
        
        if response.errors:
            self.logger.error(f"❌ Ошибка поиска в Talantix: {response.errors}")
            return []
        
        persons_data = ((response.data or {}).get("persons") or {}).get("items") or []
        self.logger.info(f"✅ Найдено {len(persons_data)} кандидатов в Talantix")
        
        return persons_data

    async def get_person_resume_ids(
        self, person_id: int, db: AsyncSession, account: Account | None = None, account_name: str | None = None
    ) -> list[str]:
        """
        Получение списка ID резюме (externalId) кандидата.
        """
        if not account:
            account = await self._get_account(db, "talantix_api", account_name)
        if not account:
            return []

        query = """
        query {
          person(id: %d) {
            ... on PersonItem {
              id
              firstName
              resumes {
                items {
                  ... on StructuredResume {
                    externalId
                    link
                  }
                }
              }
            }
          }
        }
        """ % person_id
        
        response = await self.graphql_client._send_graphql_request(
            query, None, account, db
        )
        
        if response.errors:
            self.logger.error(f"❌ Ошибка получения резюме для {person_id}: {response.errors}")
            return []
        
        person_data = (response.data or {}).get("person") or {}
        resumes = ((person_data.get("resumes") or {}).get("items")) or []
        
        return [r.get("externalId") for r in resumes if r.get("externalId")]

    async def get_person_responses(
        self, person_id: int, db: AsyncSession, account: Account | None = None, account_name: str | None = None
    ) -> dict | None:
        """
        Получение информации о кандидате и его откликах (GetFinalData).
        """
        if not account:
            account = await self._get_account(db, "talantix_api", account_name)
        if not account:
            return None

        self.logger.info(f"🔍 [Account: {account.name}] Получение данных кандидата Talantix (person_id={person_id})")

        query = """
        query GetFinalData {
          person(id: %d) {
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
        """ % person_id
        
        response = await self.graphql_client._send_graphql_request(
            query, None, account, db
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

    async def get_vacancy_external_ids(
        self, vacancy_id: int, db: AsyncSession, account: Account | None = None, account_name: str | None = None
    ) -> list[str]:
        """
        Получение списка ID привязанных вакансий HH для вакансии Talantix.
        """
        if not account:
            account = await self._get_account(db, "talantix_api", account_name)
        if not account:
            return []

        query = """
        query GetVacancyDetails {
          vacancy(id: %d) {
            ... on VacancyItem {
              id
              title
              externalVacancies {
                hh {
                  items {
                    externalId
                    name
                    link
                    status
                  }
                }
              }
            }
          }
        }
        """ % vacancy_id

        response = await self.graphql_client._send_graphql_request(
            query, None, account, db
        )

        if response.errors:
            self.logger.error(f"❌ Ошибка получения деталей вакансии {vacancy_id}: {response.errors}")
            return []

        vacancy_data = (response.data or {}).get("vacancy") or {}
        hh_items = ((vacancy_data.get("externalVacancies") or {}).get("hh") or {}).get("items") or []
        
        return [v.get("externalId") for v in hh_items if v.get("externalId")]

    async def get_vacancy_managers(
        self, vacancy_id: int, db: AsyncSession, account: Account | None = None, account_name: str | None = None
    ) -> list[dict]:
        """
        Получение списка менеджеров для конкретной вакансии (GetVacancyManagers).
        """
        if not account:
            account = await self._get_account(db, "talantix_api", account_name)
        if not account:
            return []

        query = """
        query GetVacancyManagers {
          vacancy(id: %d) {
            ... on VacancyItem {
              id
              title
              vacancyManagers {
                items {
                  vacancyRole
                  manager {
                    id
                    firstName
                    lastName
                    middleName
                  }
                }
              }
            }
          }
        }
        """ % vacancy_id

        response = await self.graphql_client._send_graphql_request(
            query, None, account, db
        )

        if response.errors:
            self.logger.error(f"❌ Ошибка получения менеджеров вакансии {vacancy_id}: {response.errors}")
            return []

        vacancy_data = (response.data or {}).get("vacancy") or {}
        manager_items = ((vacancy_data.get("vacancyManagers") or {}).get("items") or [])
        
        result = []
        for item in manager_items:
            manager = item.get("manager") or {}
            result.append({
                'manager_id': manager.get('id'),
                'vacancyRole': item.get('vacancyRole'),
                'firstName': manager.get('firstName'),
                'lastName': manager.get('lastName'),
                'middleName': manager.get('middleName')
            })
        
        return result

    async def create_person_comment(
        self,
        person_id: int,
        text: str,
        db: AsyncSession,
        account: Account | None = None,
        account_name: str | None = None,
        visible_for_all: bool = True,
        vacancy_id: int | None = None
    ) -> int | None:
        """
        Создание комментария к кандидату в Talantix.
        
        Args:
            person_id: ID кандидата в Talantix
            text: Текст комментария
            db: Сессия БД
            account: Аккаунт Talantix
            account_name: Имя аккаунта Talantix
            visible_for_all: Видимость комментария (True = виден всем)
            vacancy_id: ID вакансии в Talantix (опционально)
            
        Returns:
            ID созданного комментария или None при ошибке
        """
        if not account:
            account = await self._get_account(db, "talantix_api", account_name)
        
        if not account:
            self.logger.error(f"create_person_comment: Аккаунт talantix_api (name={account_name}) не найден")
            return None

        self.logger.info(f"💬 [Account: {account.name}] Создание комментария для кандидата {person_id} в Talantix")

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

        if vacancy_id:
            variables["commentCreate"]["vacancyId"] = int(vacancy_id)
        
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
        Формирует текст комментария для Talantix в формате HTML.
        
        Args:
            event_type: 'qualified', 'rescheduled', 'cancelled', 'rejected', 'silence'
            candidate_name: Имя кандидата
            vacancy_title: Название вакансии
            profile_data: Профиль кандидата
            interview_date/time: Дата и время собеседования
            old_date/time: Старая дата/время (для переноса)
            reason: Причина (для отмены/отказа)
            platform: Платформа (HH, Avito)
        """
        platform_str = f" ({platform.upper()})" if platform else ""
        profile = profile_data or {}
        
        # Перевод значений
        emp_type = profile.get('employment_type')
        emp_label = "Полная" if emp_type == "full" else "Частичная" if emp_type == "part" else "—"
        
        hours = profile.get('ready_20_40_hours')
        hours_label = "✅ Да" if hours == "yes" else "❌ Нет" if hours == "no" else "—"
        
        shift = profile.get('shift_preference')
        shift_map = {"morning": "Утро", "evening": "Вечер", "any": "Любая"}
        shift_label = shift_map.get(shift, "—")
        
        contract = profile.get('employment_contract_ready')
        contract_label = "✅ Да" if contract == "yes" else "❌ Нет" if contract == "no" else "—"
        
        military = profile.get('has_military_document')
        military_label = "✅ Да" if military == "yes" else "❌ Нет" if military == "no" else "—"
        
        # Формируем блок анкеты
        profile_items = [
            f"<li><b>Возраст:</b> {profile.get('age', '—')}</li>",
            f"<li><b>Гражданство:</b> {profile.get('citizenship', '—')}</li>",
            f"<li><b>Занятость:</b> {emp_label}</li>",
        ]
        
        if emp_type == "part":
            profile_items.append(f"<li><b>Готов 20-40ч:</b> {hours_label}</li>")
        elif emp_type == "full":
            profile_items.append(f"<li><b>Смена:</b> {shift_label}</li>")
        
        profile_items.append(f"<li><b>Оформление ТК:</b> {contract_label}</li>")
        profile_items.append(f"<li><b>Военный билет:</b> {military_label}</li>")
        
        profile_html = "<b>Анкета кандидата:</b><ul>" + "".join(profile_items) + "</ul>"
        
        # Заголовки в зависимости от типа события
        if event_type == 'qualified':
            header = f"🚀 <b>Запись на собеседование{platform_str}</b>"
            body = (
                f"{header}<br/><br/>"
                f"📌 <b>Вакансия:</b> {vacancy_title}<br/><br/>"
                f"{profile_html}"
                f"📅 <b>Собеседование:</b> {interview_date} в {interview_time}<br/>"
                f"Рекрутер и Директор оповещены."
            )
        
        elif event_type == 'rescheduled':
            header = f"🔄 <b>Перенос собеседования{platform_str}</b>"
            body = (
                f"{header}<br/><br/>"
                f"📌 <b>Вакансия:</b> {vacancy_title}<br/><br/>"
                f"{profile_html}"
                f"📅 <b>Было:</b> {old_date} в {old_time}<br/>"
                f"📅 <b>Стало:</b> {interview_date} в {interview_time}"
            )
        
        elif event_type == 'cancelled':
            header = f"❌ <b>Отмена собеседования{platform_str}</b>"
            reason_text = f"<br/>📝 <b>Причина:</b> {reason}" if reason else ""
            body = (
                f"{header}<br/><br/>"
                f"📌 <b>Вакансия:</b> {vacancy_title}<br/><br/>"
                f"🗓 <b>Отменено:</b> {interview_date} в {interview_time}"
                f"{reason_text}"
            )
        
        elif event_type == 'rejected':
            header = f"⛔ <b>Отказ кандидату{platform_str}</b>"
            reason_text = f"<br/>📝 <b>Причина:</b> {reason}" if reason else ""
            body = (
                f"{header}<br/><br/>"
                f"📌 <b>Вакансия:</b> {vacancy_title}<br/><br/>"
                f"{profile_html}"
                f"{reason_text}"
            )
        
        elif event_type == 'silence':
            header = f"⏰ <b>Напоминание отправлено{platform_str}</b>"
            body = (
                f"{header}<br/><br/>"
                f"📌 <b>Вакансия:</b> {vacancy_title}<br/><br/>"
                f"Кандидат не отвечает на сообщения"
            )
        
        else:
            body = f"Событие: {event_type}<br/>Вакансия: {vacancy_title}"
        
        return body

    async def notify_talantix_comment(
        self,
        dialogue: 'Dialogue',
        event_type: str,
        db: AsyncSession,
        reason: str = None,
        account_name: str | None = None
    ):
        """
        Универсальный метод для создания комментария в Talantix.
        Находит person_id в metadata_json диалога и создаёт комментарий.
        
        Args:
            dialogue: Объект диалога из БД
            event_type: Тип события ('qualified', 'rescheduled', 'cancelled', 'rejected', 'silence')
            db: Сессия БД
            reason: Причина (для отмены/отказа)
            account_name: Имя аккаунта Talantix
        """
        # Если имя не передано, берем из аккаунта диалога (HH аккаунта)
        if not account_name and dialogue.account:
            account_name = dialogue.account.name

        # Получаем данные из metadata_json
        metadata = dialogue.metadata_json or {}
        talantix_data = metadata.get('talantix') or {}
        person_id = talantix_data.get('person_id')
        vacancy_id = talantix_data.get('vacancy_id')
        
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
        talantix_account = await self._get_account(db, "talantix_api", account_name)
        if not talantix_account:
            self.logger.warning(f"⚠️ Аккаунт talantix_api (name={account_name}) не найден. Пропуск комментария.")
            return None
        
        # Создаём комментарий
        return await self.create_person_comment(
            person_id=person_id,
            text=comment_text,
            account=talantix_account,
            db=db,
            vacancy_id=vacancy_id
        )

# Глобальный экземпляр сервиса
talantix_crm_service = TalantixService()