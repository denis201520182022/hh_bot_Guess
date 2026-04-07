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

# Глобальный экземпляр сервиса
talantix_crm_service = TalantixService()