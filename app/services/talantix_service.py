# app\services\talantix_service.py
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

TALANTIX_INTERNAL_API_ENDPOINT = os.getenv("TALANTIX_INTERNAL_API_ENDPOINT", "https://talantix.ru")
TALANTIX_CONCURRENCY_LIMIT = 5
MSK_TZ = datetime.timezone(datetime.timedelta(hours=3))

class TalantixCalendClient:
    def __init__(self):
        self.base_url = TALANTIX_INTERNAL_API_ENDPOINT
        self.logger = logging.getLogger("talantix.calend_client")
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

    @staticmethod
    def _parse_cookies(cookies_str: str) -> dict[str, str]:
        """Парсит строку cookies в dict."""
        result = {}
        for part in cookies_str.split("; "):
            if "=" in part:
                key, value = part.split("=", 1)
                result[key] = value
        return result

    @staticmethod
    def _serialize_cookies(cookies: dict[str, str]) -> str:
        """Сериализует dict cookies в строку."""
        return "; ".join(f"{k}={v}" for k, v in cookies.items())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10),
        retry=retry_if_exception_type(httpx.HTTPError),
        reraise=True,
    )
    async def _send_request(
        self, method: str, path: str, account: Account, db: AsyncSession, **kwargs
    ):
        url = self.base_url + path

        cookies_str: str = account.auth_data.get("cookies", "")
        if not cookies_str:
            error_msg = f"❌ Ошибка: Cookies у аккаунта {account.id} отсутствуют. Внутренний API Talantix недоступен."
            self.logger.error(error_msg)
            await self._send_alert(error_msg)
            raise ValueError("Cookies не найдены")

        cookies = self._parse_cookies(cookies_str)

        headers = {
            "x-tms-api-version": "34",
            "x-xsrf-token": cookies.get("_xsrf", ""),
            "Cookie": cookies_str,
            "Accept": "application/json",
        }

        try:
            async with DistributedSemaphore(
                name="talantix_api_global", limit=TALANTIX_CONCURRENCY_LIMIT
            ):
                resp = await self.http_client.request(method, url, headers=headers, **kwargs)

            # Проверка авторизации (сессия могла протухнуть)
            if resp.status_code in [401, 403]:
                error_msg = (
                    f"🚨 **TALANTIX COOKIES EXPIRED**\n"
                    f"Аккаунт: {account.name} (ID: {account.id})\n"
                    f"Ошибка: {resp.status_code} Unauthorized/Forbidden\n"
                    f"Действие: Требуется ручное обновление TALANTIX_COOKIES в .env!"
                )
                self.logger.error(error_msg)
                await self._send_alert(error_msg)
                resp.raise_for_status()

            # Обновляем cookies из ответа
            new_cookies = cookies | dict(resp.cookies)
            account.auth_data = {"cookies": self._serialize_cookies(new_cookies)}

            await db.commit()

            resp.raise_for_status()
            return resp.json()

        except httpx.HTTPStatusError as e:
            error_msg = f"❌ Talantix Internal API Error {e.response.status_code} на {url}: {e.response.text[:200]}"
            self.logger.error(error_msg)
            if e.response.status_code >= 500:
                await self._send_alert(error_msg)
            raise
        except Exception as e:
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА Talantix Internal API: {e}"
            self.logger.error(error_msg, exc_info=True)
            await self._send_alert(error_msg)
            raise

    async def get_calendar(self, date: int, account: Account, db: AsyncSession):
        path = "/ats/calendar"
        return await self._send_request("GET", path, account, db, params={"date": date})

    @staticmethod
    def _to_int(value) -> int | None:
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _slot_time_label(slot_item: dict, start_ms: int | None) -> str:
        if start_ms is not None:
            dt_msk = datetime.datetime.fromtimestamp(start_ms / 1000, tz=MSK_TZ)
            return dt_msk.strftime("%Y-%m-%d %H:%M")

        raw_time = slot_item.get("time")
        if isinstance(raw_time, str):
            raw_time = raw_time.strip()
            if len(raw_time) == 5 and raw_time[2] == ":":
                return raw_time
            return raw_time

        return ""

    def _extract_slots_from_calendar_payload(self, payload: dict, target_date: str) -> list[dict]:
        """
        Извлекает реальный slots list/map из ответа Talantix internal API.
        Поддерживает несколько форматов ответа (ats.calendar.slots / data.slots / slots).
        """
        ats = payload.get("ats", {}) if isinstance(payload, dict) else {}
        calendar = ats.get("calendar", {}) if isinstance(ats, dict) else {}
        data = payload.get("data", {}) if isinstance(payload, dict) else {}

        raw_slots = None
        for candidate in (
            calendar.get("slots") if isinstance(calendar, dict) else None,
            data.get("slots") if isinstance(data, dict) else None,
            payload.get("slots") if isinstance(payload, dict) else None,
        ):
            if candidate:
                raw_slots = candidate
                break

        if raw_slots is None:
            self.logger.warning(
                "Talantix calendar payload has no explicit slots (keys: top=%s ats=%s calendar=%s)",
                list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
                list(ats.keys()) if isinstance(ats, dict) else type(ats).__name__,
                list(calendar.keys()) if isinstance(calendar, dict) else type(calendar).__name__,
            )
            return []

        if isinstance(raw_slots, dict):
            raw_items = list(raw_slots.values())
        elif isinstance(raw_slots, list):
            raw_items = raw_slots
        else:
            self.logger.warning("Talantix slots unsupported type: %s", type(raw_slots).__name__)
            return []

        parsed: list[dict] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue

            start_ms = self._to_int(item.get("startDate"))
            if start_ms is None:
                start_ms = self._to_int(item.get("start"))
            if start_ms is None:
                start_ms = self._to_int(item.get("date"))

            slot_id = self._to_int(item.get("id"))
            if slot_id is None:
                slot_id = self._to_int(item.get("slotId"))
            if slot_id is None and start_ms is not None:
                slot_id = start_ms
            if slot_id is None:
                continue

            available = item.get("available")
            if available is None:
                available = item.get("isAvailable")
            if available is None and "busy" in item:
                available = not bool(item.get("busy"))
            if available is None:
                available = True
            available = bool(available)

            time_label = self._slot_time_label(item, start_ms)
            if not time_label:
                continue

            # Фильтрация по целевой дате
            if start_ms is not None:
                date_msk = datetime.datetime.fromtimestamp(start_ms / 1000, tz=MSK_TZ).strftime(
                    "%Y-%m-%d"
                )
                if date_msk != target_date:
                    continue
            elif " " in time_label:
                date_part = time_label.split(" ")[0]
                if date_part != target_date:
                    continue

            parsed.append(
                {
                    "id": slot_id,
                    "time": time_label,
                    "available": available,
                }
            )

        parsed.sort(key=lambda s: self._to_int(s.get("id")) or 0)
        self.logger.info(
            "Extracted Talantix slots from API: target_date=%s total=%s available=%s",
            target_date,
            len(parsed),
            len([s for s in parsed if s.get("available")]),
        )
        return parsed

    async def get_available_slots(self, date: str, account: Account, db: AsyncSession) -> list[dict]:
        """
        Получение доступных слотов на указанную дату.
        date: формат YYYY-MM-DD
        Возвращает список слотов: [{"id": ..., "time": "...", "available": true}, ...]
        """
        dt_local = datetime.datetime.strptime(date, "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=MSK_TZ
        )
        timestamp = int(dt_local.timestamp() * 1000)
        self.logger.info(
            "get_available_slots: date=%s, day_start_msk=%s, day_start_ts=%s",
            date,
            dt_local.isoformat(),
            timestamp,
        )

        result = await self.get_calendar(timestamp, account, db)

        # Ответ имеет структуру: {"ats": {"calendar": {...}}}
        ats_data = result.get("ats", {})
        calendar_data = ats_data.get("calendar", {})

        # 1) Предпочитаем реальные слоты от Talantix (если они есть в payload)
        api_slots = self._extract_slots_from_calendar_payload(result, date)
        if api_slots:
            available = [slot for slot in api_slots if slot.get("available", False)]
            self.logger.info(
                "get_available_slots(api-native): date=%s available=%s times=%s ids=%s",
                date,
                len(available),
                [slot.get("time") for slot in available],
                [slot.get("id") for slot in available],
            )
            return available

        # 2) Fallback: строим слоты по meetingsMap (legacy behavior)
        # Используем только если API не вернул slots list.
        meetings_map = calendar_data.get("meetingsMap", {})
        occupied_times = set()

        for meeting in meetings_map.values():
            start_date = meeting.get("startDate")
            if start_date:
                occupied_times.add(start_date)

        self.logger.warning(
            "get_available_slots(fallback): date=%s, meetings_count=%s, occupied_count=%s",
            date,
            len(meetings_map) if isinstance(meetings_map, dict) else 0,
            len(occupied_times),
        )

        # Генерируем слоты на каждый час рабочего дня (9:00 - 19:00)
        slots = []
        for hour in range(9, 20):  # 9, 10, 11, ..., 19
            slot_dt = dt_local.replace(hour=hour)
            slot_timestamp = int(slot_dt.timestamp() * 1000)

            is_available = slot_timestamp not in occupied_times

            slots.append({
                "id": slot_timestamp,
                "time": slot_dt.strftime("%Y-%m-%d %H:%M"),
                "available": is_available
            })

        # Возвращаем только доступные слоты
        available = [slot for slot in slots if slot.get("available", False)]
        self.logger.warning(
            "get_available_slots(fallback): date=%s, generated=%s, available=%s, available_times=%s",
            date,
            len(slots),
            len(available),
            [slot.get("time") for slot in available],
        )
        return available

    async def book_slot(
        self,
        start_date: int,
        end_date: int,
        person_ids: list[int] | None = None,
        vacancy_ids: list[int] | None = None,
        manager_ids: list[int] | None = None,
        title: str | None = None,
        comment: str | None = None,
        place: str = "",
        timezone: str = "Europe/Moscow",
        send_person_message: bool = False,
        sync_with_external_calendar: bool = False,
        person_message_file_ids: list[int] | None = None,
        account: Account | None = None,
        db: AsyncSession | None = None,
    ) -> dict:
        """
        Создание встречи/собеседования в календаре Talantix.

        Args:
            start_date: timestamp начала встречи в ms (UTC)
            end_date: timestamp конца встречи в ms (UTC)
            person_ids: список ID кандидатов (personId)
            vacancy_ids: список ID вакансий
            manager_ids: список ID менеджеров-участников
            title: название встречи (тип)
            comment: комментарий в формате HTML
            place: место проведения
            timezone: часовой пояс (по умолчанию europe/moscow)
            send_person_message: отправить уведомление кандидату
            sync_with_external_calendar: синхронизация с внешним календарём
            person_message_file_ids: файлы для сообщения кандидату
            account: аккаунт Talantix
            db: сессия БД

        Returns:
            dict: ответ API с данными календаря (включая созданную встречу)
        """
        if account is None or db is None:
            raise ValueError("account и db обязательны для book_slot")

        path = "/ats/calendar/meeting"

        payload: dict[str, Any] = {
            "startDate": start_date,
            "endDate": end_date,
            "timezone": timezone,
            "personIds": person_ids or [],
            "vacancyIds": vacancy_ids or [],
            "managerIds": manager_ids or [],
            "type": title or "",
            "comment": {
                "dangerousHtml": comment or ""
            },
            "place": place,
            "sendPersonMessage": send_person_message,
            "syncWithExternalCalendar": sync_with_external_calendar,
            "personMessageFileIds": person_message_file_ids or [],
        }

        self.logger.info(
            "book_slot: start_date=%s, end_date=%s, person_ids=%s, vacancy_ids=%s, manager_ids=%s, type=%s",
            start_date, end_date, person_ids, vacancy_ids, manager_ids, title,
        )
        return await self._send_request("POST", path, account, db, json=payload)

    async def get_managers(
        self,
        roles: list[str] | None = None,
        search_by_name: str = "",
        account: Account | None = None,
        db: AsyncSession | None = None,
    ) -> list[dict]:
        """
        Получение списка менеджеров через GraphQL API.

        Args:
            roles: список ролей (ADMIN, RECRUITER, LINE_MANAGER, MEMBER)
            search_by_name: поиск по имени
            account: аккаунт Talantix
            db: сессия БД

        Returns:
            list[dict]: список менеджеров с полями id, firstName, lastName, email, managerRole
        """
        if account is None or db is None:
            raise ValueError("account и db обязательны для get_managers")

        if roles is None:
            roles = ["ADMIN", "RECRUITER", "LINE_MANAGER", "MEMBER"]

        query = """
            query FilteredManagers(
                $managerLicenseTypes: [ManagerLicenseType!],
                $roles: [ManagerRole!],
                $searchByName: String,
                $after: String,
                $sortType: ManagerSortTypeInput
            ) {
                managers(
                    filter: {
                        managerLicenseTypes: $managerLicenseTypes,
                        roles: $roles,
                        searchByName: $searchByName
                    },
                    after: $after,
                    first: 50,
                    sortType: $sortType
                ) {
                    ... on Managers {
                        items {
                            id
                            hhManagerId
                            firstName
                            lastName
                            middleName
                            email
                            managerRole
                            __typename
                        }
                        pageInfo {
                            endCursor
                            hasNextPage
                            __typename
                        }
                        __typename
                    }
                    __typename
                }
            }
        """

        variables = {
            "roles": roles,
            "searchByName": search_by_name,
            "sortType": "BY_MANAGER_ID",
            "after": None,
        }

        graphql_payload = {
            "operationName": "FilteredManagers",
            "query": query.strip(),
            "variables": variables,
        }

        path = "/ats/graphql?operationName=FilteredManagers"

        self.logger.info(
            "get_managers: roles=%s, search_by_name=%s", roles, search_by_name,
        )

        result = await self._send_request(
            "POST", path, account, db, json=graphql_payload,
        )

        managers_data = result.get("data", {}).get("managers", {})
        items = managers_data.get("items", [])

        self.logger.info("get_managers: found %s managers", len(items))
        return items

    async def cancel_interview(
        self,
        interview_id: int,
        account: Account | None = None,
        db: AsyncSession | None = None,
    ) -> dict:
        """
        Отмена записанного собеседования через GraphQL мутацию.

        Args:
            interview_id: ID встречи в календаре Talantix
            account: аккаунт Talantix
            db: сессия БД

        Returns:
            dict: ответ API с результатом удаления
        """
        if account is None or db is None:
            raise ValueError("account и db обязательны для cancel_interview")

        query = """
            mutation DeleteCalendarMeeting($id: Int!) {
                deleteCalendarMeeting(id: $id) {
                    __typename
                    ... on CalendarMeetingDeleteError {
                        errorType
                        __typename
                    }
                    ... on CalendarMeetingDeleteSuccess {
                        id
                        __typename
                    }
                }
            }
        """

        graphql_payload = {
            "operationName": "DeleteCalendarMeeting",
            "query": query.strip(),
            "variables": {"id": interview_id},
        }

        path = "/ats/graphql?operationName=DeleteCalendarMeeting"

        self.logger.info("cancel_interview: interview_id=%s", interview_id)
        return await self._send_request(
            "POST", path, account, db, json=graphql_payload,
        )


class TalantixService:
    """
    Сервис для работы с календарем Talantix.
    """

    def __init__(self):
        self.calend_client = TalantixCalendClient()
        self.logger = logging.getLogger("talantix.service")

    async def _get_account(
        self, db: AsyncSession, platform: str
    ) -> Account | None:
        """Получение аккаунта из БД по платформе."""
        result = await db.execute(
            select(Account).where(Account.platform == platform, Account.is_active == True)
        )
        return result.scalar_one_or_none()

    # --- МЕТОДЫ ДЛЯ КАЛЕНДАРЯ ---

    async def get_available_slots(self, date: str | None = None, days: int = 7) -> dict[str, list[str]]:
        """
        Получение доступных слотов на указанную дату или на период.
        """
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_calend")
            if not account:
                self.logger.error("Аккаунт talantix_calend не найден")
                return {}

            try:
                moscow_tz = datetime.timezone(datetime.timedelta(hours=3))
                
                if date:
                    dates_to_check = [date]
                else:
                    today = datetime.datetime.now(moscow_tz).date()
                    dates_to_check = [
                        (today + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                        for i in range(days)
                    ]
                
                result = {}
                for check_date in dates_to_check:
                    slots = await self.calend_client.get_available_slots(check_date, account, db)
                    times = []
                    for slot in slots:
                        time_str = slot.get("time", "")
                        if time_str:
                            time_part = time_str.split(" ")[-1] if " " in time_str else time_str
                            times.append(time_part)
                    result[check_date] = times
                    self.logger.info(
                        "service.get_available_slots: date=%s, count=%s, times=%s",
                        check_date,
                        len(times),
                        times,
                    )

                return result
                
            except Exception as e:
                self.logger.error(f"Ошибка получения слотов: {e}")
                return {}

    async def get_nearest_slots(self, limit: int = 2) -> list[tuple[str, str]]:
        """
        Получение ближайших доступных слотов (на сегодня и завтра).
        """
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_calend")
            if not account:
                self.logger.error("Аккаунт talantix_calend не найден")
                return []

            try:
                moscow_tz = datetime.timezone(datetime.timedelta(hours=3))
                now_msk = datetime.datetime.now(moscow_tz)
                today = now_msk.date()
                current_hour = now_msk.hour
                
                dates_to_check = [
                    today.strftime("%Y-%m-%d"),
                    (today + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                ]
                
                result = []
                for check_date in dates_to_check:
                    slots = await self.calend_client.get_available_slots(check_date, account, db)
                    self.logger.info(
                        "service.get_nearest_slots: date=%s raw_slots=%s",
                        check_date,
                        [slot.get("time") for slot in slots],
                    )
                    
                    for slot in slots:
                        time_str = slot.get("time", "")
                        if not time_str:
                            continue
                        
                        time_part = time_str.split(" ")[-1] if " " in time_str else time_str
                        slot_hour = int(time_part.split(":")[0])
                        
                        if check_date == today.strftime("%Y-%m-%d") and slot_hour <= current_hour:
                            continue
                        
                        result.append((check_date, time_part))
                        self.logger.info(
                            "service.get_nearest_slots: accepted slot date=%s time=%s current_len=%s limit=%s",
                            check_date,
                            time_part,
                            len(result),
                            limit,
                        )
                        
                        if len(result) >= limit:
                            return result
                
                return result
                
            except Exception as e:
                self.logger.error(f"Ошибка получения ближайших слотов: {e}")
                return []

    @staticmethod
    def _normalize_hhmm(value: str | None) -> str | None:
        if not value:
            return None
        try:
            parts = str(value).strip().split(":")
            if len(parts) < 2:
                return None
            hour = int(parts[0])
            minute = int(parts[1])
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                return None
            return f"{hour:02d}:{minute:02d}"
        except (TypeError, ValueError):
            return None

    async def resolve_slot_id(self, date: str, time_str: str) -> int | None:
        """
        Находит slot_id в Talantix по дате и времени (HH:MM).
        """
        normalized_target = self._normalize_hhmm(time_str)
        if not normalized_target:
            self.logger.warning("resolve_slot_id: не удалось нормализовать время %s", time_str)
            return None

        self.logger.info("🔍 resolve_slot_id: поиск slot_id для date=%s time=%s (normalized=%s)", date, time_str, normalized_target)
        
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_calend")
            if not account:
                self.logger.error("❌ resolve_slot_id: Аккаунт talantix_calend не найден")
                return None

            try:
                slots = await self.calend_client.get_available_slots(date, account, db)
                for slot in slots:
                    slot_time = slot.get("time", "")
                    time_part = slot_time.split(" ")[-1] if " " in slot_time else slot_time
                    slot_normalized = self._normalize_hhmm(time_part)
                    if slot_normalized != normalized_target:
                        continue
                    slot_id = slot.get("id")
                    if slot_id is None:
                        continue
                    try:
                        result = int(slot_id)
                        self.logger.info("✅ resolve_slot_id: найден slot_id=%s для %s %s", result, date, time_str)
                        return result
                    except (TypeError, ValueError):
                        continue
                self.logger.warning("❌ resolve_slot_id: slot_id не найден для %s %s", date, time_str)
            except Exception as e:
                self.logger.error("❌ Ошибка resolve_slot_id (%s %s): %s", date, time_str, e, exc_info=True)
                return None

        return None

    async def book_interview(
        self,
        start_date: int,
        end_date: int,
        person_ids: list[int] | None = None,
        vacancy_ids: list[int] | None = None,
        manager_ids: list[int] | None = None,
        title: str | None = None,
        comment: str | None = None,
        place: str = "",
        timezone: str = "Europe/Moscow",
        send_person_message: bool = False,
        sync_with_external_calendar: bool = False,
        person_message_file_ids: list[int] | None = None,
    ) -> int | None:
        """
        Создание встречи/собеседования в календаре Talantix.

        Args:
            start_date: timestamp начала встречи в ms (UTC)
            end_date: timestamp конца встречи в ms (UTC)
            person_ids: список ID кандидатов (personId)
            vacancy_ids: список ID вакансий
            manager_ids: список ID менеджеров-участников (создатель + участники)
            title: название встречи (тип)
            comment: комментарий в формате HTML
            place: место проведения
            timezone: часовой пояс (по умолчанию europe/moscow)
            send_person_message: отправить уведомление кандидату
            sync_with_external_calendar: синхронизация с внешним календарём
            person_message_file_ids: файлы для сообщения кандидату

        Returns:
            int | None: ID созданной встречи (meeting_id) или None при ошибке
        """
        self.logger.info(
            "🔍 book_interview вызван: start_date=%s, end_date=%s, person_ids=%s, vacancy_ids=%s, manager_ids=%s, title=%s",
            start_date, end_date, person_ids, vacancy_ids, manager_ids, title,
        )

        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_calend")
            if not account:
                self.logger.error("❌ Аккаунт talantix_calend не найден")
                return None

            try:
                result = await self.calend_client.book_slot(
                    start_date=start_date,
                    end_date=end_date,
                    person_ids=person_ids,
                    vacancy_ids=vacancy_ids,
                    manager_ids=manager_ids,
                    account=account,
                    db=db,
                    title=title,
                    comment=comment,
                    place=place,
                    timezone=timezone,
                    send_person_message=send_person_message,
                    sync_with_external_calendar=sync_with_external_calendar,
                    person_message_file_ids=person_message_file_ids,
                )

                # Извлекаем meeting_id из ответа
                # Ответ имеет структуру: {"ats": {"calendar": {"meetingsMap": {"2091160": {...}}}}}
                ats = result.get("ats", {})
                calendar = ats.get("calendar", {})
                meetings_map = calendar.get("meetingsMap", {})

                if meetings_map:
                    # Берём последнюю созданную встречу (самый большой ключ = последний ID)
                    meeting_id = max(int(k) for k in meetings_map.keys())
                    self.logger.info(
                        "✅ Встреча создана: meeting_id=%s, person_ids=%s, vacancy_ids=%s, manager_ids=%s",
                        meeting_id, person_ids, vacancy_ids, manager_ids,
                    )
                    return meeting_id
                else:
                    self.logger.warning("⚠️ meetingsMap пуст в ответе book_slot")
                    return None

            except Exception as e:
                self.logger.error(f"❌ Ошибка создания встречи: {e}", exc_info=True)
                return None

    async def release_interview(self, interview_id: int) -> bool:
        """
        Освобождение забронированного слота (отмена собеседования).

        Args:
            interview_id: ID встречи в календаре Talantix

        Returns:
            bool: True если встреча успешно удалена
        """
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_calend")
            if not account:
                self.logger.error("Аккаунт talantix_calend не найден")
                return False

            try:
                result = await self.calend_client.cancel_interview(
                    interview_id=interview_id,
                    account=account,
                    db=db,
                )

                # Проверяем результат GraphQL мутации
                delete_result = result.get("data", {}).get("deleteCalendarMeeting", {})
                typename = delete_result.get("__typename", "")

                if typename == "CalendarMeetingDeleteSuccess":
                    self.logger.info(f"✅ Собеседование {interview_id} отменено")
                    return True
                elif typename == "CalendarMeetingDeleteError":
                    error_type = delete_result.get("errorType", "unknown")
                    self.logger.error(f"❌ Ошибка отмены собеседования {interview_id}: {error_type}")
                    return False
                else:
                    self.logger.warning(f"⚠️ Неожиданный ответ при отмене {interview_id}: {typename}")
                    return False

            except Exception as e:
                self.logger.error(f"❌ Ошибка отмены собеседования {interview_id}: {e}", exc_info=True)
                return False

    async def get_managers(
        self,
        roles: list[str] | None = None,
        search_by_name: str = "",
    ) -> list[dict]:
        """
        Получение списка менеджеров (участников) из Talantix.

        Args:
            roles: список ролей для фильтрации (ADMIN, RECRUITER, LINE_MANAGER, MEMBER)
            search_by_name: поиск по имени

        Returns:
            list[dict]: список менеджеров с полями:
                - id: ID менеджера в Talantix
                - hhManagerId: ID менеджера в HH
                - firstName, lastName, middleName: имя
                - email: email
                - managerRole: роль (ADMIN, RECRUITER, LINE_MANAGER, MEMBER)

        Example:
            managers = await talantix_service.get_managers()
            for m in managers:
                print(f"{m['id']}: {m['firstName']} {m['lastName']} ({m['managerRole']})")
        """
        async with AsyncSessionLocal() as db:
            account = await self._get_account(db, "talantix_calend")
            if not account:
                self.logger.error("Аккаунт talantix_calend не найден")
                return []

            try:
                managers = await self.calend_client.get_managers(
                    roles=roles,
                    search_by_name=search_by_name,
                    account=account,
                    db=db,
                )
                self.logger.info("service.get_managers: found %s managers", len(managers))
                return managers
            except Exception as e:
                self.logger.error(f"Ошибка получения менеджеров: {e}", exc_info=True)
                return []


# Глобальный экземпляр сервиса
talantix_service = TalantixService()
