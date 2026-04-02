# app/services/sheets.py
import logging
import re
import asyncio
import json
from typing import List, Dict, Optional, Any
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.core.config import settings
from app.core.rabbitmq import mq 

logger = logging.getLogger("google_sheets_scheduling")

class GoogleSheetsService:
    def __init__(self):
        # Используем настройки из блока scheduling.google_sheets
        self.conf = settings.scheduling.google_sheets
        self.spreadsheet_url = self.conf.spreadsheet_url
        self.calendar_sheet = self.conf.calendar_sheet_name
        
        # Путь к кредам из .env
        self.creds_path = settings.GOOGLE_CREDENTIALS_JSON
        
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self._spreadsheet_id = self._extract_id(self.spreadsheet_url)

    def _extract_id(self, url: str) -> str:
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
        if match: return match.group(1)
        raise ValueError(f"Не удалось извлечь ID таблицы из ссылки: {url}")

    async def _send_critical_alert(self, error_text: str, payload: Optional[Dict] = None):
        """
        Отправка алерта с подробным дампом данных, которые не удалось обработать.
        """
        try:
            full_message = f"🚨 **ОШИБКА КАЛЕНДАРЯ (GS)**\n\n**Проблема:** {error_text}"
            
            if payload:
                # Форматируем данные в JSON блок
                data_dump = json.dumps(payload, indent=2, ensure_ascii=False)
                full_message += f"\n\n**Данные для ручного ввода:**\n```json\n{data_dump}\n```"

            await mq.publish("tg_alerts", {
                "type": "system",
                "text": full_message,
                "alert_type": "admin_only"
            })
        except Exception as e:
            logger.error(f"Не удалось отправить алерт: {e}")

    def _get_service(self):
        creds = Credentials.from_service_account_file(self.creds_path, scopes=self.scopes)
        return build('sheets', 'v4', credentials=creds, cache_discovery=False)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def _execute_google_call(self, func, *args, **kwargs):
        return await asyncio.to_thread(func(*args, **kwargs).execute)

    async def _get_all_calendar_rows(self) -> List[List[str]]:
        try:
            service = await asyncio.to_thread(self._get_service)
            range_name = f"'{self.calendar_sheet}'!A2:D"
            result = await self._execute_google_call(
                service.spreadsheets().values().get,
                spreadsheetId=self._spreadsheet_id, range=range_name
            )
            return result.get('values', [])
        except Exception as e:
            await self._send_critical_alert(f"Сбой чтения календаря: {e}")
            raise e

    # --- МЕТОДЫ ДЛЯ КАЛЕНДАРЯ ---

    async def book_slot(self, target_date: str, target_time: str, candidate_name: str) -> bool:
        """Занимает слот"""
        return await self._update_slot_status(target_date, target_time, "Занято", candidate_name)

    async def release_slot(self, target_date: str, target_time: str) -> bool:
        """Освобождает слот"""
        if not target_date or not target_time: return False
        return await self._update_slot_status(target_date, target_time, "Свободно", "")

    async def _update_slot_status(self, target_date: str, target_time: str, status: str, name: str) -> bool:
        # Формируем контекст данных на случай ошибки
        context = {
            "sheet": "Календарь",
            "date": target_date,
            "time": target_time,
            "new_status": status,
            "candidate": name
        }
        try:
            rows = await self._get_all_calendar_rows()
            for idx, row in enumerate(rows):
                if len(row) >= 2 and row[0].strip() == target_date and row[1].strip() == target_time:
                    row_number = idx + 2
                    service = await asyncio.to_thread(self._get_service)
                    update_range = f"'{self.calendar_sheet}'!C{row_number}:D{row_number}"
                    body = {'values': [[status, name]]}
                    
                    await self._execute_google_call(
                        service.spreadsheets().values().update,
                        spreadsheetId=self._spreadsheet_id,
                        range=update_range,
                        valueInputOption="RAW",
                        body=body
                    )
                    return True
            
            # Если не нашли такой слот
            await self._send_critical_alert("Слот не найден в таблице (проверьте дату/время)", context)
            return False
        except Exception as e:
            await self._send_critical_alert(f"Ошибка обновления статуса слота: {e}", context)
            return False


    async def get_available_slots(self, target_date: str) -> List[str]:
        try:
            rows = await self._get_all_calendar_rows()
            return [row[1].strip() for row in rows if len(row) >= 3 and row[0].strip() == target_date and row[2].strip().lower() == "свободно"]
        except Exception:
            return []

    async def get_all_slots_map(self) -> Dict[str, List[str]]:
        try:
            rows = await self._get_all_calendar_rows()
            slots_map = {}
            for row in rows:
                if len(row) < 3: continue
                d, t, s = row[0].strip(), row[1].strip(), row[2].strip().lower()
                if s == "свободно":
                    if d not in slots_map: slots_map[d] = []
                    slots_map[d].append(t)
            return slots_map
        except Exception:
            return {}

sheets_service = GoogleSheetsService()