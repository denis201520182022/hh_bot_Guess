# output_chanels/google_sheets/gs_card.py
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

logger = logging.getLogger("gs_reporter")

class GoogleSheetsReporter:
    def __init__(self):
        # Используем настройки из блока services.google_sheets_report
        self.conf = settings.services.google_sheets_report
        self.spreadsheet_url = self.conf.spreadsheet_url
        self.candidates_sheet = self.conf.candidates_sheet_name
        
        # Путь к кредам из .env
        self.creds_path = settings.GOOGLE_CREDENTIALS_JSON
        
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self._spreadsheet_id = self._extract_id(self.spreadsheet_url)

    def _extract_id(self, url: str) -> str:
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
        if match: return match.group(1)
        raise ValueError(f"Не удалось извлечь ID таблицы из ссылки: {url}")

    async def _send_critical_alert(self, error_text: str, payload: Optional[Dict] = None):
        """Отправка алерта в Telegram при сбое записи"""
        try:
            full_message = f"🚨 **ОШИБКА ОБРАБОТКИ КАНДИДАТА (GS)**\n\n**Проблема:** {error_text}"
            if payload:
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

    async def append_candidate(self, data: Dict[str, Any]):
        """Добавляет строку в общую таблицу кандидатов (Reporting)"""
        try:
            service = await asyncio.to_thread(self._get_service)
            # Структура строки: ФИО, Телефон, Вакансия, Ссылка, Дата собеса, Статус
            row_values = [
                data.get("full_name", ""),
                data.get("phone", ""),
                data.get("vacancy", ""),
                data.get("chat_link", ""),
                data.get("interview_dt", "Не назначено"),
                data.get("status", "Квалифицирован")
            ]
            await self._execute_google_call(
                service.spreadsheets().values().append,
                spreadsheetId=self._spreadsheet_id,
                range=f"'{self.candidates_sheet}'!A:F",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={'values': [row_values]}
            )
            logger.info(f"📊 Кандидат {data.get('full_name')} успешно записан в Google Sheets")
        except Exception as e:
            await self._send_critical_alert(
                f"Ошибка записи кандидата в общий список: {e}", 
                {"sheet": self.candidates_sheet, "data": data}
            )

# Глобальный инстанс для использования в воркерах
gs_reporter = GoogleSheetsReporter()
