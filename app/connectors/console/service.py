# app/connectors/console/service.py
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models import Account
from app.core.rabbitmq import mq

logger = logging.getLogger(__name__)

class ConsoleConnectorService:
    async def send_message(self, account: Account, db: AsyncSession, chat_id: str, text: str, user_id: str = "me"):
        """
        Фиктивная отправка сообщения. Вместо реального API выводим текст в консоль 
        и отправляем в очередь console_output для перехвата тестовым скриптом.
        """
        logger.info(f"🖥️ [CONSOLE CONNECTOR] Отправка сообщения в чат {chat_id}: \n{text}")
        
        # Отправляем в очередь console_output, чтобы скрипт мог прочитать
        await mq.publish("console_output", {"chat_id": chat_id, "text": text})
        
        return {"id": "console_msg_" + chat_id}

console_connector = ConsoleConnectorService()
