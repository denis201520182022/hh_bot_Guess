# app/utils/tg_alerts.py    
import logging
from typing import Optional, Dict, Any
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BufferedInputFile
from sqlalchemy import select

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.db.models import TelegramUser

logger = logging.getLogger(__name__)

# Твой персональный ID для технических алертов
MY_TECH_ADMIN_ID = 1975808643

def esc(text: Any) -> str:
    """Экранирование спецсимволов для MarkdownV2"""
    return str(text).replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`').replace('>', '\\>')

async def _get_recipients(alert_type: str) -> list[int]:
    """
    Определяет список получателей.
    """
    # Если это баланс или общая рассылка — берем всех из БД
    if alert_type in ["balance", "all"]:
        async with AsyncSessionLocal() as session:
            stmt = select(TelegramUser.telegram_id)
            result = await session.execute(stmt)
            return list(result.scalars().all())
    
    # Во всех остальных случаях (технические алерты, ошибки) — только ТЫ
    return [MY_TECH_ADMIN_ID]

async def send_system_alert(message_text: str, alert_type: str = "admin_only"):
    """
    Отправляет системное уведомление.
    Если alert_type='balance' — всем. Иначе — только тебе.
    """
    recipients = await _get_recipients(alert_type)
    if not recipients:
        return

    async with Bot(token=settings.TELEGRAM_BOT_TOKEN) as bot:
        for chat_id in recipients:
            try:
                await bot.send_message(chat_id=chat_id, text=message_text)
            except Exception as e:
                logger.warning(f"Ошибка отправки системного алерта в {chat_id}: {e}")

async def send_verification_alert(
    dialogue_id: int,
    external_chat_id: str,
    db_data: Dict[str, Any],
    llm_data: Dict[str, Any],
    history_text: Optional[str] = None,
    reasoning: str = "не указано"
):
    """
    Технический алерт: только тебе (MY_TECH_ADMIN_ID).
    """
    alert_text = (
        f"🚨 *INCIDENT: Ошибка верификации данных*\n\n"
        f"Диалог ID: `{dialogue_id}`\n"
        f"Chat ID: `{esc(external_chat_id)}`\n\n"
        f"📉 *Данные в БД:* {esc(db_data)}\n"
        f"🤖 *Deep Check LLM:* {esc(llm_data)}\n\n"
        f"🧐 *Обоснование:* _{esc(reasoning)}_\n\n"
        f"⛔ *Данные в БД НЕ! обновлены.*"
    )

    async with Bot(
        token=settings.TELEGRAM_BOT_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    ) as bot:
        try:
            await bot.send_message(chat_id=MY_TECH_ADMIN_ID, text=alert_text)
            if history_text:
                file = BufferedInputFile(history_text.encode('utf-8'), filename=f"verify_err_{external_chat_id}.txt")
                await bot.send_document(chat_id=MY_TECH_ADMIN_ID, document=file, caption="📜 История")
        except Exception as e:
            logger.error(f"Ошибка отправки алерта верификации: {e}")

async def send_hallucination_alert(
    dialogue_id: int,
    external_chat_id: str,
    user_said: str,
    llm_suggested: str,
    corrected_val: str,
    history_text: Optional[str] = None,
    reasoning: str = "не указано"
):
    """
    Технический алерт: только тебе (MY_TECH_ADMIN_ID).
    """
    alert_text = (
        f"📅 *INCIDENT: Ошибка извлечения (Галлюцинация)*\n\n"
        f"Диалог ID: `{dialogue_id}`\n"
        f"Chat ID: `{esc(external_chat_id)}`\n\n"
        f"👤 *Кандидат:* _{esc(user_said)}_\n"
        f"🤖 *LLM:* `{esc(llm_suggested)}`\n"
        f"✅ *Исправлено:* `{esc(corrected_val)}`\n\n"
        f"🧐 *Обоснование:* _{esc(reasoning)}_"
    )

    async with Bot(
        token=settings.TELEGRAM_BOT_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    ) as bot:
        try:
            await bot.send_message(chat_id=MY_TECH_ADMIN_ID, text=alert_text)
            if history_text:
                file = BufferedInputFile(history_text.encode('utf-8'), filename=f"hallucination_{external_chat_id}.txt")
                await bot.send_document(chat_id=MY_TECH_ADMIN_ID, document=file, caption="📜 История")
        except Exception as e:
            logger.error(f"Ошибка отправки алерта галлюцинации: {e}")


async def handle_alert_task(message_body: dict):
    """Диспетчер системных алертов (ошибки, верификация, галлюцинации)"""
    alert_type = message_body.get("type") # 'system', 'verification', 'hallucination'
    
    try:
        if alert_type == 'system':
            await send_system_alert(
                message_text=message_body.get("text"),
                alert_type=message_body.get("alert_type", "admin_only")
            )
        
        elif alert_type == 'verification':
            await send_verification_alert(
                dialogue_id=message_body.get("dialogue_id"),
                external_chat_id=message_body.get("external_chat_id"),
                db_data=message_body.get("db_data"),
                llm_data=message_body.get("llm_data"),
                history_text=message_body.get("history_text"),
                reasoning=message_body.get("reasoning")
            )
            
        elif alert_type == 'hallucination':
            await send_hallucination_alert(
                dialogue_id=message_body.get("dialogue_id"),
                external_chat_id=message_body.get("external_chat_id"),
                user_said=message_body.get("user_said"),
                llm_suggested=message_body.get("llm_suggested"),
                corrected_val=message_body.get("corrected_val"),
                history_text=message_body.get("history_text"),
                reasoning=message_body.get("reasoning")
            )
            
        logger.info(f"🔔 Алерт типа '{alert_type}' успешно обработан")
    except Exception as e:
        logger.error(f"💥 Ошибка при обработке алертов в воркере: {e}")