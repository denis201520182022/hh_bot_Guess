# worker_services.py
import asyncio
import json
import logging
import datetime
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.core.rabbitmq import mq
from app.db.session import AsyncSessionLocal
from app.db.models import Dialogue
from app.utils.logger import logger, set_log_context, log_context
from app.utils.tg_alerts import handle_alert_task

# Импортируем инструменты вывода
from app.output_chanels.telegram.tg_cards import send_tg_notification
from app.output_chanels.google_sheets.gs_card import gs_reporter

# Импорты для работы ТГ-бота
from app.tg_bot.handlers import router as main_router
from app.tg_bot.middlewares import DbSessionMiddleware

# Инициализация ТГ-бота
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Регистрируем мидлварь для базы данных и подключаем роутеры
dp.update.middleware(DbSessionMiddleware(AsyncSessionLocal))
dp.include_router(main_router)

async def handle_reporting_task(message_body: dict):
    """
    Диспетчер задач отчетности. 
    Распределяет данные между Telegram, Google Sheets и другими каналами на основе конфига.
    """
    dialogue_id = message_body.get("dialogue_id")
    event_type = message_body.get("type", "qualified")
    
    async with AsyncSessionLocal() as db:
        # Загружаем диалог со всеми связями
        stmt = (
            select(Dialogue)
            .where(Dialogue.id == dialogue_id)
            .options(
                selectinload(Dialogue.candidate),
                selectinload(Dialogue.vacancy),
                selectinload(Dialogue.account)
            )
        )
        result = await db.execute(stmt)
        dialogue = result.scalar_one_or_none()

        if not dialogue:
            logger.error(f"❌ Диалог {dialogue_id} не найден в БД для формирования отчета")
            return

        candidate = dialogue.candidate
        vacancy = dialogue.vacancy
        account = dialogue.account
        meta = dialogue.metadata_json or {}

        # --- ЗАДЕРЖКА (Race Condition Protection) ---
        # Оставляем 10 секунд, чтобы база точно успела обновиться всеми воркерами
        await asyncio.sleep(10)

        results = []

        try:
            # 1. КАНАЛ: TELEGRAM (Карточки)
            if settings.services.telegram.enabled:
                if event_type == 'qualified' and settings.services.telegram.interview_cards:
                    await send_tg_notification(bot, dialogue, candidate, vacancy, account)
                    results.append("Telegram ✅")
                # Здесь можно добавить другие типы карточек (reject_cards, reschedule_cards)

            # 2. КАНАЛ: GOOGLE SHEETS (Отчеты)
            if settings.services.google_sheets_report.enabled:
                if event_type == 'qualified':
                    # ГЕНЕРИРУЕМ ССЫЛКУ ЗАВИСИМО ОТ ПЛАТФОРМЫ
                    if account.platform == 'avito':
                        link = f"https://www.avito.ru/profile/messenger/channel/{dialogue.external_chat_id}"
                    elif account.platform == 'hh':
                        res_id = candidate.profile_data.get("hh_resume_id") or candidate.platform_user_id.split('_')[0]
                        link = f"https://hh.ru/resume/{res_id}"
                    else:
                        link = dialogue.external_chat_id

                    await gs_reporter.append_candidate({
                        "full_name": candidate.full_name,
                        "phone": candidate.phone_number,
                        "vacancy": vacancy.title if vacancy else "Не указана",
                        "chat_link": link,
                        "interview_dt": f"{meta.get('interview_date')} {meta.get('interview_time')}",
                        "status": f"Записан ({account.platform})"
                    })
                    results.append("Google Sheets ✅")

            if results:
                logger.info(f"✅ Отчет по диалогу {dialogue_id} отправлен в каналы: {', '.join(results)}")
            else:
                logger.warning(f"⚠️ Ни один канал вывода не сработал для события {event_type} (проверьте config.yaml)")

        except Exception as e:
            logger.exception(f"💥 Ошибка в dispatch_reporting_task: {e}")

async def run_alerts_consumer():
    """Слушатель очереди системных алертов"""
    queue = await mq.channel.get_queue("tg_alerts")
    logger.info("👷 Alerts Consumer запущен...")
    
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process(ignore_processed=True):
                log_context.set({})
                try:
                    payload = json.loads(message.body.decode())
                    set_log_context(
                        service="alerts",
                        alert_type=payload.get("type"),
                        dialogue_id=payload.get("dialogue_id")
                    )
                    await handle_alert_task(payload)
                except json.JSONDecodeError:
                    # Если пришел мусор вместо JSON - нет смысла возвращать, удаляем
                    logger.error("❌ Получен некорректный JSON в алертах, сообщение отброшено.")
                    await message.reject(requeue=False)

                except Exception as e:
                    logger.exception("💥 Ошибка обработки системного алерта")
                    logger.info("♻️ Возвращаем сообщение в очередь (NACK)...")
                    # ВОТ ОНО: Возвращаем в очередь
                    await message.nack(requeue=True)
                    # Добавляем небольшую паузу, чтобы не спамить логами, если сервис лежит
                    await asyncio.sleep(1)

async def run_services_consumer():
    """Слушатель очереди универсальных выходов (services_output)"""
    queue = await mq.channel.get_queue("services_output")
    logger.info("👷 Services Output Consumer запущен...")
    
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process(ignore_processed=True):
                log_context.set({})
                await asyncio.sleep(10) 
                try:
                    payload = json.loads(message.body.decode())
                    set_log_context(
                        service="reporting",
                        event_type=payload.get("type"),
                        dialogue_id=payload.get("dialogue_id")
                    )
                    await handle_reporting_task(payload)
                except json.JSONDecodeError:
                    logger.error("❌ Некорректный JSON в уведомлениях services_output, сообщение отброшено.")
                    await message.reject(requeue=False)
                except Exception as e:
                    logger.exception("💥 Ошибка в Services Output Worker")
                    logger.info("♻️ Возвращаем сообщение в очередь (NACK)...")
                    await message.nack(requeue=True)
                    await asyncio.sleep(1)

async def main():
    """Основной цикл запуска воркера"""
    await mq.connect()
    
    # Запускаем консьюмеры RabbitMQ задачами
    alerts_task = asyncio.create_task(run_alerts_consumer())
    services_task = asyncio.create_task(run_services_consumer())
    
    logger.info("🚀 [worker_services] & Interactive Bot стартуют...")
    
    try:
        # Запускаем поллинг ТГ-бота (drop_pending_updates=True чтобы не отвечать на старье)
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        alerts_task.cancel()
        services_task.cancel()
        await mq.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Воркер остановлен пользователем")
