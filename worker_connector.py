# worker_connector.py
import asyncio
import json
import logging
import signal
from aio_pika import IncomingMessage
from app.core.rabbitmq import mq
from app.connectors.avito import avito_connector
from app.connectors.hh import hh_connector
from app.db.session import engine
from app.utils.logger import logger, set_log_context, log_context

async def on_avito_inbound(message: IncomingMessage):
    """
    Обработка входящего события от Авито.
    Используем ignore_processed=True для ручного управления подтверждением (ACK/NACK).
    """
    async with message.process(ignore_processed=True):
        # 1. Сначала пытаемся распарсить JSON
        log_context.set({})
        try:
            body = json.loads(message.body.decode())
        except json.JSONDecodeError:
            logger.error("❌ Критическая ошибка: Некорректный JSON в очереди avito_inbound. Сообщение отброшено.")
            await message.reject(requeue=False)
            return

        # 2. Обрабатываем событие
        try:
            set_log_context(
                source=body.get("source"),
                event_type=body.get("type"),
                avito_user_id=body.get("avito_user_id")
            )
            logger.info(f"📥 [Connector] Унификация события от Avito (Source: {body.get('source')})")
            await avito_connector.process_avito_event(body)

            # Если всё прошло успешно - подтверждаем выполнение
            await message.ack()

        except Exception as e:
            # Логируем ошибку
            error_msg = f"❌ Ошибка в Унификаторе (Avito):\n{str(e)}"
            logger.exception("❌ Ошибка при унификации события Avito")

            # --- ОТПРАВКА АЛЕРТА В TG ВОРКЕР (твоя исходная логика) ---
            try:
                alert_payload = {
                    "type": "system",
                    "text": error_msg,
                    "alert_type": "admin_only"
                }
                await mq.publish("tg_alerts", alert_payload)
            except Exception as amqp_err:
                logger.error(f"Не удалось отправить алерт в очередь: {amqp_err}")

            # --- ВОЗВРАТ В ОЧЕРЕДЬ ---
            # Делаем небольшую паузу, чтобы не перегружать систему мгновенными повторами при сбое БД
            logger.info("♻️ Возвращаем задачу в очередь RabbitMQ (requeue=True)...")
            await asyncio.sleep(1)
            await message.nack(requeue=True)


async def on_hh_inbound(message: IncomingMessage):
    """
    Обработка входящего события от HH.
    """
    async with message.process(ignore_processed=True):
        log_context.set({})
        try:
            body = json.loads(message.body.decode())
        except json.JSONDecodeError:
            logger.error("❌ Критическая ошибка: Некорректный JSON в hh_inbound.")
            await message.reject(requeue=False)
            return

        try:
            # ИСПРАВЛЕНИЕ: Берем account_id, так как поллер шлет его
            set_log_context(
                source=body.get("source"),
                account_id=body.get("account_id"),
                folder=body.get("folder")
            )
            
            logger.info(f"📥 [Connector] Унификация события от HH (Source: {body.get('source')})")
            
            # Наш сервис уже готов принимать этот body
            await hh_connector.process_hh_event(body)

            await message.ack()

        except Exception as e:
            error_msg = f"❌ Ошибка в Унификаторе (HH):\n{str(e)}"
            logger.exception("❌ Ошибка при унификации события HH")

            try:
                await mq.publish("tg_alerts", {
                    "type": "system",
                    "text": error_msg,
                    "alert_type": "admin_only"
                })
            except Exception as amqp_err:
                logger.error(f"Не удалось отправить алерт: {amqp_err}")

            logger.info("♻️ Возвращаем задачу HH в очередь (requeue=True)...")
            await asyncio.sleep(1)
            await message.nack(requeue=True)

async def main():
    await mq.connect()
    channel = mq.channel
    # Унификатор быстрый, можно брать много задач (prefetch_count=50)
    await channel.set_qos(prefetch_count=50)

    # === AVITO QUEUE ===
    inbound_queue_avito = await channel.get_queue("avito_inbound")
    await inbound_queue_avito.consume(on_avito_inbound)

    # === HH QUEUE ===
    inbound_queue_hh = await channel.get_queue("hh_inbound")
    await inbound_queue_hh.consume(on_hh_inbound)

    logger.info("👷 [worker_connector] запущен", extra={"prefetch_count": 50})

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: stop_event.set())

    await stop_event.wait()
    await mq.close()
    await engine.dispose()
    logger.info("👋 [worker_connector] остановлен.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass