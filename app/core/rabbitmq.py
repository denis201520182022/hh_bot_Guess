# app/core/rabbitmq.py
import json
import aio_pika
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

class RabbitMQManager:
    def __init__(self):
        # Используем URL из наших новых настроек
        self.url = settings.RABBITMQ_URL
        self.connection = None
        self.channel = None

    async def connect(self):
        """Установка соединения и создание основных очередей"""
        if not self.connection or self.connection.is_closed:
            try:
                self.connection = await aio_pika.connect_robust(self.url)
                self.channel = await self.connection.channel()
                
                # --- ОБЪЯВЛЯЕМ ОЧЕРЕДИ ---

                # === ВХОДЯЩИЕ СОБЫТИЯ ОТ ПЛАТФОРМ (Универсальная точка входа) ===
                # Avito
                await self.channel.declare_queue("avito_inbound", durable=True)
                # HH
                await self.channel.declare_queue("hh_inbound", durable=True)

                # Задачи для ИИ движка (универсальная)
                await self.channel.declare_queue("engine_tasks", durable=True)

                # Исходящие сообщения (для отправки в платформы входа)
                await self.channel.declare_queue("outbound_messages", durable=True)

                # Системные алерты и уведомления (Telegram)
                await self.channel.declare_queue("tg_alerts", durable=True)
                await self.channel.declare_queue("services_output", durable=True) #для тг, срм, гугл таблиц, короче каналы выхода
                
                
                
                logger.info("✅ Подключение к RabbitMQ установлено, очереди инициализированы.")
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к RabbitMQ: {e}")
                raise e

    async def publish(self, queue_name: str, message: dict):
        """Отправка сообщения в очередь"""
        if not self.connection or self.connection.is_closed or not self.channel or self.channel.is_closed:
            await self.connect()
            
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message, ensure_ascii=False).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=queue_name
        )

    async def close(self):
        if self.connection:
            await self.connection.close()
            logger.info("🔌 Соединение с RabbitMQ закрыто.")

# Глобальный экземпляр для импорта
mq = RabbitMQManager()
