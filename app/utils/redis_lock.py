# app/utils/redis_lock.py

import asyncio
import logging
import time
import json
from typing import Optional
from redis.asyncio import Redis

from app.core.config import settings
from app.core.rabbitmq import mq # Для алертов

logger = logging.getLogger("redis_manager")

# Глобальный клиент (Singleton)
_redis_client: Optional[Redis] = None

def get_redis_client() -> Redis:
    """Инициализация и получение асинхронного клиента Redis"""
    global _redis_client
    if _redis_client is None:
        # Берем параметры из твоего центрального конфига
        _redis_client = Redis.from_url(
            settings.REDIS_URL, 
            decode_responses=True,
            # Добавляем параметры надежности
            socket_timeout=5,
            retry_on_timeout=True
        )
    return _redis_client

async def close_redis():
    """Закрытие соединений при остановке"""
    global _redis_client
    if _redis_client:
        await _redis_client.aclose()
        logger.info("🔒 [Action: redis_cleanup] Redis connection closed")

async def send_redis_alert(error_msg: str):
    """Отправка алерта если Redis упал (критично для всей системы)"""
    try:
        await mq.publish("tg_alerts", {
            "type": "system",
            "text": f"🚨 **REDIS CRITICAL ERROR**\n\n{error_msg}",
            "alert_type": "admin_only"
        })
    except:
        logger.error("Не удалось отправить алерт через RabbitMQ")

# --- 1. РАСПРЕДЕЛЕННЫЙ СЕМАФОР (Для OpenAI) ---

class DistributedSemaphore:
    """Контролирует КОЛИЧЕСТВО ОДНОВРЕМЕННЫХ запросов между всеми воркерами"""
    def __init__(self, name: str, limit: int, timeout: int = 60):
        self.client = get_redis_client()
        self.name = f"semaphore:{name}"
        self.limit = limit
        self.timeout = timeout 

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

    async def acquire(self):
        start_wait = time.time()
        while True:
            # Lua-скрипт гарантирует атомарность (никто не вклинится между GET и INCR)
            lua_script = """
            local current = redis.call('get', KEYS[1])
            if current == false then current = 0 else current = tonumber(current) end

            if current < tonumber(ARGV[1]) then
                redis.call('incr', KEYS[1])
                redis.call('expire', KEYS[1], ARGV[2])
                return 1
            else
                return current
            end
            """
            try:
                result = await self.client.eval(lua_script, 1, self.name, self.limit, self.timeout)
                if result == 1:
                    return True
                
                # Если занято — логируем раз в 10 секунд
                if int(time.time() - start_wait) % 10 == 0:
                    logger.info(f"⏳ [Action: semaphore_wait] '{self.name}' is FULL ({result}/{self.limit})")
                
            except Exception as e:
                logger.error(f"❌ Redis Semaphore Error: {e}")
                await send_redis_alert(f"Semaphore {self.name} failed: {e}")
                await asyncio.sleep(2)
            
            # Спим чуть-чуть перед следующей попыткой
            await asyncio.sleep(0.1)

    async def release(self):
        lua_script = """
        local current = redis.call('get', KEYS[1])
        if current and tonumber(current) > 0 then
            redis.call('decr', KEYS[1])
            return 1
        end
        return 0
        """
        try:
            await self.client.eval(lua_script, 1, self.name)
        except Exception as e:
            logger.error(f"❌ Redis Release Error: {e}")

# --- 2. БЛОКИРОВКИ (Для предотвращения Race Condition) ---

async def acquire_lock(key: str, timeout: int = 60) -> bool:
    """Захватывает эксклюзивный доступ к ресурсу (например, к диалогу)"""
    client = get_redis_client()
    lock_key = f"lock:{key}"
    try:
        # NX=True (только если нет), EX=timeout (автоудаление)
        return await client.set(lock_key, "1", nx=True, ex=timeout)
    except Exception as e:
        logger.error(f"❌ Redis Lock Error ({key}): {e}")
        return False

async def release_lock(key: str):
    """Освобождает блокировку"""
    client = get_redis_client()
    try:
        await client.delete(f"lock:{key}")
    except Exception as e:
        logger.error(f"❌ Redis Unlock Error: {e}")

# --- 3. RATE LIMITER (Ограничение скорости запросов в сек/мин) ---

class DistributedRateLimiter:
    """Ограничивает ЧАСТОТУ запросов (например, 100 запросов в минуту)"""
    def __init__(self, name: str, limit: int, period: int):
        self.client = get_redis_client()
        self.key = f"rate_limit:{name}"
        self.limit = limit
        self.period = period
        self._lua_script = """
        local current = redis.call('incr', KEYS[1])
        if current == 1 then
            redis.call('expire', KEYS[1], ARGV[2])
        end
        if current > tonumber(ARGV[1]) then
            return {0, redis.call('ttl', KEYS[1])}
        end
        return {1, 0}
        """

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass  # Rate limiter не требует очистки после выхода

    async def acquire(self):
        while True:
            try:
                # result[0] - статус (1-ок, 0-лимит), result[1] - сколько ждать
                res = await self.client.eval(self._lua_script, 1, self.key, self.limit, self.period)
                if res[0] == 1:
                    return True
                
                wait_time = max(res[1], 1)
                logger.debug(f"🐢 [Action: rate_limit_hit] '{self.key}' exceeded. Waiting {wait_time}s")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"❌ Redis RateLimit Error: {e}")
                await asyncio.sleep(1)