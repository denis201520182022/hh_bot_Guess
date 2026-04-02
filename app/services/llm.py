# app/services/llm.py
import os
import json
import logging
import asyncio
import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
import httpx
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import settings
from app.core.rabbitmq import mq
# Предполагаем, что этот путь будет таким (реализуем в след. файле)
from app.utils.redis_lock import DistributedSemaphore, close_redis

load_dotenv()
logger = logging.getLogger("llm_service")

from app.core.config import settings



SMART_MODEL=settings.llm.smart_model
MAIN_MODEL=settings.llm.main_model
MAX_TOKENS=settings.llm.max_tokens
TEMPERATURE=settings.llm.temperature
request_timeout=settings.llm.request_timeout


# Лимит одновременных запросов (чтобы не ловить Rate Limit от OpenAI)
GLOBAL_LLM_LIMIT = settings.llm.global_concurrency

# --- НАСТРОЙКА ПРОКСИ ---
SQUID_HOST = os.getenv("SQUID_PROXY_HOST")
SQUID_PORT = os.getenv("SQUID_PROXY_PORT")
SQUID_USER = os.getenv("SQUID_PROXY_USER")
SQUID_PASS = os.getenv("SQUID_PROXY_PASSWORD")

proxy_url = None
if SQUID_HOST:
    proxy_url = f"http://{SQUID_USER}:{SQUID_PASS}@{SQUID_HOST}:{SQUID_PORT}"

async_http_client = httpx.AsyncClient(
    proxy=proxy_url,
    timeout=request_timeout
)

client = AsyncOpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    http_client=async_http_client
)

if proxy_url:
    logger.info(f"🌐 OpenAI клиент настроен через прокси: {SQUID_HOST}:{SQUID_PORT}")



# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

async def send_llm_alert(error_type: str, error_msg: str, diag_id: Any):
    """Отправка алерта при критическом сбое LLM"""
    try:
        payload = {
            "type": "system",
            "text": f"🚨 **LLM CRITICAL ERROR**\n\n**Тип:** {error_type}\n**Диалог:** `{diag_id}`\n**Ошибка:** {error_msg}",
            "alert_type": "admin_only"
        }
        await mq.publish("tg_alerts", payload)
    except:
        logger.error("Не удалось отправить алерт о сбое LLM")

def calculate_usage(usage, model_name: str) -> Dict[str, Any]:
    """Детальный расчет токенов и кэша (как в HH боте)"""
    cached_tokens = 0
    if hasattr(usage, "prompt_tokens_details") and usage.prompt_tokens_details:
        cached_tokens = getattr(usage.prompt_tokens_details, "cached_tokens", 0)
    
    prompt_tokens = usage.prompt_tokens
    cache_pc = (cached_tokens / prompt_tokens * 100) if prompt_tokens > 0 else 0
    
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
        "cached_tokens": cached_tokens,
        "cache_percentage": round(cache_pc, 1),
        "model": model_name
    }


# --- ОСНОВНЫЕ МЕТОДЫ ---

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
async def get_bot_response(
    system_prompt: str, 
    dialogue_history: List[Dict], 
    user_message: str, 
    extra_context: Optional[Dict] = None,
    attempt_tracker: Optional[List] = None
) -> Dict[str, Any]:
    """
    Основной метод диалога. 
    Использует РАСПРЕДЕЛЕННЫЙ СЕМАФОР для контроля лимитов между воркерами.
    """
    if attempt_tracker is not None:
        attempt_tracker.append(datetime.datetime.now())

    diag_id = (extra_context or {}).get("dialogue_id", "unknown")
    ctx_logger = logging.LoggerAdapter(logger, extra_context or {})
    
    # Добавляем в системный промпт текущее время (важно для дат)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_system_prompt = f"{system_prompt}\n\nCurrent time: {now_str}"

    messages = [{"role": "system", "content": full_system_prompt}]
    messages.extend(dialogue_history)
    messages.append({"role": "user", "content": user_message})

    try:
        ctx_logger.info(f"🧬🧬🧬 [Action: llm_request_start] Model: {MAIN_MODEL}")

        # Используем Redis-семафор "llm_global"
        async with DistributedSemaphore(name="llm_global", limit=GLOBAL_LLM_LIMIT):
            response = await client.chat.completions.create(
                model=MAIN_MODEL,
                messages=messages,
                max_completion_tokens=MAX_TOKENS,
                response_format={"type": "json_object"},
                frequency_penalty=0.7,
                temperature=TEMPERATURE
            )

        content = response.choices[0].message.content
        stats = calculate_usage(response.usage, MAIN_MODEL)

        ctx_logger.info(
            f"✅ LLM Response. [Action: llm_response_success] "
            f"Tokens: {stats['total_tokens']} (Cached: {stats['cache_percentage']}%)"
        )

        return {
            "parsed_response": json.loads(content),
            "usage_stats": stats
        }

    except Exception as e:
        error_name = type(e).__name__
        ctx_logger.warning(f"⚠️ [Action: llm_request_retry] Error: {error_name}: {e}")
        
        # Если это последняя попытка (retry провалился 3 раза)
        # Tenacity выбросит исключение дальше, но мы успеем отправить алерт
        if attempt_tracker and len(attempt_tracker) >= 3:
            await send_llm_alert(f"Final Retry Failed ({MAIN_MODEL})", str(e), diag_id)
            
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
async def get_smart_bot_response(
    system_prompt: str, 
    dialogue_history: List[Dict], 
    user_message: str, 
    extra_context: Optional[Dict] = None,
    attempt_tracker: Optional[List] = None
) -> Dict[str, Any]:
    """
    Метод для сложных задач (gpt-4o).
    """
    if attempt_tracker is not None:
        attempt_tracker.append(datetime.datetime.now())

    diag_id = (extra_context or {}).get("dialogue_id", "unknown")
    ctx_logger = logging.LoggerAdapter(logger, extra_context or {})
    
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Для моделей o1/gpt-4o рекомендуется роль developer
    messages = [
        {"role": "developer", "content": f"{system_prompt}\nContext time: {now_str}"}
    ]
    messages.extend(dialogue_history)
    messages.append({"role": "user", "content": user_message})

    try:
        ctx_logger.info(f"🧠🧠 [Action: smart_llm_start] Model: {SMART_MODEL}")

        async with DistributedSemaphore(name="llm_global", limit=GLOBAL_LLM_LIMIT):
            response = await client.chat.completions.create(
                model=SMART_MODEL,
                messages=messages,
                max_completion_tokens=MAX_TOKENS,
                response_format={"type": "json_object"},
                temperature=TEMPERATURE
            )

        content = response.choices[0].message.content
        stats = calculate_usage(response.usage, SMART_MODEL)

        ctx_logger.info(
            f"✅ SMART Response. [Action: smart_llm_success] "
            f"Tokens: {stats['total_tokens']} (Cached: {stats['cache_percentage']}%)"
        )

        return {
            "parsed_response": json.loads(content),
            "usage_stats": stats
        }

    except Exception as e:
        ctx_logger.warning(f"⚠️ [Action: smart_llm_retry] Error: {e}")
        if attempt_tracker and len(attempt_tracker) >= 3:
            await send_llm_alert(f"Final Smart Retry Failed", str(e), diag_id)
        raise

async def cleanup_llm():
    """Полная очистка при выключении"""
    await async_http_client.aclose()
    await close_redis()
    logger.info("🔒 [Action: llm_cleanup] HTTP client and Redis connections closed")