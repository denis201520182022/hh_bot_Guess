# app/connectors/hh/client.py
import os
import logging
import datetime
import asyncio
import json
from typing import Optional, Dict, Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.config import settings
from app.db.models import Account
from app.core.rabbitmq import mq
from app.utils.redis_lock import DistributedSemaphore, DistributedRateLimiter
from app.utils.logger import logger
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
# Константы HH
HH_API_PER_PAGE_LIMIT = 20
TOKEN_URL = "https://api.hh.ru/token"

# Берем credentials из окружения (как в старом боте)
CLIENT_ID = os.getenv('HH_CLIENT_ID')
CLIENT_SECRET = os.getenv('HH_CLIENT_SECRET')

# Лимиты (через Redis для распределенной работы)
# 1. Общий лимит запросов в секунду (Rate Limit)
HH_API_RATE_LIMITER_GLOBAL = DistributedRateLimiter(name="hh_api_rate", limit=100, period=1.0)
# 2. Лимит одновременных соединений (Concurrency)
GLOBAL_HH_API_LIMIT = int(os.getenv("GLOBAL_HH_API_CONCURRENCY", 10))

class HHClient:
    def __init__(self):
        # Глобальный клиент с пулом соединений
        self._http_client = httpx.AsyncClient(
            timeout=60.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )

    async def close(self):
        """Закрытие клиента при остановке системы"""
        if not self._http_client.is_closed:
            await self._http_client.aclose()
            logger.info("🔒 HH API клиент закрыт")

    async def _send_system_alert(self, text: str):
        """Отправка системного уведомления в Telegram через очередь"""
        try:
            await mq.publish("tg_alerts", {
                "type": "system",
                "text": text,
                "alert_type": "admin_only"
            })
        except Exception as e:
            logger.error(f"Не удалось отправить системный алерт: {e}")

    async def get_token(self, account: Account, db: AsyncSession) -> Optional[str]:
        """
        Получение access_token с автоматическим обновлением через refresh_token.
        Адаптировано под новую модель Account.auth_data.
        """
        if not account.is_active:
            return None
        
        # Данные авторизации из JSONB поля
        auth_data = dict(account.auth_data or {})
        now = datetime.datetime.now(datetime.timezone.utc)

        # 1. Быстрая проверка: если токен еще живет (с запасом 5 минут)
        expires_at_str = auth_data.get("token_expires_at")
        if auth_data.get("access_token") and expires_at_str:
            try:
                expires_at = datetime.datetime.fromisoformat(expires_at_str)
                if expires_at > now + datetime.timedelta(minutes=5):
                    return auth_data["access_token"]
            except ValueError:
                pass

        # 2. РАСПРЕДЕЛЕННАЯ БЛОКИРОВКА (Mutex на уровне аккаунта)
        # Чтобы только один воркер обновлял токен для этого аккаунта
        lock_name = f"hh_token_lock:{account.id}"

        async with DistributedSemaphore(name=lock_name, limit=1, timeout=60):
            # Перечитываем данные из БД (вдруг кто-то уже обновил, пока мы ждали лока)
            await db.refresh(account)
            auth_data = dict(account.auth_data or {})
            
            # Повторная проверка после лока
            expires_at_str = auth_data.get("token_expires_at")
            if auth_data.get("access_token") and expires_at_str:
                expires_at = datetime.datetime.fromisoformat(expires_at_str)
                if expires_at > now + datetime.timedelta(minutes=5):
                    return auth_data["access_token"]

            # Если мы здесь — токен реально пора обновлять
            logger.info(f"🔄 Обновление HH токена для аккаунта {account.name} (ID: {account.id})")
            
            refresh_token = auth_data.get("refresh_token")
            if not refresh_token:
                msg = f"🔴 КРИТИЧЕСКАЯ ОШИБКА: У аккаунта {account.name} (ID: {account.id}) нет refresh_token!"
                logger.error(msg)
                await self._send_system_alert(msg)
                return None

            data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }

            try:
                response = await self._http_client.post(TOKEN_URL, data=data)
                
                if response.status_code == 200:
                    tokens = response.json()
                    
                    # Обновляем структуру auth_data
                    auth_data["access_token"] = tokens["access_token"]
                    if "refresh_token" in tokens:
                        auth_data["refresh_token"] = tokens["refresh_token"]
                    
                    # Срок жизни
                    expires_in = tokens.get("expires_in", 3600)
                    exp_dt = now + datetime.timedelta(seconds=expires_in)
                    auth_data["token_expires_at"] = exp_dt.isoformat()
                    
                    # Сохраняем в БД
                    account.auth_data = auth_data
                    await db.commit()
                    
                    logger.info(f"✅ Успешно получен новый access_token для HH аккаунта {account.name}")
                    return auth_data["access_token"]

                else:
                    # Обработка специфических ошибок HH
                    try:
                        error_data = response.json()
                        error_desc = error_data.get("error_description")
                        
                        if error_desc == "token not expired":
                            logger.info(f"ℹ️ HH вернул 'token not expired' для {account.name}. Продлеваем виртуально.")
                            # Если токен не истек, просто сдвигаем срок в БД на 5 минут, чтобы не спамить рефрешем
                            auth_data["token_expires_at"] = (now + datetime.timedelta(minutes=5)).isoformat()
                            account.auth_data = auth_data
                            await db.commit()
                            return auth_data.get("access_token")
                        
                        elif error_desc in ["password invalidated", "token deactivated"] or error_data.get("oauth_error") == "token-revoked":
                            msg = f"🔴 КРИТИЧЕСКАЯ ОШИБКА HH: Авторизация отозвана для {account.name}. Требуется перезаход!"
                            logger.critical(msg)
                            account.is_active = False # Деактивируем аккаунт
                            await db.commit()
                            await self._send_system_alert(msg)
                            return None
                        else:
                            msg = f"❌ Ошибка обновления токена HH для {account.name}: {response.text}"
                            logger.error(msg)
                            await self._send_system_alert(msg)
                            return None
                            
                    except Exception as parse_e:
                        logger.error(f"Ошибка парсинга ответа HH при рефреше: {parse_e}. Body: {response.text}")
                        return None

            except Exception as e:
                logger.error(f"Сетевая ошибка при обновлении токена HH для {account.name}: {e}")
                return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        retry=retry_if_exception_type((httpx.ConnectTimeout, httpx.ReadTimeout)),
        reraise=True
    )
    async def _request(
        self,
        account: Account,
        db: AsyncSession,
        method: str,
        endpoint: str,
        full_url: str = None,
        **kwargs,
    ) -> Any:
        """
        Универсальный метод запроса к HH API с автоматическим обновлением токенов,
        ретраями и распределенным ограничением нагрузки.
        """
        # 1. Получаем актуальный токен
        token = await self.get_token(account, db)
        if not token:
            raise ConnectionError(f"❌ Не удалось получить токен для аккаунта {account.name}")

        url = full_url or f"https://api.hh.ru/{endpoint}"
        
        # Настройка заголовков
        headers = kwargs.pop('headers', {})
        headers["Authorization"] = f"Bearer {token}"
        headers["HH-User-Agent"] = "ZaBota-Bot/1.0 (hbfys@mail.com)"

        # Логирование запроса (для отладки)
        logger.debug(f"🌐 HH REQUEST: {method} {url} | Params: {kwargs.get('params')}")

        # 2. Ограничение нагрузки через Redis (Rate Limit + Semaphore)
        async with HH_API_RATE_LIMITER_GLOBAL:
            async with DistributedSemaphore(name="hh_api_global", limit=GLOBAL_HH_API_LIMIT):
                response = await self._http_client.request(method, url, headers=headers, **kwargs)

        # 3. Обработка ошибок (4xx, 5xx)
        if response.status_code >= 400:
            logger.warning(f"⚠️ HH API Error {response.status_code}: {response.text}")

            # Специальная обработка 403 (истек или отозван токен)
            if response.status_code == 403:
                should_retry_with_new_token = False
                try:
                    error_data = response.json()
                    oauth_error = error_data.get("oauth_error")

                    if oauth_error in ["token-revoked", "token-expired"]:
                        logger.warning(f"🔄 Токен HH для {account.name} протух (403 {oauth_error}). Сбрасываю и пробую обновить...")
                        
                        # Сбрасываем токен в auth_data
                        auth_data = dict(account.auth_data or {})
                        auth_data["access_token"] = None
                        auth_data["token_expires_at"] = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).isoformat()
                        account.auth_data = auth_data
                        await db.commit()
                        
                        should_retry_with_new_token = True
                except Exception as e:
                    logger.error(f"Ошибка парсинга 403 ответа HH: {e}")

                if should_retry_with_new_token:
                    # Рекурсивно вызываем этот же метод. get_token увидит сброшенный токен и сделает рефреш.
                    return await self._request(account, db, method, endpoint, full_url, **kwargs)

            # Для 201/204 возвращаем None
            if response.status_code in [201, 204]:
                return None

            # Выбрасываем исключение для всех остальных 4xx/5xx
            response.raise_for_status()

        # Возврат JSON, если он есть
        return response.json() if response.content else None

    async def get_responses_from_folder(
        self,
        account: Account,
        db: AsyncSession,
        folder_id: str,
        vacancy_ids: list,
        since_datetime: datetime.datetime = None,
        check_for_updates: bool = False
    ) -> list:
        """
        Асинхронно получает список откликов из указанной папки,
        делая ОТДЕЛЬНЫЙ запрос для КАЖДОЙ вакансии и 'помечая' каждый отклик
        ID его вакансии, обрабатывая ВСЕ страницы или до since_datetime.
        Если check_for_updates=True, запрашивает только отклики с обновлениями.
        """
        logger.debug(
            f"🔎 HH_API: Запрос откликов из папки '{folder_id}' для {len(vacancy_ids)} вакансий"
            f"{(f' с датой от {since_datetime.isoformat()}' if since_datetime else '')}"
            f"{(', только с обновлениями' if check_for_updates else '')}..."
        )

        tasks = []

        for vacancy_id in vacancy_ids:
            if not vacancy_id:
                continue

            async def fetch_for_vacancy(vid):
                all_items_for_vacancy = []
                page = 0
                stop_fetching_for_this_vacancy = False

                if since_datetime:
                    logger.debug(f"  [DEBUG] Для вакансии {vid}, since_datetime (UTC): {since_datetime.isoformat()}")

                try:
                    while True:
                        params = {
                            "vacancy_id": str(vid),
                            "page": str(page),
                            "per_page": str(HH_API_PER_PAGE_LIMIT),
                            "order_by": "created_at",
                            "order": "desc"
                        }

                        # Корректные параметры фильтрации обновлений в зависимости от папки
                        if check_for_updates:
                            if folder_id == 'response':
                                params["show_only_new_responses"] = "true"
                            else:
                                params["show_only_new"] = "true"

                        # Вызов унифицированного запроса внутри класса
                        response_data = await self._request(
                            account, db, "GET", f"negotiations/{folder_id}", params=params
                        )

                        if not response_data or not response_data.get("items"):
                            logger.debug(f"  [DEBUG] Для вакансии {vid}, страница {page}: Нет данных или items пусты. Завершаю пагинацию.")
                            break

                        new_items_to_add = []

                        for item_index, item in enumerate(response_data["items"]):
                            if since_datetime:
                                item_created_at_str = item.get("created_at")
                                if item_created_at_str:
                                    try:
                                        item_created_at = datetime.datetime.fromisoformat(item_created_at_str)
                                        if item_created_at.tzinfo is None:
                                            item_created_at = item_created_at.replace(tzinfo=datetime.timezone.utc)
                                        else:
                                            item_created_at = item_created_at.astimezone(datetime.timezone.utc)

                                        logger.debug(
                                            f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')} ({item_index}):\n"
                                            f"    'created_at' (API string): {item_created_at_str}\n"
                                            f"    Parsed 'created_at' (UTC): {item_created_at.isoformat()}\n"
                                            f"    Сравнение: {item_created_at.isoformat()} < {since_datetime.isoformat()} = {item_created_at < since_datetime}"
                                        )

                                        if item_created_at < since_datetime:
                                            logger.debug(f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')}: ОТБРОШЕН, СТАРЕЕ since_datetime. Активирую раннюю остановку.")
                                            stop_fetching_for_this_vacancy = True
                                            break
                                        else:
                                            new_items_to_add.append(item)
                                            logger.debug(f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')}: ДОБАВЛЕН в new_items_to_add.")
                                    except ValueError:
                                        logger.warning(f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')}: Не удалось распарсить 'created_at'.")
                                        new_items_to_add.append(item)
                                else:
                                    logger.warning(f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')}: Отсутствует поле 'created_at'.")
                                    new_items_to_add.append(item)
                            else:
                                new_items_to_add.append(item)

                        all_items_for_vacancy.extend(new_items_to_add)

                        if stop_fetching_for_this_vacancy:
                             break

                        if page >= response_data.get("pages", 1) - 1:
                            break

                        page += 1

                    # Финальная фильтрация (на случай, если HH вернул что-то не по порядку)
                    if since_datetime:
                        final_filtered_items = []
                        for item in all_items_for_vacancy:
                            item_created_at_str = item.get("created_at")
                            if item_created_at_str:
                                try:
                                    item_created_at = datetime.datetime.fromisoformat(item_created_at_str).astimezone(datetime.timezone.utc)
                                    if item_created_at >= since_datetime:
                                        final_filtered_items.append(item)
                                except ValueError:
                                    final_filtered_items.append(item)
                            else:
                                final_filtered_items.append(item)
                        return [(item, str(vid)) for item in final_filtered_items]
                    else:
                        return [(item, str(vid)) for item in all_items_for_vacancy]

                except Exception as e:
                    logger.error(f"❌ Ошибка запроса откликов для вакансии {vid} (папка {folder_id}, стр {page}): {e}")
                    return []

            tasks.append(fetch_for_vacancy(vacancy_id))

        # Выполняем запросы по всем вакансиям параллельно
        results_from_all_vacancies = await asyncio.gather(*tasks)

        all_responses_with_vacancy_id = []
        for single_vacancy_responses in results_from_all_vacancies:
            all_responses_with_vacancy_id.extend(single_vacancy_responses)

        logger.debug(f"✅ Суммарно найдено {len(all_responses_with_vacancy_id)} откликов в папке '{folder_id}'.")
        return all_responses_with_vacancy_id

    async def get_messages(self, account: Account, db: AsyncSession, messages_url: str) -> list:
        """
        Асинхронно получает ПОЛНУЮ историю сообщений постранично.
        """
        logger.debug(f"🔎 HH_API: Запрос истории сообщений по URL: {messages_url}...")
        all_messages, page = [], 0

        while True:
            try:
                params = {"page": page, "per_page": str(HH_API_PER_PAGE_LIMIT)}
                
                # Вызываем наш универсальный запрос. 
                # Так как messages_url — это полный путь, передаем его в full_url
                response_data = await self._request(
                    account, db, "GET", "", full_url=messages_url, params=params
                )

                if not response_data or not response_data.get("items"):
                    break

                all_messages.extend(response_data["items"])

                # Проверка пагинации
                if page >= response_data.get("pages", 1) - 1:
                    break
                page += 1
            except Exception as e:
                logger.error(f"❌ Ошибка при получении страницы {page} сообщений HH: {e}")
                break

        # Сортируем историю по времени (от старых к новым)
        all_messages.sort(key=lambda x: x.get("created_at", ""))
        return all_messages

    async def send_message(self, account: Account, db: AsyncSession, negotiation_id: str, message_text: str) -> int | bool:
        """
        Асинхронно отправляет сообщение в чат отклика.
        Возвращает 200 при успехе, 403 при фатальной ошибке (вакансия закрыта) или False при сбое.
        """
        logger.info(f"✉️ HH_API: Отправка сообщения в диалог {negotiation_id} (Аккаунт: {account.name})...")
        
        try:
            await self._request(
                account,
                db,
                "POST",
                f"negotiations/{negotiation_id}/messages",
                data={"message": message_text},
            )
            return 200

        except httpx.HTTPStatusError as e:
            # --- ТВОЯ ЛОГИКА ОБРАБОТКИ ФАТАЛЬНЫХ ОШИБОК ---
            if e.response.status_code == 403:
                try:
                    error_body = e.response.json()
                    errors_list = error_body.get("errors", [])

                    # Проверяем на invalid_vacancy (вакансия закрыта) И resume_not_found (резюме удалено)
                    # Если любая из этих ошибок - возвращаем 403, чтобы система знала, что чат мертв.
                    fatal_errors = ["invalid_vacancy", "resume_not_found"]

                    if any(err.get("value") in fatal_errors for err in errors_list):
                        logger.warning(
                            f"⚠️ Сообщение HH не отправлено (Чат: {negotiation_id}). "
                            f"Причина: Вакансия закрыта или резюме удалено. Остановка диалога."
                        )
                        return 403
                except Exception:
                    pass 
            
            logger.error(f"❌ Не удалось отправить сообщение HH в диалог {negotiation_id}: {e}")
            return False

        except Exception as e:
            logger.error(f"❌ Непредвиденная ошибка при отправке сообщения в HH: {e}", exc_info=True)
            return False

    async def move_response_to_folder(self, account: Account, db: AsyncSession, negotiation_id: str, folder_id: str):
        """
        Асинхронно перемещает отклик в указанную папку (PUT запрос).
        """
        logger.info(f"📁 HH_API: Перемещение отклика {negotiation_id} в папку '{folder_id}'...")
        try:
            endpoint = f"negotiations/{folder_id}/{negotiation_id}"
            await self._request(account, db, "PUT", endpoint)
            logger.info(f"✅ УСПЕХ: Отклик {negotiation_id} перемещен в '{folder_id}'.")

        except httpx.HTTPStatusError as e:
            # Твоя логика: если вакансия закрыта или резюме удалено — просто игнорим, перемещать нечего
            if e.response.status_code == 403:
                try:
                    error_body = e.response.json()
                    errors_list = error_body.get("errors", [])
                    fatal_errors = ["resume_not_found", "invalid_vacancy"]

                    if any(err.get("value") in fatal_errors for err in errors_list):
                        logger.warning(f"⚠️ Невозможно переместить {negotiation_id}: {error_body}. Игнорируем.")
                        return 
                except Exception:
                    pass
            
            logger.error(f"❌ Ошибка перемещения отклика {negotiation_id} в {folder_id}: {e}")
            raise e

    async def get_negotiation_folder(self, account: Account, db: AsyncSession, hh_response_id: str) -> Optional[str]:
        """
        Получает ID текущей папки отклика (employer_state).
        """
        try:
            logger.debug(f"🔎 HH_API: Проверка папки для отклика {hh_response_id}...")
            data = await self._request(account, db, "GET", f"negotiations/{hh_response_id}")

            # Твое исправление: используем именно employer_state.id
            if data and data.get("employer_state") and data["employer_state"].get("id"):
                folder_id = data["employer_state"]["id"]
                logger.debug(f"📍 Отклик {hh_response_id} находится в папке '{folder_id}'.")
                return folder_id

            logger.warning(f"❓ Не удалось определить папку для {hh_response_id}.")
            return None

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.info(f"ℹ️ Отклик {hh_response_id} не найден (404).")
                return "404"
            logger.error(f"❌ Ошибка получения папки для {hh_response_id}: {e}")
            return None

    async def get_vacancy_details(self, account: Account, db: AsyncSession, vacancy_id: str) -> Optional[dict]:
        """
        Запрашивает полные данные вакансии (описание, требования).
        """
        logger.debug(f"🔎 HH_API: Запрос деталей вакансии {vacancy_id}")
        try:
            return await self._request(account, db, "GET", f"vacancies/{vacancy_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка получения деталей вакансии {vacancy_id}: {e}")
            return None

    async def get_active_vacancies(self, account: Account, db: AsyncSession) -> list:
        """
        Получает список всех активных вакансий работодателя (для синхронизации).
        """
        logger.debug(f"🔎 HH_API: Получение списка активных вакансий для {account.name}...")
        try:
            # 1. Сначала узнаем ID работодателя через /me
            me_data = await self._request(account, db, "GET", "me")
            employer_id = me_data.get('employer', {}).get('id')
            if not employer_id:
                logger.error(f"❌ Не удалось получить employer_id для {account.name}")
                return []

            # 2. Собираем все активные вакансии постранично
            all_vacancies = []
            page = 0
            while True:
                resp = await self._request(
                    account, db, "GET", f"employers/{employer_id}/vacancies/active",
                    params={'page': page, 'per_page': 100}
                )
                if not resp or not resp.get('items'):
                    break
                
                all_vacancies.extend(resp['items'])
                
                if page >= resp.get('pages', 1) - 1:
                    break
                page += 1
            
            return all_vacancies
        except Exception as e:
            logger.error(f"❌ Ошибка при получении списка активных вакансий: {e}")
            return []
# Экземпляр клиента
hh = HHClient()