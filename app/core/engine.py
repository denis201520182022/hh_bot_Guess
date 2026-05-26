# app/core/engine.py hgyuf
import logging
import asyncio
import json
# app/core/engine.py (в секции импортов)
from app.utils.pii_masker import extract_and_mask_pii
from sqlalchemy import select, update, delete
import datetime
from app.services.sheets import sheets_service
import time
from sqlalchemy import select, update, delete # Добавили delete
from app.db.models import Dialogue, Candidate, JobContext, Account, LlmLog, AnalyticsEvent, Director, InterviewReminder, InterviewFollowup # Добавили AnalyticsEvent, Director
from app.core.rabbitmq import mq
from typing import Dict, Any, List, Optional
from decimal import Decimal
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.attributes import flag_modified
from app.services.sheets import sheets_service

from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal
from sqlalchemy import select, update, delete # Добавить delete
from app.db.models import Dialogue, Candidate, JobContext, Account, LlmLog, AnalyticsEvent # Добавить AnalyticsEvent
from app.connectors import get_connector
from app.connectors.hh import hh_connector, hh
from app.output_chanels.talantix.talantix_crm import talantix_crm_service
from app.services.talantix_service import talantix_service
# Наши модули
from app.utils.analytics import log_event
from app.utils.redis_lock import acquire_lock, release_lock
from app.db.session import AsyncSessionLocal
from app.db.models import Dialogue, Candidate, JobContext, Account, LlmLog
from app.services.knowledge_base import kb_service
from app.services.llm import get_bot_response, get_smart_bot_response

from app.core.config import settings
from app.db.models import InterviewReminder
from app.db.models import LlmLog
from app.db.models import Dialogue, Candidate, JobContext, Account, AnalyticsEvent
from sqlalchemy import delete
from app.core.config import settings
from app.db.models import InterviewReminder
from sqlalchemy import delete
from app.utils.pii_masker import extract_and_mask_pii 
from app.core.exceptions import DialogueLockedError

from zoneinfo import ZoneInfo
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


# Настройка логгера
logger = logging.getLogger("Engine")

class Engine:
    """
    Мозг системы. Полный аналог run_hh_worker.py, но адаптированный под Event-Driven архитектуру.
    """
    # --- ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ (МЯСО ДВИЖКА) ---

    COST_LIMIT_ALERT = 8.0  # Рублей
    COST_LIMIT_BLOCK = 20.0 # Рублей
    RUB_RATE = 85.0         # Курс для расчета

    def _get_history_as_text(self, dialogue: Dialogue) -> str:
        """Формирует текстовый файл истории диалога для алертов"""
        lines = [
            f"=== ИСТОРИЯ ДИАЛОГА (ID: {dialogue.id}) ===",
            f"Chat ID: {dialogue.external_chat_id}",
            f"Кандидат: {dialogue.candidate.full_name or 'Аноним'}",
            f"Вакансия: {dialogue.vacancy.title if dialogue.vacancy else 'Не указана'}",
            "-" * 50
        ]
        for entry in (dialogue.history or []):
            role = "👤 Кандидат" if entry.get('role') == 'user' else "🤖 Бот"
            content = entry.get('content', '')
            content_str = str(content)
            
            # ФИЛЬТР: Пропускаем и системные команды бота, и мусор Авито
            if not content_str.startswith('[SYSTEM') and not content_str.startswith('[Системное сообщение]'):
                lines.append(f"{role}: {content}")
        return "\n".join(lines)

    
    
    def _is_technical_message(self, content: Any) -> bool:
        """Определяет, является ли сообщение системным/техническим мусором."""
        if not isinstance(content, str):
            return False
        
        content_strip = content.strip()
        # Список маркеров, которые мы не хотим показывать LLM
        forbidden_markers = [
            
            "[Системное сообщение]"
        ]
        
        return any(marker in content_strip for marker in forbidden_markers)
    
    async def _get_human_slots_block(self, sheet_name: str = "Calendar") -> str:
        """Формирует текстовый блок со свободными слотами для промпта."""
        all_slots = await sheets_service.get_all_slots_map(sheet_name)
        if not all_slots:
            return "\n[ИНФОРМАЦИЯ О СЛОТАХ] На данный момент свободных окон в графике нет."

        moscow_tz = ZoneInfo("Europe/Moscow")
        now_msk = datetime.datetime.now(moscow_tz)
        today_str = now_msk.strftime("%Y-%m-%d")

        weekdays = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
        months = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"]

        lines = ["\n[СПИСОК ДОСТУПНЫХ ОКОН ДЛЯ ЗАПИСИ]:"]
        
        # Сортируем даты по порядку
        for date_iso in sorted(all_slots.keys()):
            slots = all_slots[date_iso]
            if not slots:
                continue

            dt = datetime.datetime.strptime(date_iso, "%Y-%m-%d")
            
            # Пропускаем прошедшие дни
            if dt.date() < now_msk.date():
                continue
                
            # Если день сегодняшний, фильтруем прошедшие часы
            if date_iso == today_str:
                slots = [s for s in slots if int(s.split(':')[0]) > now_msk.hour]
                if not slots:
                    continue

            human_date = f"{dt.day} {months[dt.month - 1]} ({weekdays[dt.weekday()]})"
            lines.append(f"• {human_date}: {', '.join(slots)}")

        return "\n".join(lines)

    def _validate_age_in_text(self, text: str, suggested_age: Any) -> bool:
        """Проверяет, соответствует ли извлеченный LLM возраст тому, что реально написал пользователь."""
        if not suggested_age:
            return False
        try:
            age_to_check = int(suggested_age)
        except (ValueError, TypeError):
            return False

        import re
        if re.search(r'(?<!\d)' + str(age_to_check) + r'(?!\d)', text):
            return True

        age_words = {
            14: "четырнадцать", 15: "пятнадцать", 16: "шестнадцать",
            17: "семнадцать", 18: "восемнадцать", 19: "девятнадцать",
            20: "двадцать", 21: "двадцать один", 22: "двадцать два",
            23: "двадцать три", 24: "двадцать четыре", 25: "двадцать пять",
            26: "двадцать шесть", 27: "двадцать семь", 28: "двадцать восемь",
            29: "двадцать девять", 30: "тридцать"
        }
        if age_to_check in age_words:
            word = age_words[age_to_check]
            if word in text.lower():
                return True

        # 3. Дополнительная проверка (оставляем без изменений)
        all_numbers_in_text = re.findall(r'\b(1[4-9]|[2-6][0-9]|70)\b', text)
        
        if all_numbers_in_text and str(age_to_check) not in all_numbers_in_text:
            # Если нашли числа, но нашего среди них нет, пробуем еще раз с мягким поиском
            # (на случай если re.findall выше что-то упустил из-за границ слов)
            if re.search(r'(?<!\d)' + str(age_to_check) + r'(?!\d)', text):
                return True
            logger.warning(f"AGE VALIDATION FAILED: LLM suggested {age_to_check}, but found {all_numbers_in_text} in text.")
            return False

        if not all_numbers_in_text:
            # Если чисел вообще не нашли (например "мне полтинник"), а LLM нашла - это подозрительно, но
            # если мы прошли проверку №1, мы бы уже вернули True.
            return False

        return True

    async def _log_llm_usage(self, db: AsyncSession, dialogue: Dialogue, context: str, usage_stats: dict = None, model_name: str = "gpt-4o-mini"):
        """
        Универсальная функция для подсчета токенов и стоимости.
         пишет статистику в JSONB.
        """
        
        try:
            # 1. Выбор тарифа (Копия из HH)
            MODEL_PRICING = {
                "gpt-4o-mini": {"input": 0.150, "output": 0.600},
                "gpt-4o": {"input": 2.500, "output": 10.000}
            }
            pricing = MODEL_PRICING.get(model_name, MODEL_PRICING["gpt-4o-mini"])
            price_input = pricing["input"]
            price_output = pricing["output"]

            # 2. Извлечение данных
            stats = usage_stats or {}
            p_tokens = stats.get('prompt_tokens', 0)
            c_tokens = stats.get('completion_tokens', 0)
            cached_tokens = stats.get('cached_tokens', 0)
            total_tokens = stats.get('total_tokens', 0)

            # 3. Расчет стоимости (Логика HH: скидка 50% на кэш)
            non_cached_input = max(0, p_tokens - cached_tokens)
            
            cost_input_regular = (non_cached_input / 1_000_000) * price_input
            cost_input_cached = (cached_tokens / 1_000_000) * (price_input / 2) 
            cost_output = (c_tokens / 1_000_000) * price_output
            
            # Используем Decimal для точности, как в HH
            total_call_cost = Decimal(str(cost_input_regular + cost_input_cached + cost_output))

            # 4. Создание записи лога (Таблица LlmLog)
            
            
            usage_log = LlmLog(
                dialogue_id=dialogue.id,
                prompt_type=f"{context} ({model_name})", # Аналог dialogue_state_at_call
                model=model_name,
                prompt_tokens=p_tokens,
                completion_tokens=c_tokens,
                
                cost=total_call_cost
            )
            db.add(usage_log)

            # 5. Обновление счетчиков диалога (JSONB usage_stats)
            if total_tokens > 0:
                # Берем текущий JSON или пустой словарь
                current_stats = dict(dialogue.usage_stats or {})
                
                # Извлекаем старую стоимость, конвертируем в Decimal для сложения
                prev_cost = Decimal(str(current_stats.get("total_cost", 0)))
                new_total_cost = prev_cost + total_call_cost
                
                # Обновляем общие счетчики
                current_stats["total_cost"] = float(new_total_cost) # JSON не поддерживает Decimal, конвертируем обратно во float
                current_stats["tokens"] = current_stats.get("tokens", 0) + total_tokens
                
                # Сохраняем детализацию (как было в колонках HH бота)
                current_stats["total_prompt_tokens"] = current_stats.get("total_prompt_tokens", 0) + p_tokens
                current_stats["total_completion_tokens"] = current_stats.get("total_completion_tokens", 0) + c_tokens
                current_stats["total_cached_tokens"] = current_stats.get("total_cached_tokens", 0) + cached_tokens
                
                dialogue.usage_stats = current_stats
                

        except Exception as e:
            logger.error(f"Ошибка при логировании токенов ({context}): {e}")

    async def _verify_date_audit(self, db: AsyncSession, dialogue: Dialogue, suggested_date: str, history_messages: list, calendar_context: str, log_extra: dict) -> str:
        """
        Техническая проверка даты (Аудит). Возвращает исправленную дату в формате YYYY-MM-DD.
        """
        # 1. Фильтруем системные команды и сопоставляем роли для понимания GPT-4o
        clean_history_lines = []
        for m in history_messages:
            content = m.get('content', '')
            # Пропускаем технические команды
            if self._is_technical_message(content):
                continue
            if isinstance(content, str) and content.startswith('[SYSTEM COMMAND]'):
                continue
            
            # Определяем человекочитаемую роль
            role_label = "Кандидат" if m.get('role') == 'user' else "Бот"
            clean_history_lines.append(f"{role_label}: {content}")

        # 2. Берем последние 10 сообщений для анализа даты
        recent_text = "\n".join(clean_history_lines[-10:])
        
        verify_prompt = (
            f"Ты — строгий технический аудитор системы записи. Твоя задача — проверить, соответствует ли дата, предложенная первой моделью, ЖЕЛАНИЮ КАНДИДАТА когда пройти собеседование.\n\n"
            f"[ДИНАМИЧЕСКИЙ КАЛЕНДАРЬ]\n{calendar_context}\n\n"
            f"[ВВОДНЫЕ ДАННЫЕ]\n"
            f"Первая модель предлагает записать на: {suggested_date}\n\n"
            f"⚠️ КРИТИЧЕСКИ ВАЖНЫЕ ПРАВИЛА АНАЛИЗА (ЧИТАЙ ВНИМАТЕЛЬНО):\n"
            f"Очень важно!!! выезд на вахту (готовность приступить к работа) - это другой вопрос, который не имеет отношения к записи на собеседование. Ответ на этот вопрос игнорируй и не определяй дату по нему!\n"
            f"1. АБСОЛЮТНЫЙ ПРИОРИТЕТ — у ПОСЛЕДНЕГО сообщения кандидата.\n"
            f"   - Если ранее договорились на одну дату, но в конце кандидат спросил 'А можно завтра?' или 'Давайте в пятницу' — значит, он ПЕРЕДУМАЛ.\n"
            f"   - В этом случае ты ОБЯЗАН вернуть новую дату (завтра/пятницу), а не ту, что была согласована ранее.\n\n"
            f"2. Сверь дату с календарем:\n"
            f"   - Если кандидат говорит например 'завтра' — ищи в календаре строку с меткой ЗАВТРА.\n"
            f"   - Если кандидат говорит день недели (напр. Вторник) — бери ближайший Вторник из таблицы (если не сказано 'следующий').\n\n"
            f"3. Вердикт:\n"
            f"   - Если {suggested_date} совпадает с ПОСЛЕДНИМ желанием кандидата — верни её.\n"
            f"   - Если первая модель проигнорировала смену даты кандидатом — верни ПРАВИЛЬНУЮ дату.\n"
            f"   - Если кандидат в диалоге вообще не называл дату или отказался называть — верни 'none'.\n\n"
            f"Ответ строго в формате JSON:\n"
            f"{{\n"
            f'  "reasoning": "Обоснование. Пример: Кандидат сначала согласился на 20-е число, но в последнем сообщении спросил про завтра (21-е). Первая модель ошиблась, оставив 20-е. Не более 2 предложений, пиши кратко",\n'
            f'  "correct_date": "YYYY-MM-DD или none"\n'
            f"}}"
        )

        verify_attempts = []
        try:
            # Вызываем LLM через твой обработчик
            response = await get_smart_bot_response(
                system_prompt=verify_prompt,
                dialogue_history=[],
                user_message=f"ИСТОРИЯ ДИАЛОГА:\n{recent_text}",
                
                attempt_tracker=verify_attempts,
                extra_context=log_extra
            )

            if response and 'usage_stats' in response:
                await self._log_llm_usage(db, dialogue, "Date_Audit_Call", response['usage_stats'], model_name="gpt-4o")

            parsed = response.get('parsed_response', {})
            return parsed.get("correct_date", suggested_date), parsed.get("reasoning", "Без обоснования")
        except Exception as e:
            logger.error(f"Критическая ошибка аудита даты: {e}", extra=log_extra)
            return suggested_date # В случае падения пропускаем как есть (fallback)
        

    def _check_eligibility(self, profile: dict) -> tuple[bool, str | None]:
        """
        Возвращает (True, None) если подходит, или (False, "reason") если отказ.
        Проверка по критериям клиента.
        ВАЖНО: отсутствие данных НЕ считается отказом — проверяем только то, что уже известно.
        """
        # --- Критерий 1: Возраст (18-50) ---
        age = profile.get("age")
        if age is not None:
            try:
                age_val = int(age)
                if not (18 <= age_val <= 50):
                    return False, f"age_out_of_range_{age_val}"
            except (ValueError, TypeError):
                pass

        # --- Критерий 2: Гражданство (только РФ) ---
        citizenship = str(profile.get("citizenship", "")).strip().lower()
        if citizenship:
            is_rf = any(x in citizenship for x in ["россия", "рф", "российская", "russia"])
            if not is_rf:
                return False, "non_rf_citizenship"

        # --- Критерий 3: Военный билет для мужчин ---
        gender = str(profile.get("gender", "")).strip().lower()
        if gender == "male":
            military_doc = str(profile.get("has_military_document", "")).strip().lower()
            if military_doc == "no":
                return False, "no_military_document_male"

        # --- Критерий 4: Готовность к ТК РФ ---
        contract_ready = str(profile.get("employment_contract_ready", "")).strip().lower()
        if contract_ready == "no":
            return False, "not_ready_for_tk_rf"

        # --- Критерий 5: Тип занятости и готовность к часам ---
        employment_type = str(profile.get("employment_type", "")).strip().lower()
        if employment_type == "part":
            ready_hours = str(profile.get("ready_20_40_hours", "")).strip().lower()
            if ready_hours == "no":
                return False, "part_time_not_ready_for_hours"
        # Если full — подходит, если неизвестно — пропускаем

        return True, None
    
    def _generate_calendar_context_2(self, slots_data: Optional[Dict[str, List[str]]] = None) -> str:
        """
        Генерирует расширенный текстовый блок с календарем на 3 недели и доступными слотами.
        slots_data: словарь { "2026-02-12": ["10:00", "12:00"], ... }
        """
        moscow_tz = ZoneInfo("Europe/Moscow")
        now_msk = datetime.datetime.now(moscow_tz)
        
        weekdays_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        months_ru = [
            "января", "февраля", "марта", "апреля", "мая", "июня",
            "июля", "августа", "сентября", "октября", "ноября", "декабря"
        ]
        
        weekday_next_form = {
            "Понедельник": "Следующий понедельник",
            "Вторник": "Следующий вторник",
            "Среда": "Следующая среда",
            "Четверг": "Следующий четверг",
            "Пятница": "Следующая пятница",
            "Суббота": "Следующая суббота",
            "Воскресенье": "Следующее воскресенье"
        }

        current_weekday = weekdays_ru[now_msk.weekday()]
        current_date_str = now_msk.strftime("%Y.%m.%d")
        current_time_str = now_msk.strftime("%H:%M")

        calendar_context_lines = []

        # Заголовок таблицы (Добавлен столбец AVAILABLE_SLOTS)
        calendar_context_lines.append(
            "IDX | DATE | WEEKDAY | RELATIVE | HUMAN_LABEL | AVAILABLE_SLOTS | MARKER"
        )

        for i in range(21):
            date_cursor = now_msk + datetime.timedelta(days=i)
            wd_idx = date_cursor.weekday()
            wd_name = weekdays_ru[wd_idx]

            day = date_cursor.day
            month_name = months_ru[date_cursor.month - 1]
            date_dotted = date_cursor.strftime("%Y.%m.%d")
            date_iso = date_cursor.strftime("%Y-%m-%d")

            # Определяем статус
            if i == 0:
                relative = "СЕГОДНЯ"
                human_label = f"сегодня {wd_name} - {day} {month_name}"
            elif i == 1:
                relative = "ЗАВТРА"
                human_label = f"завтра {wd_name} - {day} {month_name}"
            elif i == 2:
                relative = "ПОСЛЕЗАВТРА"
                human_label = f"послезавтра {wd_name} - {day} {month_name}"
            elif 7 <= i < 14:
                relative = "СЛЕДУЮЩАЯ_НЕДЕЛЯ"
                human_label = f"{weekday_next_form[wd_name]} - {day} {month_name}"
            elif i >= 14:
                relative = "ЧЕРЕЗ_НЕДЕЛЮ"
                human_label = f"Через неделю в {wd_name.lower()} - {day} {month_name}"
            else:
                relative = ""
                human_label = f"{wd_name} - {day} {month_name}"

            # --- ИСПРАВЛЕННЫЙ БЛОК ЛОГИКИ СЛОТОВ ---
            if slots_data is None:
                # Если данные не переданы (для Аудитора), просто ставим прочерк
                slots_str = "---"
            else:
                # Получаем слоты из переданных данных
                day_slots = slots_data.get(date_iso, [])
                
                # Если это сегодня — фильтруем слоты, которые уже прошли (+1 час запаса на дорогу)
                if i == 0 and day_slots:
                    day_slots = [s for s in day_slots if int(s.split(':')[0]) > now_msk.hour]

                # Форматируем строку слотов
                if wd_idx == 6: # Воскресенье
                    slots_str = "ВЫХОДНОЙ"
                elif not day_slots:
                    slots_str = "МЕСТ НЕТ"
                else:
                    slots_str = ", ".join(day_slots)

            marker = "ТЫ_ЗДЕСЬ" if i == 0 else ""

            line = (
                f"{i} | "
                f"{date_dotted} | "
                f"{wd_name} | "
                f"{relative} | "
                f"{human_label} | "
                f"{slots_str} | "
                f"{marker}"
            )

            calendar_context_lines.append(line)

        calendar_string = "\n".join(calendar_context_lines)

        calendar_context = (
            f"\n\n[CRITICAL CALENDAR CONTEXT]\n"
            f"ТЕКУЩАЯ ДАТА И ВРЕМЯ (МСК): {now_msk.strftime('%Y-%m-%d %H:%M')}\n"
            f"СЕГОДНЯ: {current_weekday}, {current_date_str}\n\n"
            f"СЕЙЧАС: {current_time_str} (МСК)\n"
            f"⚠️ ВАЖНО: Ты ОЧЕНЬ ПЛОХО считаешь даты в уме. НИКОГДА НЕ ВЫЧИСЛЯЙ ДАТЫ САМОСТОЯТЕЛЬНО!\n"
            f"Используй ТОЛЬКО эту таблицу (таблица начинается с СЕГОДНЯ и идет на 21 дней вперед):\n\n"
            f"{calendar_string}\n\n"

            f"ОПИСАНИЕ КОЛОНОК:\n"
            f"IDX — порядковый номер строки\n"
            f"DATE — дата (ЕДИНСТВЕННЫЙ источник истины)\n"
            f"WEEKDAY — день недели\n"
            f"RELATIVE — относительный статус дня\n"
            f"HUMAN_LABEL — человекочитаемая подпись\n"
            f"AVAILABLE_SLOTS — список доступного времени (предлагай ТОЛЬКО его)\n"
            f"MARKER — специальные метки (например, ТЫ_ЗДЕСЬ)\n\n"

            f"ПРАВИЛА РАБОТЫ С ДАТАМИ:\n"
            f"1. Если кандидат говорит ТОЛЬКО день недели ('понедельник', 'вторник'):\n"
            f"   → Найди ПЕРВУЮ строку, где WEEKDAY совпадает\n"
            f"   → И поле RELATIVE пустое\n"
            f"   → Скопируй DATE\n\n"

            f"2. Если кандидат говорит 'СЛЕДУЮЩИЙ <день недели>' (например, 'следующий понедельник'):\n"
            f"   → Найди строку, где WEEKDAY совпадает\n"
            f"   → И RELATIVE = СЛЕДУЮЩАЯ_НЕДЕЛЯ\n"
            f"   → Скопируй DATE\n\n"

            f"3. Если кандидат говорит 'сегодня':\n"
            f"   → Найди строку, где RELATIVE = СЕГОДНЯ\n"
            f"   → Скопируй DATE\n\n"

            f"4. Если кандидат говорит 'завтра':\n"
            f"   → Найди строку, где RELATIVE = ЗАВТРА\n"
            f"   → Скопируй DATE\n\n"

            f"5. Если кандидат говорит 'послезавтра':\n"
            f"   → Найди строку, где RELATIVE = ПОСЛЕЗАВТРА\n"
            f"   → Скопируй DATE\n\n"

            f"6. Если кандидат называет дату:\n"
            f"   → Найди строку, где DATE совпадает\n"
            f"   → Используй эту DATE\n\n"

            f"7. Если кандидат называет день недели, совпадающий с сегодняшним, но НЕ говорит 'сегодня':\n"
            f"   → Найди строку, где WEEKDAY совпадает\n"
            f"   → И RELATIVE = СЛЕДУЮЩАЯ_НЕДЕЛЯ\n"
            f"   → Скопируй DATE\n\n"

            f"8. ВСЕГДА используй ТОЛЬКО значение из колонки DATE в формате YYYY-MM-DD\n"
            f"9. НИКОГДА не вычисляй даты вручную\n"
            f"10. КАЖДАЯ СТРОКА ТАБЛИЦЫ = ОДИН КАЛЕНДАРНЫЙ ДЕНЬ\n"
            f"11. НЕ ОБЪЕДИНЯЙ СТРОКИ И НЕ СОЗДАВАЙ НОВЫЕ ДАТЫ\n"
            f"═══════════════════════════════════════════════════════════\n"
            f"ПРИМЕРЫ:\n"
            f"═══════════════════════════════════════════════════════════\n"
            f"Кандидат: 'понедельник' → WEEKDAY=Понедельник, RELATIVE пусто → DATE\n"
            f"Кандидат: 'следующий понедельник' → WEEKDAY=Понедельник, RELATIVE=СЛЕДУЮЩАЯ_НЕДЕЛЯ → DATE\n"
            f"Кандидат: 'завтра' → RELATIVE=ЗАВТРА → DATE\n"
        )

        return calendar_context


    def _get_missing_fields_map(self, profile: dict) -> Dict[str, str]:
        """Централизованная логика определения того, каких данных не хватает в профиле."""
        missing_data_map = {}

        # Обязательные поля
        if not profile.get("age"):
            missing_data_map["age"] = "Возраст (целое число лет)"
        if not profile.get("citizenship"):
            missing_data_map["citizenship"] = "Гражданство (название страны)"
        if not profile.get("employment_type"):
            missing_data_map["employment_type"] = "Тип занятости (full — полная, part — частичная)"
        if not profile.get("employment_contract_ready"):
            missing_data_map["employment_contract_ready"] = "Готовность к оформлению по ТК РФ (yes/no)"

        # Условные поля
        employment_type = str(profile.get("employment_type", "")).strip().lower()
        if employment_type == "part" and not profile.get("ready_20_40_hours"):
            missing_data_map["ready_20_40_hours"] = "Готовность работать 20-40 часов в неделю (yes/no)"
        if employment_type == "full" and not profile.get("shift_preference"):
            missing_data_map["shift_preference"] = "Предпочтение по смене (morning/evening/any)"

        # Военный билет — только для мужчин
        gender = str(profile.get("gender", "")).strip().lower()
        if gender == "male" and not profile.get("has_military_document"):
            missing_data_map["has_military_document"] = "Наличие военного билета или приписного (yes/no)"

        return missing_data_map


    async def _assemble_dynamic_prompt(self, prompt_library: dict, dialogue_state: str, user_message: str, vacancy_description: str, dialogue: Dialogue = None, sheet_name: str = "Calendar") -> str:
        """Сборка системного промпта из блоков библиотеки"""
        required_blocks = ['#ROLE_AND_STYLE#']

        state_map = {
            'initial': ['#QUALIFICATION_RULES#', '#FAQ#'],
            'awaiting_questions': ['#QUALIFICATION_RULES#', '#FAQ#', '#DECLINED_VAC#'],
            'awaiting_phone': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],
            'awaiting_citizenship': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],
            'awaiting_age': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],

            'awaiting_employment_type': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],
            'awaiting_ready_20_40_hours': ['#PART_TIME_RULES#', '#DECLINED_VAC#'],
            'awaiting_shift_preference': ['#SHIFT_RULES#', '#DECLINED_VAC#'],
            'awaiting_employment_contract': ['#FINAL_ASK#', '#DECLINED_VAC#'],
            'awaiting_military_document': ['#MILITARY_RULES#', '#DECLINED_VAC#'],

            'qualification_complete': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],

            'init_scheduling_spb': ['#SCHEDULING_ALGORITHM#'],
            'scheduling_spb_day': ['#SCHEDULING_ALGORITHM#'],
            'scheduling_spb_time': ['#SCHEDULING_ALGORITHM#'],
            'interview_scheduled_spb': ['#SCHEDULING_ALGORITHM#', '#FAQ#'],

            'call_later': ['#QUALIFICATION_RULES#', '#FAQ#'],
            'clarifying_declined_vacancy': ['#QUALIFICATION_RULES#'],

            'post_qualification_chat': ['#POSTCVAL#', '#FAQ#']
        }

        # [ДИНАМИЧЕСКИЙ ПРОМПТ] Для clarifying_anything собираем только нужные блоки правил
        if dialogue_state == 'clarifying_anything':
            profile = dialogue.candidate.profile_data or {} if dialogue and dialogue.candidate else {}
            missing = self._get_missing_fields_map(profile)
            
            blocks = ['#DECLINED_VAC#']
            if any(k in missing for k in ['age', 'citizenship', 'employment_type']):
                blocks.append('#QUALIFICATION_RULES#')
            if 'employment_contract_ready' in missing:
                blocks.append('#FINAL_ASK#')
            if 'ready_20_40_hours' in missing:
                blocks.append('#PART_TIME_RULES#')
            if 'shift_preference' in missing:
                blocks.append('#SHIFT_RULES#')
            if 'has_military_document' in missing:
                blocks.append('#MILITARY_RULES#')
            
            # Если вообще ничего не нашли (редкий случай), даем базовые правила
            if len(blocks) == 1:
                blocks.append('#QUALIFICATION_RULES#')
                
            required_blocks.extend(blocks)
        else:
            required_blocks.extend(state_map.get(dialogue_state, ['#QUALIFICATION_RULES#']))

        # Убираем дубли и собираем текст
        final_keys = list(dict.fromkeys(required_blocks))
        prompt_pieces = [prompt_library.get(key, '') for key in final_keys]

        # Заменяем плейсхолдеры в промптах на данные из job_contexts
        vacancy_title = ""
        vacancy_full_address = ""
        if dialogue and dialogue.vacancy:
            vacancy_title = dialogue.vacancy.title or ""
            if dialogue.vacancy.description_data:
                vacancy_full_address = dialogue.vacancy.description_data.get("full_address", "")

        # Замена плейсхолдеров во всех кусках промпта
        if vacancy_title or vacancy_full_address:
            # Магазин это то что написано в названии вакансии в скобках ()
            shop_name = ""
            if "(" in vacancy_title and ")" in vacancy_title:
                shop_name = vacancy_title.split("(")[-1].split(")")[0].strip()
            
            display_address = vacancy_full_address
            if shop_name:
                display_address = f"{vacancy_full_address} ({shop_name})" if vacancy_full_address else shop_name

            prompt_pieces = [
                piece.replace("[название вакансии]", vacancy_title)
                     .replace("[адрес вакансии]", display_address)
                for piece in prompt_pieces
            ]
        
        # Определяем состояния, для которых нужен календарь
        SCHEDULING_STATES = ['init_scheduling_spb', 'scheduling_spb_day', 'scheduling_spb_time', 'post_qualification_chat', 'interview_scheduled_spb']

        # Если текущее состояние требует календаря, генерируем и добавляем его
        if dialogue_state in SCHEDULING_STATES:
            # 1. Добавляем "Человеческий" список слотов (твоя просьба)
            human_slots = await self._get_human_slots_block(sheet_name)
            prompt_pieces.append(human_slots)

            # 2. Добавляем Динамический календарь (технический блок для выбора дат)
            all_slots = await sheets_service.get_all_slots_map(sheet_name)
            calendar_block = self._generate_calendar_context_2(all_slots)
            prompt_pieces.append(calendar_block)

        # Вставляем контекст вакансии
        vacancy_context = f"\n[ОПИСАНИЕ ВАКАНСИИ]\n{vacancy_description}"
        prompt_pieces.insert(1, vacancy_context)
        
        return "\n\n".join(prompt_pieces)

    async def _schedule_interview_reminders(self, db: AsyncSession, dialogue: Dialogue, date_str: str, time_str: str):
        """
        Универсально создает любое количество напоминаний из конфига.
        """
        

        if not settings.reminders.interview.enabled:
            return

        try:
            # 1. Время собеседования в МСК
            naive_dt = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            interview_dt_msk = naive_dt.replace(tzinfo=MOSCOW_TZ)
            now_msk = datetime.datetime.now(MOSCOW_TZ)
            
            # 2. Очищаем старые напоминания
            await db.execute(delete(InterviewReminder).where(
                InterviewReminder.dialogue_id == dialogue.id, 
                InterviewReminder.status == 'pending'
            ))
            
            # 3. Итерируемся по списку из конфига
            for cfg in settings.reminders.interview.items:
                scheduled_at = None

                if cfg.type == "fixed_time" and cfg.at_time:
                    # Логика "За X дней в HH:MM"
                    try:
                        target_hour, target_minute = map(int, cfg.at_time.split(':'))
                        target_day = interview_dt_msk - datetime.timedelta(days=cfg.days_before)
                        scheduled_at = target_day.replace(
                            hour=target_hour, 
                            minute=target_minute, 
                            second=0, 
                            microsecond=0
                        )
                    except Exception as e:
                        logger.error(f"Ошибка парсинга fixed_time {cfg.at_time}: {e}")

                elif cfg.type == "relative" and cfg.minutes_before is not None:
                    # Логика "За X минут до"
                    scheduled_at = interview_dt_msk - datetime.timedelta(minutes=cfg.minutes_before)

                # 4. Если время рассчитано и оно в будущем — сохраняем
                if scheduled_at and scheduled_at > now_msk:
                    db.add(InterviewReminder(
                        dialogue_id=dialogue.id,
                        reminder_type=cfg.id, # Используем ID из конфига как тип
                        scheduled_at=scheduled_at.astimezone(datetime.timezone.utc)
                    ))
                    logger.debug(f"Запланировано напоминание '{cfg.id}' на {scheduled_at}")

            await db.flush()



        except Exception as e:
            error_msg = f"⚠️ Ошибка планирования напоминаний для диалога {dialogue.id}: {e}"
            logger.error(error_msg)
            await mq.publish("tg_alerts", {
                "type": "system",
                "text": error_msg,
                "alert_type": "admin_only"
            })

    async def _create_talantix_meeting(
        self,
        dialogue: Dialogue,
        interview_date: str,
        interview_time: str,
        talantix_data: dict,
        ctx_logger: logging.LoggerAdapter,
    ) -> int | None:
        """
        Создаёт встречу в календаре Talantix.

        Args:
            dialogue: объект диалога
            interview_date: дата в формате YYYY-MM-DD
            interview_time: время в формате HH:MM
            talantix_data: dict из metadata_json.talantix с полями:
                - person_id: ID кандидата в Talantix
                - vacancy_id: ID вакансии в Talantix
                - managers: список менеджеров с manager_id

        Returns:
            int | None: ID созданной встречи (meeting_id) или None при ошибке
        """
        if not settings.services.talantix.enabled:
            ctx_logger.debug("Talantix integration is disabled in config. Skipping _create_talantix_meeting.")
            return None

        # 1. Конвертируем дату/время в timestamp (MSK = UTC+3)
        moscow_tz = datetime.timezone(datetime.timedelta(hours=3))
        # Вместо moscow_tz используем UTC, чтобы не было сдвига при получении timestamp
        dt_naive = datetime.datetime.strptime(f"{interview_date} {interview_time}", "%Y-%m-%d %H:%M")
        # Мы принудительно ставим зону UTC, чтобы 14:00 превратилось в 14:00 UTC
        dt_utc = dt_naive.replace(tzinfo=datetime.timezone.utc)

        start_ts = int(dt_utc.timestamp() * 1000)
        end_ts = start_ts + 30 * 60 * 1000  # длительность 30 минут

        person_id = talantix_data.get("person_id")
        vacancy_id = talantix_data.get("vacancy_id")
        vacancy_title = talantix_data.get("vacancy_title", "")
        managers = talantix_data.get("managers", [])

        # Собираем manager_ids (создатель + участники)
        manager_ids = [m["manager_id"] for m in managers if m.get("manager_id")]

        # Если нет менеджеров — пробуем получить хотя бы одного ADMIN
        if not manager_ids:
            try:
                all_managers = await talantix_service.get_managers(roles=["ADMIN"])
                if all_managers:
                    manager_ids = [all_managers[0]["id"]]
                    ctx_logger.info(f"⚠️ Менеджеры не найдены в talantix_data, использую ADMIN: {manager_ids}")
            except Exception as e:
                ctx_logger.error(f"❌ Ошибка получения ADMIN менеджеров: {e}")

        # Формируем комментарий
        candidate_name = dialogue.candidate.full_name or "Кандидат"
        comment_html = f"<p>Собеседование: {candidate_name}</p>"
        if vacancy_title:
            comment_html += f"<p>Вакансия: {vacancy_title}</p>"

        ctx_logger.info(
            f"📅 Talantix: создаю встречу date={interview_date} time={interview_time}, "
            f"person_id={person_id}, vacancy_id={vacancy_id}, managers={manager_ids}"
        )

        success = await talantix_service.book_interview(
            start_date=start_ts,
            end_date=end_ts,
            person_ids=[person_id] if person_id else [],
            vacancy_ids=[vacancy_id] if vacancy_id else [],
            manager_ids=manager_ids,
            title="Собеседование",
            comment=comment_html,
            place="",
            timezone="Europe/Moscow",
            send_person_message=False,
            sync_with_external_calendar=False,
            person_message_file_ids=[],
        )

        if not success:
            ctx_logger.error("❌ Talantix: не удалось создать встречу")
            return None

        meeting_id = success  # book_interview теперь возвращает meeting_id
        ctx_logger.info(f"✅ Talantix: встреча создана, meeting_id={meeting_id}")
        return meeting_id


    async def process_engine_task(self, task_data: Dict[str, Any]):
        """
        Точка входа (аналог process_pending_dialogues из референса, но для одной задачи).
        """
        dialogue_id = task_data.get("dialogue_id")
        trigger = task_data.get("trigger")

        if not dialogue_id:
            logger.error(f"❌ Задача без dialogue_id: {task_data}")
            return

        # 1. Создаем контекст логгера (как в rec_log_context)
        log_context = {
            "dialogue_id": dialogue_id,
            "worker": "engine",
            "trigger": trigger
        }
        ctx_logger = logging.LoggerAdapter(logger, log_context)

        start_time = time.monotonic()
        
        # 2. Открываем сессию БД (каждая задача в своей сессии)
        async with AsyncSessionLocal() as db:
            try:
                await self._process_single_dialogue(dialogue_id, db, ctx_logger, task_data)
                trigger = task_data.get("trigger")
            except DialogueLockedError:
                # Просто пробрасываем в воркер, он сам залогирует WARNING
                raise 
            except Exception as e:
                ctx_logger.error(f"💥 Критическая ошибка обработки диалога: {e}", exc_info=True)
                # Тут можно добавить отправку алерта в Sentry/Telegram
                raise e
            finally:
                duration = time.monotonic() - start_time
                ctx_logger.info(f"🏁 Обработка завершена за {duration:.2f} сек.")


    def _count_real_messages(self, history: List[dict]) -> int:
        """Считает количество 'реальных' сообщений в истории (без системных команд)."""
        if not history:
            return 0
        count = 0
        for msg in history:
            content = msg.get('content', '')
            if not self._is_technical_message(content) and not str(content).startswith('[SYSTEM'):
                count += 1
        return count

    async def _process_single_dialogue(self, dialogue_id: int, db: AsyncSession, ctx_logger: logging.LoggerAdapter, task_data: Dict[str, Any]):
        """
        Адаптированная версия process_single_dialogue.
        Загружает контекст, блокирует диалог и готовит данные для обработки.
        """
        dialogue_processing_start_time = time.monotonic()

        # === 1. БЛОКИРОВКА (Опционально Redis, сейчас используем DB Lock) ===
        dialogue = None
        trigger = task_data.get("trigger") # Добавить эту строку
        # === 1. REDIS LOCK (Защита от Race Condition между воркерами) ===
        lock_key = f"dialogue_process_{dialogue_id}"
        # Таймаут 60 секунд (хватит на любой LLM запрос + логику)
        if not await acquire_lock(lock_key, timeout=60):
            ctx_logger.warning(f"⚠️ Диалог {dialogue_id} уже обрабатывается другим воркером. Пропуск.")
            raise DialogueLockedError("Dialogue is locked by another worker.")
        try:
            # Проверка активности сессии
            if not db.is_active:
                ctx_logger.error(f"Session is not active for dialogue {dialogue_id}")
                return

            db_fetch_start = time.monotonic()

            # === 2. ЗАГРУЗКА ДАННЫХ С БЛОКИРОВКОЙ (Row-Level Lock) ===
            # Используем selectinload для жадной загрузки связей
            stmt = (
                select(Dialogue)
                .filter_by(id=dialogue_id)
                .options(
                    selectinload(Dialogue.vacancy),     # JobContext
                    selectinload(Dialogue.candidate),   # Candidate
                    selectinload(Dialogue.account),     # Account (вместо Recruiter)
                    selectinload(Dialogue.reminders),   # InterviewReminder
                    selectinload(Dialogue.followups),   # InterviewFollowup
                    selectinload(Dialogue.vacancy).selectinload(JobContext.director)  # Director через vacancy
                )
                .with_for_update()      # Блокируем строку от других воркеров
            )
            
            result = await db.execute(stmt)
            dialogue = result.scalar_one_or_none()

            # Если диалог занят другим процессом или не найден
            if not dialogue:
                ctx_logger.debug(f"Dialogue {dialogue_id} is locked or not found. Skipping.")
                return

            # === 2.05 ПРОВЕРКА АКТУАЛЬНОСТИ НА СТАРТЕ (HH ONLY) ===
            hh_msg_count_start = 0
            trigger = task_data.get("trigger", "unknown")
            is_reminder = trigger in ["silence_reminder", "follow_up"]

            if dialogue.account.platform == 'hh' and dialogue.external_chat_id and not is_reminder:
                try:
                    from app.connectors.hh.client import hh
                    status_data = await hh.get_negotiation_status(dialogue.account, db, dialogue.external_chat_id)
                    if status_data and status_data.get("counters"):
                        hh_msg_count_start = status_data["counters"].get("messages", 0)
                        
                        # Счетчик из задачи (то, что видел сканер в момент формирования)
                        task_msg_count = task_data.get("initial_msg_count", 0)
                        
                        # Если в HH уже больше, чем было в задаче — значит, пока задача шла,
                        # сканер успел прислать еще одну, более свежую.
                        if hh_msg_count_start > task_msg_count:
                            ctx_logger.warning(
                                f"🛑 ПРЕРЫВАНИЕ (START): В HH {hh_msg_count_start} сообщений, а в задаче {task_msg_count}. "
                                f"Уже есть более свежая задача в очереди. Пропускаю."
                            )
                            return # Выходим без rollback
                        else:
                            ctx_logger.info(
                                f"✅ HH Check (START): В HH {hh_msg_count_start} сообщений, в задаче {task_msg_count}. "
                                f"Продолжаю обработку."
                            )
                except Exception as e:
                    ctx_logger.error(f"⚠️ Ошибка проверки актуальности HH на старте: {e}")
            elif is_reminder:
                ctx_logger.info(f"🔔 Режим напоминания/дожима ({trigger}). Пропускаю проверку START.")

            account = dialogue.account
            if not account:
                ctx_logger.error(f"Account for dialogue {dialogue_id} not found")
                return

            # === 2.1 ОПРЕДЕЛЕНИЕ ДИРЕКТОРА (для Google Sheets и TG) ===
            director = None
            sheet_name = "Calendar"  # Default fallback
            
            if dialogue.vacancy and dialogue.vacancy.director:
                director = dialogue.vacancy.director
                sheet_name = director.google_sheet_name
            else:
                # Fallback: ищем директора по account_id
                director_stmt = select(Director).where(
                    Director.account_id == account.id,
                    Director.is_active == True
                ).limit(1)
                director_result = await db.execute(director_stmt)
                director = director_result.scalar_one_or_none()
                if director:
                    sheet_name = director.google_sheet_name

            ctx_logger.extra.update({
                "director_id": director.id if director else None,
                "director_name": director.name if director else "Unknown",
                "sheet_name": sheet_name
            })

            # === 3. ОБНОВЛЕНИЕ КОНТЕКСТА ЛОГГЕРА ===
            # Теперь логгер знает все детали, как в референсе
            ctx_logger.extra.update({
                "external_chat_id": dialogue.external_chat_id,  # Аналог hh_response_id
                "account_name": account.name,                   # Аналог recruiter_name
                "vacancy_id": dialogue.vacancy_id,
                "vacancy_title": dialogue.vacancy.title if dialogue.vacancy else "Unknown",
                "candidate_id": dialogue.candidate_id,
                "state": dialogue.current_state,
                "candidate_data": dialogue.candidate.profile_data if dialogue.candidate else {}
            })


            ctx_logger.debug(
                f"Processing dialogue {dialogue.external_chat_id}...",
                extra={"action": "start_processing", "fetch_time": time.monotonic() - db_fetch_start}
            )
            # === БЛОК КОНТРОЛЯ СТОИМОСТИ (STOP-CRANE) ===
            try:
                usage_stats = dialogue.usage_stats or {}
                total_cost_usd = usage_stats.get("total_cost", 0)
                total_cost_rub = float(total_cost_usd) * self.RUB_RATE

                # 1. СТОП-КРАН (Блокировка при критическом расходе)
                if total_cost_rub > self.COST_LIMIT_BLOCK:
                    ctx_logger.critical(f"🛑 КРИТИЧЕСКИЙ РАСХОД: {total_cost_rub:.2f} руб. Блокирую диалог!")
                    
                    # Меняем статус, чтобы воркер больше не трогал этот диалог
                    dialogue.status = 'closed_by_cost' 
                    # Опционально: можно добавить спец. метку в метаданные
                    meta = dict(dialogue.metadata_json or {})
                    meta["block_reason"] = "cost_limit_exceeded"
                    dialogue.metadata_json = meta
                    
                    await db.commit()
                    
                    # Отправляем экстренное уведомление в ТГ
                    await mq.publish("tg_alerts", {
                        "type": "system",
                        "alert_type": "admin_only",
                        "text": f"🚨 **STOP-CRANE ACTIVATED**\nДиалог: `{dialogue.id}`\nЧат: `{dialogue.external_chat_id}`\nРасход: `{total_cost_rub:.2f} руб`\n*Обработка остановлена автоматически.*"
                    })
                    return # ПРЕРЫВАЕМ выполнение метода

                # 2. АЛЕРТ (Уведомление при превышении порога 8 руб)
                if total_cost_rub > self.COST_LIMIT_ALERT:
                    alert_key = f"cost_alert_sent:{dialogue.id}"
                    # Используем Redis Lock как флаг однократной отправки на 3 дня
                    if await acquire_lock(alert_key, timeout=259200):
                        ctx_logger.warning(f"💸 Высокая стоимость диалога: {total_cost_rub:.2f} руб. Шлю алерт.")
                        
                        await mq.publish("tg_alerts", {
                            "type": "system",
                            "text": f"💰 **ВНИМАНИЕ: ДОРОГОЙ ДИАЛОГ**\nID: `{dialogue.id}`\nАккаунт: `{account.name}`\nСтоимость: `{total_cost_rub:.2f} руб`\nНужна проверка на зацикливание бота.",
                            "alert_type": "admin_only"
                        })

            except Exception as cost_err:
                ctx_logger.error(f"Ошибка в блоке контроля стоимости: {cost_err}")
            # === КОНЕЦ БЛОКА КОНТРОЛЯ СТОИМОСТИ ===
            # === СТАТИСТИКА: ЛОГИКА ВОСКРЕШЕНИЯ ===
            # Если кандидат был "молчуном", но написал нам (триггер не от шедулера)
            if dialogue.status == 'timed_out' and trigger not in ["reminder", "system_audit_retry", "data_fix_retry"]:
                ctx_logger.info("🧟 Кандидат воскрес! Удаляем событие timed_out из статистики.")
                await db.execute(
                    delete(AnalyticsEvent)
                    .where(AnalyticsEvent.dialogue_id == dialogue.id)
                    .where(AnalyticsEvent.event_type == 'timed_out')
                )

            # === СБРОС ТАЙМАУТА И УРОВНЯ НАПОМИНАНИЙ ===
            # Если пришло любое сообщение от пользователя (не системный триггер)
            if trigger not in ["reminder", "system_audit_retry", "data_fix_retry"]:
                if dialogue.status == 'timed_out':
                    ctx_logger.info("🔄 Кандидат вернулся! Снимаем статус timed_out.")
                    dialogue.status = 'in_progress'
                
                # Всегда сбрасываем уровень напоминаний на 0 при активности юзера
                if dialogue.reminder_level > 0:
                    ctx_logger.info(f"♻️ Сброс уровня напоминаний с {dialogue.reminder_level} на 0")
                    dialogue.reminder_level = 0


            # Загружаем Account (аналог Recruiter из HH бота)
            # В нашей модели Account уже привязан к диалогу, он подгрузился выше через selectinload
            

            # === 3. ОБНОВЛЕНИЕ КОНТЕКСТА ЛОГГЕРА ===
            # Теперь логгер знает все детали, как в референсе
            ctx_logger.extra.update({
                "external_chat_id": dialogue.external_chat_id,  # Аналог hh_response_id
                "account_name": account.name,                   # Аналог recruiter_name
                "vacancy_id": dialogue.vacancy_id,
                "vacancy_title": dialogue.vacancy.title if dialogue.vacancy else "Unknown",
                "candidate_id": dialogue.candidate_id,
                "state": dialogue.current_state,
                "candidate_data": dialogue.candidate.profile_data if dialogue.candidate else {}
            })


            ctx_logger.debug(
                f"Processing dialogue {dialogue.external_chat_id}...",
                extra={"action": "start_processing", "fetch_time": time.monotonic() - db_fetch_start}
            )

            # === 4. ПРОВЕРКА НА ТРИГГЕР НАПОМИНАНИЯ (Short Circuit) ===
            # Если задача пришла от Scheduler, отправляем статический текст без LLM
            # === 4. ПРОВЕРКА НА ТРИГГЕР НАПОМИНАНИЯ (Short Circuit) ===
            if trigger == "reminder":
                reminder_text = task_data.get("reminder_text")
                stop_bot = task_data.get("stop_bot", False)

                if reminder_text:
                    ctx_logger.info(f"📤 Отправка статического напоминания: {reminder_text[:30]}...")
                    
                    try:
                        # 1. Получаем универсальный коннектор
                        connector = get_connector(dialogue.account.platform)
                        
                        # Отправляем и СОХРАНЯЕМ ответ
                        send_result = await connector.send_message(
                            account=dialogue.account,
                            db=db,
                            chat_id=dialogue.external_chat_id,
                            text=reminder_text
                        )
                        # Вытаскиваем реальный ID от Авито
                        real_msg_id = send_result.get("id") if isinstance(send_result, dict) else None
                        
                        ctx_logger.info(f"✅ Напоминание успешно отправлено. ID: {real_msg_id}")

                    except Exception as e:
                        # 3. Обработка критических/терминальных ошибок (403/404)
                        error_str = str(e).lower()
                        if any(err in error_str for err in ["403", "404", "forbidden", "not found"]):
                            ctx_logger.warning(f"🚫 API запретил отправку напоминания. Закрываем диалог. Error: {e}")
                            dialogue.status = 'closed'
                            await db.commit()
                            return # Сообщение удалится из очереди (ACK), так как мы "обработали" ситуацию
                        
                        # 4. Временные ошибки (сеть, 500-е) — пробрасываем для ретрая
                        ctx_logger.error(f"❌ Сбой при отправке напоминания: {e}")
                        # Делаем rollback, чтобы не сохранять промежуточные изменения (если были)
                        await db.rollback()
                        # Бросаем ошибку, чтобы воркер сделал NACK и requeue=True
                        raise e

                    # --- СОХРАНЕНИЕ В ИСТОРИЮ (только после успешной отправки) ---
                    reminder_msg = {
                        # Если Авито вернул ID - берем его. Если нет - генерируем временный (fallback)
                        'message_id': str(real_msg_id) if real_msg_id else f'rem_{time.time()}',
                        'role': 'assistant',
                        'content': reminder_text,
                        'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        'state': dialogue.current_state,
                        'is_reminder': True
                    }
                    dialogue.history = (dialogue.history or []) + [reminder_msg]
                    dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
                    dialogue.reminder_level = task_data.get("new_level", dialogue.reminder_level)

                    if stop_bot:
                        dialogue.status = 'closed'
                        ctx_logger.info("🔇 Диалог переведен в статус CLOSED согласно конфигу напоминания.")

                    # Комментарий в Talantix о напоминании
                    if settings.services.talantix.enabled:
                        try:
                            await talantix_crm_service.notify_talantix_comment(
                                dialogue=dialogue,
                                event_type='silence',
                                db=db
                            )
                        except Exception as e:
                            ctx_logger.error(f"Ошибка создания комментария Talantix (silence): {e}")

                    await db.commit()
                    return # Успешный выход
                
            # === 4. ПОДГОТОВКА PENDING MESSAGES (Адаптация) ===
            
            # Нам нужно найти те сообщения пользователя с конца списка, на которые мы еще не ответили.
            
            history = dialogue.history or []
            pending_messages = []
            
            # Идем с конца истории и собираем сообщения пользователя, пока не наткнемся на бота
            for msg in reversed(history):
                if msg.get('role') == 'user':
                    # Вставляем в начало списка pending, чтобы сохранить хронологию
                    pending_messages.insert(0, msg)
                else:
                    # Как только встретили сообщение бота (assistant) — значит, всё до этого уже обработано
                    break
            
            # Если нет новых сообщений от пользователя И диалог не в спец. статусе (например, мы сами себя триггернули)
            # То можно выходить. Но пока оставим логику как есть.
            if not pending_messages:
                # В референсе был возврат, но у нас могут быть триггеры от таймера или системные команды
                # Пока просто логируем
                ctx_logger.debug(f"No new user messages found in history tail.")
                # return # Пока не делаем return, вдруг это триггер таймера
            
            
            # === 6. PII MASKING & PREPARATION ===
            # Мы НЕ добавляем сообщения в историю (они уже там), 
            # но нам нужно:
            # 1. Извлечь телефоны/ФИО для БД
            # 2. Подготовить замаскированный текст для LLM
            
            all_masked_content = []
            
            for pm in pending_messages:
                # pm - это реальный объект из dialogue.history (dict)
                original_content = pm.get('content', '')
                
                # Маскируем и пытаемся вытащить телефон/ФИО регулярками
                masked_content, extracted_fio, extracted_phone = extract_and_mask_pii(original_content)

                # --- ИЗВЛЕЧЕНИЕ ДАННЫХ РЕГУЛЯРКАМИ (Pre-LLM) ---
                if extracted_phone:
                    dialogue.candidate.phone_number = extracted_phone
                    ctx_logger.info(f"📞 Извлечен телефон из текста: {extracted_phone}")

                # if extracted_fio:
                #     # Записываем ФИО, если оно найдено регуляркой
                #     dialogue.candidate.full_name = extracted_fio
                #     ctx_logger.info(f"👤 Извлечено ФИО из текста: {extracted_fio}")

                # Собираем текст для отправки в LLM
                all_masked_content.append(masked_content)

            combined_masked_message = "\n".join(all_masked_content)
            # === СТАТИСТИКА: ПЕРВЫЙ КОНТАКТ ===
            meta = dict(dialogue.metadata_json or {})
            if not meta.get("first_contact_registered"):
                # Проверяем, есть ли в истории хоть одно НЕ системное сообщение от юзера
                has_real_user_msg = any(
                    m.get('role') == 'user' and 
                    not str(m.get('content', '')).startswith('[SYSTEM') and 
                    not str(m.get('content', '')).startswith('[Системное сообщение]')
                    for m in (dialogue.history or [])
                )

                if has_real_user_msg:
                    ctx_logger.info("🗣 Зафиксирован ПЕРВЫЙ РЕАЛЬНЫЙ контакт (ответ кандидата).")
                    await log_event(db, dialogue, 'first_contact')
                    meta["first_contact_registered"] = True
                    dialogue.metadata_json = meta
            # Получаем библиотеку промптов из базы знаний
            prompt_library = await kb_service.get_library()
            # === 7. СБОРКА ПРОМПТА ===
            # Ищем описание вакансии в базе знаний (или берем из БД)
            vacancy_title = dialogue.vacancy.title if dialogue.vacancy else "Вакансия"
            vacancy_city = dialogue.vacancy.city if dialogue.vacancy else "Город не указан"
            
            # Тут можно использовать _find_relevant_vacancy, если описания нет в БД,
            # но в нашей архитектуре описание лежит в JobContext.description_data
            relevant_vacancy_desc = "Описание не найдено"
            if dialogue.vacancy and dialogue.vacancy.description_data:
                relevant_vacancy_desc = dialogue.vacancy.description_data.get("description_text", "")

            # Собираем системный промпт из блоков (#ROLE#, #FAQ# и т.д.)
            # Получаем sheet_name из контекста (уже определен выше)
            current_sheet_name = sheet_name
            
            system_prompt = await self._assemble_dynamic_prompt(
                prompt_library,
                dialogue.current_state,
                combined_masked_message.lower(),
                relevant_vacancy_desc,
                dialogue,  # Передаем объект dialogue для замены плейсхолдеров
                current_sheet_name  # Передаем sheet_name
            )

            # Добавляем контекст задачи в конец промпта
            context_postfix = (
                f"\n\n[CURRENT TASK] Ты общаешься с кандидатом по вакансии '{vacancy_title}' "
                f"в городе '{vacancy_city}'. Текущее состояние: '{dialogue.current_state}'."
            )
            final_system_prompt = system_prompt + context_postfix

            # === 8. ВЫЗОВ LLM (MAIN CALL) ===
            llm_call_start = time.monotonic()
            llm_data = None
            attempt_tracker = [] # Ловушка для попыток (tenacity)

            try:
                raw_history = dialogue.history or []
                
                # 1. Фильтруем технический мусор
                filtered_history = [
                    msg for msg in raw_history 
                    if not self._is_technical_message(msg.get('content', '')) 
                ]

                # 2. ДИНАМИЧЕСКАЯ МАСКИРОВКА ВСЕЙ ИСТОРИИ (НОВОЕ!)
                # Мы проходим по истории и маскируем PII в каждом сообщении перед отправкой
                history_for_llm = []
                for msg in filtered_history[-25:]:
                    # Делаем копию сообщения, чтобы не изменить данные в объекте диалога (в БД)
                    msg_masked = dict(msg)
                    
                    # Маскируем контент. ФИО и телефоны из истории нам уже не нужны для записи в БД,
                    # так как они были извлечены ранее, поэтому берем только первый результат.
                    masked_text, _, _ = extract_and_mask_pii(msg_masked.get('content', ''))
                    
                    msg_masked['content'] = masked_text
                    history_for_llm.append(msg_masked)
                
                # 3. САМ ВЫЗОВ
                llm_data = await get_bot_response(
                    system_prompt=final_system_prompt,
                    dialogue_history=history_for_llm, # Теперь тут всё в [ЗАМАСКИРОВАНО]
                    user_message=combined_masked_message, # Оно уже замаскировано в шаге 6
                    attempt_tracker=attempt_tracker,
                    extra_context=ctx_logger.extra 
                )

                # --- ЛОГИКА СКРЫТЫХ РЕТРАЕВ (Tenacity) ---
                # Если tenacity делала ретраи внутри, мы должны учесть их стоимость
                total_attempts = len(attempt_tracker)
                failed_attempts = total_attempts - 1 # Все кроме последней (успешной)

                if failed_attempts > 0:
                     ctx_logger.warning(
                        f"LLM Retries detected: {failed_attempts}",
                        extra={"retry_count": failed_attempts}
                    )
                     # Логируем стоимость скрытых ретраев
                     for i in range(failed_attempts):
                         
                         await self._log_llm_usage(db, dialogue, f"{dialogue.current_state} (RETRY #{i+1})")

            except Exception as llm_error:
                # --- СЦЕНАРИЙ ПОЛНОГО ПРОВАЛА ---
                # Если упало здесь, значит tenacity исчерпал все попытки.
                # Мы должны записать расходы на ВСЕ попытки перед падением.
                
                ctx_logger.error(
                    f"❌ LLM Request FAILED completely after {len(attempt_tracker)} attempts: {llm_error}", 
                    exc_info=True,
                    extra={"action": "llm_request_failed_total"}
                )
                
                try:
                    for i in range(len(attempt_tracker)):
                        await self._log_llm_usage(
                            db, dialogue, 
                            f"{dialogue.current_state} (FAILED #{i+1}: {type(llm_error).__name__})"
                        )
                except Exception as log_ex:
                    ctx_logger.error(f"Failed to log LLM errors to DB: {log_ex}")

                raise llm_error # Пробрасываем ошибку дальше, чтобы сработал rollback

            llm_duration = time.monotonic() - llm_call_start
            ctx_logger.debug(
                f"LLM response received in {llm_duration:.2f}s",
                extra={"llm_duration": llm_duration}
            )

            # Проверка на пустоту (System Alert)
            if llm_data is None:
                
                
                
                raise ValueError("LLM returned None")

            # Распаковка ответа
            llm_response = llm_data.get("parsed_response", {})
            usage_stats = llm_data.get("usage_stats", {})

            # === 9. ЛОГИРОВАНИЕ ТОКЕНОВ (УСПЕШНОЕ) ===
            if usage_stats:
                try:
                    await self._log_llm_usage(db, dialogue, dialogue.current_state, usage_stats)
                except Exception as e:
                    ctx_logger.error(f"Error logging tokens for dialogue {dialogue.id}: {e}")

            # === 10. РАЗБОР ОТВЕТА ===
            bot_response_text = llm_response.get("response_text")
            new_state = llm_response.get("new_state", "error_state")
            extracted_data = llm_response.get("extracted_data", {})

            ctx_logger.info(f"LLM Decision: State '{dialogue.current_state}' -> '{new_state}'")


            # === 11. ВАЛИДАЦИЯ СТАТУСА ===
            ALLOWED_STATES = {
                'initial',
                'awaiting_questions',
                'awaiting_phone',
                'awaiting_citizenship',
                'awaiting_age',
                'awaiting_employment_type',
                'awaiting_ready_20_40_hours',
                'awaiting_shift_preference',
                'awaiting_employment_contract',
                'awaiting_military_document',
                'clarifying_anything',
                'clarifying_declined_vacancy',
                'qualification_complete',
                
                'forwarded_to_researcher',

                'init_scheduling_spb',
                'scheduling_spb_day',
                'scheduling_spb_time',
                'interview_scheduled_spb',

                'post_qualification_chat',
                'declined_vacancy',
                'declined_interview',
                'call_later'
            }


            

           
            

            if new_state not in ALLOWED_STATES:
                ctx_logger.error(
                    f"CRITICAL: LLM вернула недопустимый стейт: '{new_state}'",
                    extra={"action": "invalid_state_detected", "invalid_state": new_state}
                )
                
                # 1. Формируем текст замечания для модели
                hallucination_corr_cmd = {
                    'message_id': f'sys_state_hallucination_{time.time()}',
                    'role': 'user',
                    'content': (
                        f"[SYSTEM COMMAND] В твоем последнем ответе произошла техническая ошибка: "
                        f"ты вернул недопустимое состояние (new_state) '{new_state}'. "
                        f"Такого состояния НЕ СУЩЕСТВУЕТ в твоей инструкции. "
                        f"Проанализируй диалог и инструкции заново и выбери корректное состояние"
                    ),
                    'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()
                }

                # 2. Очищаем историю от старых подобных команд (чтобы не дублировать их бесконечно)
                clean_history = [
                    m for m in (dialogue.history or [])
                    if not str(m.get('message_id', '')).startswith('sys_state_hallucination')
                ]

                # 3. Сохраняем историю и добавляем системную команду в конец
                dialogue.history = clean_history + [hallucination_corr_cmd]
                dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)

                # 3. Фиксируем изменения в базе
                await db.commit()
                
                # 4. Отправляем задачу на переобработку с новой системной командой
                await mq.publish("engine_tasks", {
                    "dialogue_id": dialogue.id, 
                    "trigger": "state_correction_retry",
                    "initial_msg_count": hh_msg_count_start
                })
                
                ctx_logger.info(f"Отправлено на исправление галлюцинации стейта: {new_state}")
                return # Обязательно выходим, чтобы текущая обработка прекратилась
            # --- [END] ВАЛИДАЦИЯ СТАТУСА ---


            # === 12. ВАЛИДАЦИЯ ДАТЫ И ВРЕМЕНИ (АУДИТ + РЕГЛАМЕНТ + СЛОТЫ) ===
            DATE_CRITICAL_STATES = ['init_scheduling_spb', 'scheduling_spb_day', 'scheduling_spb_time', 'interview_scheduled_spb']
            
            

            # Список ключевых слов (как в HH)
            TIME_KEYWORDS = [
                "сегодня", "завтра", "послезавтра", "понедельник", "вторник", "сред", "четверг", 
                "пятниц", "суббот", "воскресен", "январ", "феврал", "март", "апрел", "май", "июн", 
                "июл", "август", "сентябр", "октябр", "ноябр", "декабр", "число", "время", "числа", "числ", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
            ]

            if new_state in DATE_CRITICAL_STATES:
                interview_date = extracted_data.get("interview_date")
                interview_time = extracted_data.get("interview_time")
                # Проверяем наличие маркеров времени
                bot_text_low = (bot_response_text or "").lower()
                user_text_low = combined_masked_message.lower()
                has_time_keywords = any(kw in bot_text_low or kw in user_text_low for kw in TIME_KEYWORDS)

                # Входим, если есть дата в JSON или обсуждение времени в тексте
                if interview_date or has_time_keywords:
                    ctx_logger.info("Есть дата или маркеры")

                    # --- [НОВОЕ] ПРОВЕРКА: ВЫПОЛНИЛ ЛИ БОТ ПРЕДЫДУЩУЮ КОМАНДУ? ---
                    is_obeyed = False
                    last_msg = dialogue.history[-1] if dialogue.history else {}
                    last_content = str(last_msg.get("content", ""))

                    if last_msg.get("role") == "user" and "[SYSTEM COMMAND]" in last_content:
                        import re
                        # Ищем в прошлой команде указание установить дату или null
                        target_date_match = re.search(r"interview_date' в JSON на '(\d{4}-\d{2}-\d{2})'", last_content)
                        target_null = "interview_date' в JSON на null" in last_content

                        if target_date_match and interview_date == target_date_match.group(1):
                            is_obeyed = True
                            ctx_logger.info(f"✅ Бот выполнил инструкцию по дате: {interview_date}")
                        elif target_null and (interview_date is None or interview_date == ""):
                            is_obeyed = True
                            ctx_logger.info("✅ Бот выполнил инструкцию: установил interview_date в null")

                    if not is_obeyed:
                        # --- 12.1 ЕДИНЫЙ ЦЕНТР ВАЛИДАЦИИ ДАТЫ И ДОСТУПНОСТИ СЛОТОВ ---
                        # Мы объединяем Аудитора (понимание намерений) и Гугл Таблицу (реальность)

                        # 1. Узнаем, какую дату реально ХОЧЕТ пользователь (через Умного Аудитора)
                        full_hist = (dialogue.history or [])
                        # Убираем системные команды для Аудитора, чтобы он не запутался в прошлых исправлениях
                        clean_hist_for_audit = [m for m in full_hist if not str(m.get("content", "")).startswith("[SYSTEM")]
                        
                        # Оптимизация: запускаем Аудитора только если дата новая или есть маркеры времени
                        stored_meta = dialogue.metadata_json or {}
                        stored_date = stored_meta.get("interview_date")
                        
                        run_audit = True
                        verified_date = "none"
                        audit_reason = "Ожидание аудита"

                        if stored_date == interview_date and not has_time_keywords:
                            ctx_logger.debug("Дата совпадает с сохраненной и нет новых триггеров. Пропуск аудита.")
                            run_audit = False
                            verified_date = interview_date
                            audit_reason = "Совпадение с метаданными"
                        
                        if run_audit:
                            ctx_logger.info(f"🔍 Запуск аудита даты: {interview_date}")
                            all_slots_map = await sheets_service.get_all_slots_map(sheet_name)
                            calendar_ctx = self._generate_calendar_context_2(all_slots_map)
                            
                            verified_date, audit_reason = await self._verify_date_audit(
                                db, dialogue, interview_date, clean_hist_for_audit, calendar_ctx, ctx_logger.extra
                            )
                            ctx_logger.info(f"Auditor result: {verified_date} | Reason: {audit_reason}")

                        if verified_date and verified_date != "none":
                            # 2. Проверяем РЕАЛЬНУЮ доступность этой даты в Google Sheets
                            try:
                                now_msk = datetime.datetime.now(MOSCOW_TZ)
                                today_str = now_msk.strftime('%Y-%m-%d')
                                
                                available_slots = await sheets_service.get_available_slots(verified_date, sheet_name)
                                
                                # Фильтр "Сегодня": только слоты, на которые человек успеет доехать (+1 час запаса)
                                if verified_date == today_str:
                                    available_slots = [s for s in available_slots if int(s.split(':')[0]) > now_msk.hour]

                                # Определяем день недели для текста системных команд
                                try:
                                    v_date_obj = datetime.datetime.strptime(verified_date, '%Y-%m-%d')
                                    weekdays_ru = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
                                    v_weekday = weekdays_ru[v_date_obj.weekday()]
                                except:
                                    v_weekday = "указанный день"

                                # 3. ПРИНЯТИЕ РЕШЕНИЯ (Анализ расхождений)
                                is_day_full = not available_slots
                                is_hallucination = (verified_date != interview_date)
                                
                                # Проверяем, не выполнил ли бот задачу самостоятельно с первого раза
                                is_already_correct = False
                                if is_day_full and (interview_date is None or interview_date == ""):
                                    is_already_correct = True
                                elif not is_day_full and interview_date == verified_date:
                                    is_already_correct = True
                                
                                if is_already_correct:
                                    ctx_logger.info(f"✅ Бот самостоятельно верно определил дату и доступность ({verified_date})")
                                    interview_date = verified_date # Синхронизируем для дальнейшей логики
                                else:
                                    hint_content = None
                                    if is_day_full:
                                        # Сценарий А: Мест нет. Требуем отказать и занулить JSON.
                                        hint_content = (
                                            f"[SYSTEM COMMAND] Внимание!!! На {verified_date} ({v_weekday}) нет свободных мест в графике. "
                                            f"Ты ОБЯЗАНА сообщить об этом кандидату и предложить выбрать любой другой свободный день из календаря. "
                                            f"ОБЯЗАТЕЛЬНО установи 'interview_date' в JSON на null."
                                        )
                                    elif is_hallucination:
                                        # Сценарий Б: Ошибка извлечения (дата в JSON не та), но места есть.
                                        slots_str = ", ".join(available_slots)
                                        hint_content = (
                                            f"[SYSTEM COMMAND] В прошлом шаге ты ошиблась в извлечении даты. "
                                            f"На самом деле пользователь выбрал {v_weekday} ({verified_date}). "
                                            f"На этот день есть свободные места: {slots_str}. "
                                            f"Сгенерируй ответ заново: подтверди дату ({v_weekday}, {verified_date}), "
                                            f"расскажи про доступное время и ОБЯЗАТЕЛЬНО установи 'interview_date' в JSON на '{verified_date}'."
                                        )
                                        
                                        # Алерт о галлюцинации в ТГ
                                        await mq.publish("tg_alerts", {
                                            "type": "hallucination",
                                            "dialogue_id": dialogue.id,
                                            "external_chat_id": dialogue.external_chat_id,
                                            "user_said": combined_masked_message,
                                            "llm_suggested": interview_date,
                                            "corrected_val": verified_date,
                                            "reasoning": audit_reason,
                                            "history_text": self._get_history_as_text(dialogue)
                                        })

                                    # 4. ИСПОЛНЕНИЕ (Ретрай)
                                    if hint_content:
                                        # Проверка на дубликаты (чтобы не спамить одной и той же командой подряд)
                                        last_msg_content = str(full_hist[-1].get("content", "")) if full_hist else ""
                                        
                                        if hint_content != last_msg_content:
                                            ctx_logger.info(f"Adding system command for {verified_date}. Retry triggered.")
                                            sys_msg = {
                                                "role": "user",
                                                "content": hint_content,
                                                "message_id": f"sys_date_fix_{time.time()}",
                                                "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
                                            }
                                            dialogue.history = full_hist + [sys_msg]
                                            dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
                                            await db.commit()
                                        else:
                                            ctx_logger.warning(f"System command already in history, but bot ignored it. Re-triggering retry for {verified_date}.")

                                        # В ЛЮБОМ СЛУЧАЕ прерываем текущую отправку и отправляем на ретрай
                                        await mq.publish("engine_tasks", {
                                            "dialogue_id": dialogue.id, 
                                            "trigger": "date_slots_refine",
                                            "initial_msg_count": hh_msg_count_start
                                        })
                                        return
                            except Exception as e:
                                ctx_logger.error(f"Ошибка в этапе Hint (Google Sheets): {e}")
                                await mq.publish("tg_alerts", {
                                    "type": "system",
                                    "text": f"🚨 **СБОЙ GOOGLE SHEETS:** Не удалось получить слоты для диалога `{dialogue.id}`. Проверьте таблицу!",
                                    "alert_type": "admin_only"
                                })
                                raise e

            # =====================================================================
            # [START] ШАГ 3: ЖЕСТКАЯ ВАЛИДАЦИЯ ВРЕМЕНИ (TIME ENFORCEMENT)
            # =====================================================================
            if new_state in DATE_CRITICAL_STATES and interview_date and interview_time:
                try:
                    # 1. Получаем свежий список слотов для этой даты
                    available_slots = await sheets_service.get_available_slots(interview_date, sheet_name)
                    
                    # 2. Фильтр "Сегодня"
                    now_msk = datetime.datetime.now(MOSCOW_TZ)
                    if interview_date == now_msk.strftime('%Y-%m-%d'):
                        available_slots = [s for s in available_slots if int(s.split(':')[0]) > now_msk.hour]

                    # 3. СРАВНЕНИЕ: Проверяем, входит ли время от LLM в список разрешенных
                    clean_time = interview_time.strip()
                    
                    if clean_time not in available_slots:
                        ctx_logger.warning(f"🚨 МОДЕЛЬ ВЫБРАЛА ЗАНЯТОЕ ВРЕМЯ! Выбрано: {clean_time}, Свободно: {available_slots}")

                        error_msg = f"На дату {interview_date} сейчас доступно только время: {', '.join(available_slots)}. Слот {clean_time} недоступен или уже занят."
                        
                        time_corr_cmd = {
                            'message_id': f'sys_time_corr_{time.time()}',
                            'role': 'user',
                            'content': (
                                f"[SYSTEM COMMAND] {error_msg} Извинись и предложи выбрать из свободных сейчас слотов: "
                                f"{', '.join(available_slots) if available_slots else 'другой день'}. "
                                f"ОБЯЗАТЕЛЬНО обнови поле 'interview_time' в JSON на null."
                            ),
                            'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()
                        }

                        dialogue.history = (dialogue.history or []) + [time_corr_cmd]
                        await db.commit()
                        
                       
                        await mq.publish("engine_tasks", {
                            "dialogue_id": dialogue.id, 
                            "trigger": "time_enforce_retry",
                            "initial_msg_count": hh_msg_count_start
                        })
                        return 

                except Exception as e:
                    ctx_logger.error(f"Ошибка в этапе жесткой валидации времени: {e}")
                    await mq.publish("tg_alerts", {
                        "type": "system",
                        "text": f"🚨 **СБОЙ GOOGLE SHEETS:** Не удалось получить слоты для диалога `{dialogue.id}`. Проверьте таблицу!",
                        "alert_type": "admin_only"
                    })
                    raise e

           
            
            # === 13. ОБНОВЛЕНИЕ ДАННЫХ В БД (С ВАЛИДАЦИЕЙ СТЕЙТОВ) ===
            
            # Обновляем статус диалога
            if dialogue.status == 'new':
                dialogue.status = 'in_progress'

            if extracted_data:
                # Берем стейт ДО обновления
                current_state_at_update = dialogue.current_state
                
                # Загружаем текущий профиль (или создаем новый)
                profile = dict(dialogue.candidate.profile_data or {})
                changed = False


                































                # --- 13.1 ОБРАБОТКА ВОЗРАСТА ---
                raw_age = extracted_data.get("age")
                if raw_age:
                    allowed_age_states = ['awaiting_age', 'clarifying_anything']

                    if current_state_at_update in allowed_age_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("age"):
                            ctx_logger.debug(f"Защита: поле age уже заполнено, пропускаем")
                        else:
                            try:
                                age_value = int(raw_age)
                                profile["age"] = age_value
                                changed = True
                                ctx_logger.info(f"✅ Возраст {age_value} принят")
                            except (ValueError, TypeError):
                                ctx_logger.warning(f"⚠️ Некорректный формат возраста от LLM: {raw_age}")

                # --- 13.2 ОБРАБОТКА ГРАЖДАНСТВА ---
                raw_citizenship = extracted_data.get("citizenship")
                if raw_citizenship:
                    allowed_cit_states = ['awaiting_citizenship', 'clarifying_anything']

                    if current_state_at_update in allowed_cit_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("citizenship"):
                            ctx_logger.debug(f"Защита: поле citizenship уже заполнено, пропускаем")
                        else:
                            cit_low = str(raw_citizenship).lower()
                            # Нормализуем РФ
                            is_rf = any(x in cit_low for x in ["россия", "рф", "российская", "russia"])
                            if is_rf:
                                profile["citizenship"] = "РФ"
                            else:
                                profile["citizenship"] = raw_citizenship
                            changed = True
                            ctx_logger.info(f"✅ Гражданство: {profile['citizenship']}")
                    else:
                        ctx_logger.debug(f"Игнорируем гражданство {raw_citizenship}: стейт {current_state_at_update} не разрешает.")

                # --- 13.3 ОБРАБОТКА ТИПА ЗАНЯТОСТИ (employment_type) ---
                # --- 13.3 ОБРАБОТКА ТИПА ЗАНЯТОСТИ (employment_type) ---
                raw_employment_type = extracted_data.get("employment_type")
                if raw_employment_type:
                    allowed_employment_states = ['awaiting_employment_type', 'clarifying_anything']

                    if current_state_at_update in allowed_employment_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("employment_type"):
                            ctx_logger.debug(f"Защита: поле employment_type уже заполнено, пропускаем")
                        else:
                            emp_type_low = str(raw_employment_type).lower()
                            target_state = None
                            
                            # Определяем тип и целевой стейт
                            if any(x in emp_type_low for x in ["full", "полная", "полный день", "any", "любая"]):
                                profile["employment_type"] = "full"
                                target_state = "awaiting_shift_preference"
                                ctx_logger.info(f"✅ Тип занятости: full. Насильно ставим {target_state}")
                            
                            elif any(x in emp_type_low for x in ["part", "частичная", "подработка"]):
                                profile["employment_type"] = "part"
                                target_state = "awaiting_ready_20_40_hours"
                                ctx_logger.info(f"✅ Тип занятости: part. Насильно ставим {target_state}")

                            # Если тип определен — сохраняем и мгновенно перегенерируем
                            if target_state:
                                dialogue.candidate.profile_data = profile
                                dialogue.current_state = target_state
                                # Важно: сохраняем всё, что успели извлечь до этого (age, citizenship)
                                await db.commit() 
                                
                                await mq.publish("engine_tasks", {
                                    "dialogue_id": dialogue.id, 
                                    "trigger": f"{target_state}_forced_refine",
                                    "initial_msg_count": hh_msg_count_start
                                })
                                return # ПРЕРЫВАЕМ обработку, уходим на круг перегенерации
                    else:
                        ctx_logger.debug(f"Игнорируем employment_type: стейт {current_state_at_update} не разрешает.")

                # --- 13.4 ОБРАБОТКА ГОТОВНОСТИ РАБОТАТЬ 20-40 ЧАСОВ (ready_20_40_hours) ---
                raw_ready_hours = extracted_data.get("ready_20_40_hours")
                if raw_ready_hours:
                    allowed_hours_states = ['awaiting_ready_20_40_hours', 'clarifying_anything']

                    if current_state_at_update in allowed_hours_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("ready_20_40_hours"):
                            ctx_logger.debug(f"Защита: поле ready_20_40_hours уже заполнено, пропускаем")
                        else:
                            hours_low = str(raw_ready_hours).lower()
                            if hours_low in ["yes", "да", "готов", "согласен"]:
                                profile["ready_20_40_hours"] = "yes"
                                changed = True
                                ctx_logger.info(f"✅ Готовность работать 20-40 часов: yes")
                            elif hours_low in ["no", "нет", "не готов", "не согласен"]:
                                profile["ready_20_40_hours"] = "no"
                                changed = True
                                ctx_logger.info(f"✅ Готовность работать 20-40 часов: no")
                            else:
                                ctx_logger.warning(f"⚠️ Некорректное значение ready_20_40_hours: {raw_ready_hours}")
                    else:
                        ctx_logger.debug(f"Игнорируем ready_20_40_hours: стейт {current_state_at_update} не разрешает.")

                # --- 13.5 ОБРАБОТКА ПРЕДПОЧТЕНИЙ ПО СМЕНЕ (shift_preference) ---
                raw_shift = extracted_data.get("shift_preference")
                if raw_shift:
                    allowed_shift_states = ['awaiting_shift_preference', 'clarifying_anything']

                    if current_state_at_update in allowed_shift_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("shift_preference"):
                            ctx_logger.debug(f"Защита: поле shift_preference уже заполнено, пропускаем")
                        else:
                            shift_low = str(raw_shift).lower()
                            if shift_low in ["morning", "утро", "утренняя"]:
                                profile["shift_preference"] = "morning"
                                changed = True
                                ctx_logger.info(f"✅ Предпочтение смены: morning")
                            elif shift_low in ["evening", "вечер", "вечерняя"]:
                                profile["shift_preference"] = "evening"
                                changed = True
                                ctx_logger.info(f"✅ Предпочтение смены: evening")
                            elif shift_low in ["any", "любая", "без разницы", "не важно"]:
                                profile["shift_preference"] = "any"
                                changed = True
                                ctx_logger.info(f"✅ Предпочтение смены: any")
                            else:
                                ctx_logger.warning(f"⚠️ Некорректное значение shift_preference: {raw_shift}")
                    else:
                        ctx_logger.debug(f"Игнорируем shift_preference: стейт {current_state_at_update} не разрешает.")

                # --- 13.6 ОБРАБОТКА ГОТОВНОСТИ К ТК РФ (employment_contract_ready) ---
                raw_contract = extracted_data.get("employment_contract_ready")
                if raw_contract:
                    allowed_contract_states = ['awaiting_employment_contract', 'clarifying_anything']

                    if current_state_at_update in allowed_contract_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("employment_contract_ready"):
                            ctx_logger.debug(f"Защита: поле employment_contract_ready уже заполнено, пропускаем")
                        else:
                            contract_low = str(raw_contract).lower()
                            if contract_low in ["yes", "да", "готов", "согласен"]:
                                profile["employment_contract_ready"] = "yes"
                                changed = True
                                ctx_logger.info(f"✅ Готовность к ТК РФ: yes")
                            elif contract_low in ["no", "нет", "не готов", "не согласен"]:
                                profile["employment_contract_ready"] = "no"
                                changed = True
                                ctx_logger.info(f"✅ Готовность к ТК РФ: no")
                            else:
                                ctx_logger.warning(f"⚠️ Некорректное значение employment_contract_ready: {raw_contract}")
                    else:
                        ctx_logger.debug(f"Игнорируем employment_contract_ready: стейт {current_state_at_update} не разрешает.")

                # --- 13.7 ОБРАБОТКА ВОЕННОГО БИЛЕТА (has_military_document) ---
                raw_military = extracted_data.get("has_military_document")
                if raw_military:
                    allowed_military_states = ['awaiting_military_document', 'clarifying_anything']

                    if current_state_at_update in allowed_military_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("has_military_document"):
                            ctx_logger.debug(f"Защита: поле has_military_document уже заполнено, пропускаем")
                        else:
                            military_low = str(raw_military).lower()
                            if military_low in ["yes", "да", "есть", "имеется"]:
                                profile["has_military_document"] = "yes"
                                changed = True
                                ctx_logger.info(f"✅ Военный билет: yes")
                            elif military_low in ["no", "нет", "не имеется"]:
                                profile["has_military_document"] = "no"
                                changed = True
                                ctx_logger.info(f"✅ Военный билет: no")
                            else:
                                ctx_logger.warning(f"⚠️ Некорректное значение has_military_document: {raw_military}")
                    else:
                        ctx_logger.debug(f"Игнорируем has_military_document: стейт {current_state_at_update} не разрешает.")

                # --- 13.8 ОСТАЛЬНЫЕ ПОЛЯ (Маппинг стейтов как в HH) ---
                
                # ФИО и Телефон (Колонки) - пишем всегда, если их нет (защита от перезаписи)
                # if extracted_data.get("full_name") and not dialogue.candidate.full_name:
                #     dialogue.candidate.full_name = extracted_data["full_name"]
                
                # if extracted_data.get("phone") and not dialogue.candidate.phone_number:
                #     dialogue.candidate.phone_number = extracted_data["phone"]

                
                
                
                if changed:
                    dialogue.candidate.profile_data = profile
                    is_ok = True 
                    reason = None
                    # ПРОВЕРЯЕМ НА ОТКАЗ ТОЛЬКО ЕСЛИ МЫ ЕЩЕ НЕ В ПРОЦЕССЕ ЗАПИСИ
                    SCHEDULING_STATES = ['init_scheduling_spb', 'scheduling_spb_day', 'scheduling_spb_time', 'interview_scheduled_spb', 'post_qualification_chat', 'forwarded_to_researcher']
                    
                    if current_state_at_update not in SCHEDULING_STATES:
                        is_ok, reason = self._check_eligibility(profile)
                    if not is_ok:
                        ctx_logger.info(f"⛔ МГНОВЕННЫЙ ОТКАЗ: {reason}. Прерываем анкету.")
                        new_state = 'qualification_failed'
                        dialogue.status = 'rejected'
                        # Берем прощальную фразу из твоего нового конфига
                        bot_response_text = settings.messages.qualification_failed_farewell
                        
                        # Записываем аналитику отказа
                        await log_event(
                            db, dialogue, 
                            'rejected_by_bot', 
                            event_data={"reason": reason, "at_state": current_state_at_update}
                        )
                                
                
                await db.flush()

                # --- [НОВОЕ] Логика мгновенной перегенерации для уточнения типа занятости / ТК РФ ---
                # is_part_time_refine = (new_state == 'awaiting_ready_20_40_hours' and profile.get("employment_type") == 'part')
                # is_shift_refine = (new_state == 'awaiting_shift_preference' and profile.get("employment_type") == 'full')
                is_contract_refine = (new_state == 'awaiting_employment_contract')

                # if (is_part_time_refine or is_shift_refine or is_contract_refine) and dialogue.current_state != new_state:
                if (is_contract_refine) and dialogue.current_state != new_state:
                    ctx_logger.debug(f"🔄 Переход в {new_state}. Сохраняем и отправляем на перегенерацию.")
                    
                    dialogue.current_state = new_state
                    dialogue.candidate.profile_data = profile
                    
                    await db.commit()
                    
                    await mq.publish("engine_tasks", {
                        "dialogue_id": dialogue.id, 
                        "trigger": f"{new_state}_refine_retry"
                    })
                    return # Прерываем текущий цикл, чтобы не слать старый ответ

























            # === 14. БЛОК КВАЛИФИКАЦИИ И ПРИНЯТИЯ РЕШЕНИЙ ===

            # ==========================================================================================
            # БЛОК ВАЛИДАЦИИ И ПРИНЯТИЯ РЕШЕНИЙ
            # ==========================================================================================
            
            # Проверяем условия, только если LLM пытается завершить анкету (new_state == 'qualification_complete')
            if dialogue.status not in ['qualified', 'rejected'] and new_state == 'qualification_complete':

                # --- 14.0 ПРОВЕРКА: ВОЕННЫЙ БИЛЕТ ДЛЯ МУЖЧИН ПРИЗЫВНОГО ВОЗРАСТА ---
                profile = dialogue.candidate.profile_data or {}
                gender = profile.get("gender")
                age = profile.get("age")

                if gender == "male" and age is not None and 18 <= int(age) <= 30:
                    if not profile.get("has_military_document"):
                        ctx_logger.info(f"🎖️ Кандидат — мужчина {age} лет, требуется уточнение военного билета.")

                        correction_msg = (
                            f"[SYSTEM COMMAND] Кандидат — мужчина призывного возраста ({age} лет). "
                            f"Ты ОБЯЗАНА уточнить наличие военного билета или приписного свидетельства. "
                            f"Установи стейт 'awaiting_military_document' и задай этот вопрос."
                        )

                        sys_msg = {
                            "role": "user",
                            "content": correction_msg,
                            "message_id": f"sys_military_check_{time.time()}",
                            "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
                        }

                        # Сохраняем профиль и уходим на ретрай
                        dialogue.candidate.profile_data = profile
                        dialogue.history = (dialogue.history or []) + [sys_msg]
                        dialogue.current_state = "awaiting_military_document"
                        await db.commit()

                        await mq.publish("engine_tasks", {
                            "dialogue_id": dialogue.id, 
                            "trigger": "military_refine",
                            "initial_msg_count": hh_msg_count_start
                        })
                        return

                # --- 14.1 ПРОВЕРКА: ЗАДАВАЛСЯ ЛИ ВОПРОС ПРО ТЕЛЕФОН (Копия логики HH) ---
                if not dialogue.candidate.phone_number:
                    phone_keywords = ["телефон", "номер"]
                    was_phone_asked = False
                    
                    # Пробегаем по истории сообщений БОТА
                    history_to_check = dialogue.history or []
                    for msg in history_to_check:
                        if msg.get('role') == 'assistant':
                            content_lower = str(msg.get('content', '')).lower()
                            if any(kw in content_lower for kw in phone_keywords):
                                was_phone_asked = True
                                break
                    
                    if not was_phone_asked:
                        ctx_logger.warning(f"🛑 БЛОКИРОВКА ЗАВЕРШЕНИЯ: Бот забыл спросить телефон.")
                        system_command = {
                            'message_id': f'sys_cmd_ask_phone_force_{time.time()}',
                            'role': 'user',
                            'content': (
                                "[SYSTEM COMMAND] Ты пытаешься завершить анкету (qualification_complete), "
                                "но ты не спросила номер телефона. Это критическая ошибка. "
                                "Ты ОБЯЗАНА спросить номер телефона прямо сейчас. Перейди в стейт awaiting_phone."
                            ),
                            'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()
                        }
                        dialogue.current_state = 'awaiting_phone'
                        dialogue.history = (dialogue.history or []) + [system_command]
                        await db.commit()
                        
                        
                        await mq.publish("engine_tasks", {
                            "dialogue_id": dialogue.id, 
                            "trigger": "force_phone_retry",
                            "initial_msg_count": hh_msg_count_start
                        })
                        return

                # --- 14.2 ПРОВЕРКА ПОЛНОТЫ АНКЕТЫ (Динамический LLM Recovery) ---
                profile = dialogue.candidate.profile_data or {}

                # 1. Собираем карту РЕАЛЬНО отсутствующих данных
                missing_data_map = self._get_missing_fields_map(profile)

                # Если есть пробелы — запускаем точечный поиск в истории
                if missing_data_map:
                    ctx_logger.info(f"🔍 Анкета не полна. Запуск Recovery для ключей: {list(missing_data_map.keys())}")

                    # Подготовка истории (последние 20 сообщений)
                    clean_history_lines = []
                    for m in (dialogue.history or []):
                        content = m.get('content', '')
                        # Фильтруем мусор и системные команды
                        if self._is_technical_message(content):
                            continue
                        if not str(content).startswith('[SYSTEM'):
                            role = "Кандидат" if m.get('role') == 'user' else "Бот"
                            clean_history_lines.append(f"{role}: {content}")
                    recent_history_text = "\n".join(clean_history_lines[-20:])

                    # Генерируем динамическую инструкцию по формату JSON
                    json_format_example = "{\n" + ",\n".join([f'  "{k}": <значение или null>' for k in missing_data_map.keys()]) + "\n}"

                    # Генерируем описание того, что искать
                    fields_to_search = "\n".join([f"- {k} ({v})" for k, v in missing_data_map.items()])

                    recovery_prompt = (
                        f"Ты — технический аналитик-экстрактор. Твоя задача: найти в диалоге ответы на конкретные вопросы, которые бот мог пропустить.\n\n"
                        f"[ЧТО НУЖНО НАЙТИ]:\n{fields_to_search}\n\n"
                        f"[ПРАВИЛА]:\n"
                        f"1. Используй ТОЛЬКО информацию из сообщений с пометкой 'Кандидат'.\n"
                        f"2. Если информации НЕТ в тексте, строго пиши null.\n"
                        f"3. НЕ ПРИДУМЫВАЙ данные. Если кандидат сомневается или не ответил — пиши null.\n\n"
                        f"Ответ верни СТРОГО в формате JSON:\n{json_format_example}"
                    )

                    try:
                        recovery_attempts = []
                        recovery_response = await get_bot_response(
                            system_prompt=recovery_prompt,
                            dialogue_history=[],
                            user_message=f"ИСТОРИЯ ДИАЛОГА ДЛЯ АНАЛИЗА:\n{recent_history_text}",
                            attempt_tracker=recovery_attempts,
                            extra_context=ctx_logger.extra
                        )

                        if recovery_response:
                            await self._log_llm_usage(db, dialogue, "Data_Recovery_Audit", recovery_response.get("usage_stats"), model_name="gpt-4o-mini")

                            extracted_data = recovery_response.get('parsed_response', {})
                            is_profile_updated = False

                            # Обрабатываем то, что удалось найти в истории
                            for key in list(missing_data_map.keys()):
                                val = extracted_data.get(key)
                                if val is not None and str(val).lower() != 'null':
                                    profile[key] = val
                                    ctx_logger.info(f"✨ Recovery спас поле {key}: {val}")
                                    missing_data_map.pop(key)
                                    is_profile_updated = True

                            if is_profile_updated:
                                dialogue.candidate.profile_data = profile
                                await db.flush()

                    except Exception as e:
                        ctx_logger.error(f"❌ Ошибка в блоке Recovery: {e}")

                # 2. ФИНАЛЬНЫЙ ВЕРДИКТ: Если данные все еще нужны — отправляем бота спрашивать
                if missing_data_map:
                    missing_human_names = ", ".join(missing_data_map.values())
                    ctx_logger.warning(f"⚠️ Recovery не помог. Не хватает: {missing_human_names}")

                    sys_cmd_content = (
                        f"[SYSTEM COMMAND] Анкета не завершена. Тебе НЕОБХОДИМО уточнить следующие данные: {missing_human_names}. "
                        f"Прямо сейчас задай вопрос кандидату, чтобы узнать эти сведения. "
                        f"Используй стейт clarifying_anything для уточнения этих сведений. "
                        f"ЗАПРЕЩЕНО переходить в 'qualification_complete', пока эти поля пусты."
                        f"Если ты получила недостающие данные, то сразу переходи в 'qualification_complete'"
                    )

                    sys_msg = {
                        "role": "user",
                        "content": sys_cmd_content,
                        "message_id": f"sys_missing_retry_{time.time()}",
                        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                    dialogue.history = (dialogue.history or []) + [sys_msg]
                    dialogue.current_state = "clarifying_anything"
                    await db.commit()

                    await mq.publish("engine_tasks", {
                        "dialogue_id": dialogue.id, 
                        "trigger": "data_fix_retry",
                        "initial_msg_count": hh_msg_count_start
                    })
                    return







                # --- 14.3 ФИНАЛЬНЫЙ АУДИТ ДАННЫХ (Smart LLM - Auditor) ---
                ctx_logger.info("Запуск финального аудита данных через Smart LLM...")

                # Собираем чистую историю без системных команд
                all_msgs_for_verify = (dialogue.history or [])
                verify_history_lines = []
                for m in all_msgs_for_verify:
                    content = m.get('content', '')
                    # Фильтруем через твой метод + старый фильтр
                    if self._is_technical_message(content):
                        continue
                    if not str(content).startswith('[SYSTEM'):
                        label = "Кандидат" if m.get('role') == 'user' else "Бот"
                        verify_history_lines.append(f"{label}: {content}")

                full_history_text = "\n".join(verify_history_lines)

                verification_prompt = (
                    """[SYSTEM COMMAND] Ты — технический АУДИТОР данных.
                    Проанализируй диалог и извлеки финальные данные для квалификации.

                    ПРАВИЛА:
                    1. citizenship: Если Россия (РФ, Российская федерация) -> верни "РФ". Иначе — название страны.
                    2. age: Верни целое число лет. Если возраст не назывался — верни null.
                    3. employment_type: "full" (полная занятость) или "part" (частичная/подработка). Если не известно — null.
                    4. employment_contract_ready: "yes" если готов к ТК РФ, "no" если не готов. Если не известно — null.
                    5. ready_20_40_hours: "yes"/"no" — готовность работать 20-40 часов. Только если employment_type=part.
                    6. shift_preference: "morning"/"evening"/"any" — предпочтение по смене. Только если employment_type=full.
                    7. has_military_document: "yes"/"no" — наличие военного билета. Только для мужчин.

                    Верни ответ ТОЛЬКО в формате JSON:
                    {
                        "age": <число или null>,
                        "citizenship": "<строка или null>",
                        "employment_type": "<full/part или null>",
                        "employment_contract_ready": "<yes/no или null>",
                        "ready_20_40_hours": "<yes/no или null>",
                        "shift_preference": "<morning/evening/any или null>",
                        "has_military_document": "<yes/no или null>",
                        "reasoning": "<твое краткое обоснование>"
                    }
                    """
                )

                verify_attempts = []
                try:
                    verify_response = await get_bot_response(
                        system_prompt=verification_prompt,
                        dialogue_history=[],
                        user_message=f"ИСТОРИЯ ДИАЛОГА:\n{full_history_text}",
                        attempt_tracker=verify_attempts,
                        extra_context=ctx_logger.extra
                    )

                    if verify_response:
                        await self._log_llm_usage(db, dialogue, "Final_Audit", verify_response.get("usage_stats"), model_name="gpt-4o")

                        v_data = verify_response.get('parsed_response', {})

                        # Сравниваем аудит с тем, что у нас в БД
                        discrepancies = []

                        audit_fields = [
                            "age", "citizenship", "employment_type", "employment_contract_ready",
                            "ready_20_40_hours", "shift_preference", "has_military_document"
                        ]

                        for field in audit_fields:
                            v_val = v_data.get(field)
                            db_val = profile.get(field)

                            # Нормализуем для сравнения
                            v_str = str(v_val).strip().lower() if v_val is not None else ""
                            db_str = str(db_val).strip().lower() if db_val is not None else ""

                            if v_str and v_str != "null" and v_str != db_str:
                                discrepancies.append({
                                    "field": field,
                                    "db_value": db_val,
                                    "audit_value": v_val
                                })

                        if discrepancies:
                            ctx_logger.warning(f"🚨 РАССИНХРОН АУДИТА! Найдено {len(discrepancies)} расхождений:")
                            for d in discrepancies:
                                ctx_logger.warning(f"   {d['field']}: БД={d['db_value']}, Аудит={d['audit_value']}")

                            # Отправляем алерт верификации
                            await mq.publish("tg_alerts", {
                                "type": "verification",
                                "dialogue_id": dialogue.id,
                                "external_chat_id": dialogue.external_chat_id,
                                "discrepancies": discrepancies,
                                "reasoning": v_data.get("reasoning", "не указано"),
                                "history_text": self._get_history_as_text(dialogue)
                            })

                    ctx_logger.info("✅ Финальная верификация (Аудитор) пройдена.")

                except Exception as e:
                    ctx_logger.error(f"Ошибка процесса аудитора: {e}", exc_info=True)
                    # В случае ошибки LLM аудита — не рискуем, возвращаемся
                    return
















































                    # === 14.4 ПРИНЯТИЕ РЕШЕНИЯ (ELIGIBILITY) ===
                ctx_logger.info(f"[{dialogue.external_chat_id}] Запуск проверки критериев квалификации.")

                profile = dialogue.candidate.profile_data or {}
                is_ok, reason = self._check_eligibility(profile)

                # --- ИТОГОВОЕ РЕШЕНИЕ ---
                if is_ok:
                    # --- СЦЕНАРИЙ 1: ПОДХОДИТ (Начинаем запись) ---
                    ctx_logger.info(
                        f"[{dialogue.external_chat_id}] Кандидат прошел проверку. Запуск автоматической записи.",
                        extra={"action": "qualification_passed_by_code"}
                    )

                    # 1. Сохраняем текущие ответы в историю (чтобы LLM их видела при перегенерации)
                    current_history = list(dialogue.history or [])
                    dialogue.history = (current_history)[-150:]

                    # 2. Формируем системную команду для LLM
                    system_command = {
                        'message_id': f'sys_cmd_start_sched_{time.time()}',
                        'role': 'user',
                        'content': (
                            '[SYSTEM COMMAND] Кандидат успешно прошел квалификацию. '
                            'Начни запись на собеседование: предложи выбрать день, используя календарь из промпта.'
                        ),
                        'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }

                    # 3. Обновляем диалог для перегенерации
                    # мы не используем pending_messages для этого, а кладем прямо в историю
                    dialogue.history.append(system_command)
                    dialogue.current_state = 'init_scheduling_spb'
                    dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
                    
                    await db.commit()

                    # 4. Ретрай задачи в RabbitMQ для мгновенного ответа с датами
                    
                    await mq.publish("engine_tasks", {
                        "dialogue_id": dialogue.id, 
                        "trigger": "start_scheduling_trigger",
                        "initial_msg_count": hh_msg_count_start
                    })
                    return

                else:
                    # --- СЦЕНАРИЙ 2: ОТКАЗ ---
                    ctx_logger.info(
                        f"[{dialogue.external_chat_id}] Отказ по критериям квалификации. Причина: {reason}",
                        extra={"action": "qualification_failed_by_code", "reason": reason}
                    )

                    # Устанавливаем статус и вежливую фразу из ТЗ
                    new_state = 'qualification_failed'
                    dialogue.status = 'rejected'
                    bot_response_text = (
                        "Спасибо! Я передам Вашу анкету для рассмотрения. "
                        "Если по Вашей анкету будет принято положительное решение, "
                        "с Вами свяжутся в течение трёх рабочих дней."
                    )
                    
                    
                    # ИСПРАВЛЕНИЕ: Проверка на существование записи перед добавлением
                    existing_rejected_event = await db.scalar(
                        select(AnalyticsEvent)
                        .filter(AnalyticsEvent.dialogue_id == dialogue.id)
                        .filter(AnalyticsEvent.event_type == 'rejected_by_bot')
                    )

                    await log_event(
                        db, dialogue, 
                        'rejected_by_bot', 
                        event_data={"reason": "eligibility_failed", "details": profile},
                        check_duplicates=True
                    )

            # === 15. ОБРАБОТКА СПЕЦИФИЧНЫХ СОСТОЯНИЙ (Call Later & Scheduling) ===

            # --- 15.1 Состояние "Перезвонить позже" (call_later) ---
            if new_state == 'call_later':
                meta = dict(dialogue.metadata_json or {})
                
                # Проверяем, не помечали ли мы это уже (аналог проверки очереди в HH)
                if not meta.get("call_later_flag"):
                    ctx_logger.info(f"[{dialogue.external_chat_id}] Кандидат попросил связаться позже. Фиксируем.")
                    
                    
                    await log_event(
                        db, dialogue, 
                        'call_later_requested', 
                        event_data={"previous_state": dialogue.current_state}
                    )
                    
                    meta["call_later_flag"] = True
                    dialogue.metadata_json = meta
                else:
                    ctx_logger.debug("Флаг call_later уже стоит. Пропуск.")

            ## --- 15.2 Логика ПЕРЕНОСА (Reschedule) для уже квалифицированных ---
            if new_state in ['forwarded_to_researcher', 'interview_scheduled_spb'] and dialogue.status == 'qualified':
                if new_state == 'interview_scheduled_spb':
                    interview_date = extracted_data.get("interview_date")
                    interview_time = extracted_data.get("interview_time")

                    if interview_date and interview_time:
                        meta = dict(dialogue.metadata_json or {})
                        old_date = meta.get("interview_date")
                        old_time = meta.get("interview_time")

                        # ПРОВЕРКА НА ПЕРЕНОС (Reschedule)
                        if old_date is not None and (old_date != interview_date or old_time != interview_time):
                            ctx_logger.info(f"🔄 ПЕРЕНОС В КАЛЕНДАРЕ: {old_date} {old_time} -> {interview_date} {interview_time}")

                            # 1. ПРЯМОЕ ДЕЙСТВИЕ: Освобождаем старый слот
                            await sheets_service.release_slot(old_date, old_time, sheet_name)

                            # 2. ПРЯМОЕ ДЕЙСТВИЕ: Занимаем новый слот
                            await sheets_service.book_slot(
                                target_date=interview_date,
                                target_time=interview_time,
                                candidate_name=dialogue.candidate.full_name or "Кандидат",
                                candidate_phone=dialogue.candidate.phone_number or "",
                                sheet_name=sheet_name
                            )

                            # 3. Talantix: удаляем старую встречу и создаём новую
                            if settings.services.talantix.enabled:
                                talantix_data = meta.get("talantix")
                                old_meeting_id = talantix_data.get("meeting_id") if talantix_data else None

                                if old_meeting_id:
                                    try:
                                        ctx_logger.info(f"🗑️ Talantix: удаляю старую встречу meeting_id={old_meeting_id}")
                                        await talantix_service.release_interview(interview_id=old_meeting_id)
                                    except Exception as e:
                                        ctx_logger.error(f"❌ Ошибка удаления встречи в Talantix: {e}", exc_info=True)

                                if talantix_data:
                                    try:
                                        meeting_id = await self._create_talantix_meeting(
                                            dialogue=dialogue,
                                            interview_date=interview_date,
                                            interview_time=interview_time,
                                            talantix_data=talantix_data,
                                            ctx_logger=ctx_logger,
                                        )
                                        if meeting_id:
                                            if "talantix" not in meta:
                                                meta["talantix"] = {}
                                            meta["talantix"]["meeting_id"] = meeting_id
                                            dialogue.metadata_json = meta
                                            flag_modified(dialogue, "metadata_json")
                                            ctx_logger.info(f"✅ Talantix: новая встреча meeting_id={meeting_id}")
                                        else:
                                            ctx_logger.warning("⚠️ Talantix: новая встреча не создана (meeting_id=None)")
                                    except Exception as e:
                                        ctx_logger.error(f"❌ Ошибка создания встречи в Talantix (reschedule): {e}", exc_info=True)

                            # 4. Пушим задачу на уведомление в RabbitMQ
                            await mq.publish("services_output", {
                                "dialogue_id": dialogue.id,
                                "type": "rescheduled",
                                "old_slot": f"{old_date} {old_time}",
                                "new_slot": f"{interview_date} {interview_time}"
                            })
                            
                            # 4. Аналитика и напоминания
                            await log_event(db, dialogue, 'interview_rescheduled', {
                                "old": f"{old_date} {old_time}", "new": f"{interview_date} {interview_time}"
                            })
                            await self._schedule_interview_reminders(db, dialogue, interview_date, interview_time)

                            # 5. Комментарий в Talantix
                            if settings.services.talantix.enabled:
                                try:
                                    await talantix_crm_service.notify_talantix_comment(
                                        dialogue=dialogue,
                                        event_type='rescheduled',
                                        db=db
                                    )
                                except Exception as e:
                                    ctx_logger.error(f"Ошибка создания комментария Talantix (rescheduled): {e}")

                            # Обновляем метаданные
                            meta["interview_date"] = interview_date
                            meta["interview_time"] = interview_time
                            dialogue.metadata_json = meta
                            flag_modified(dialogue, "metadata_json")
                            
                        else:
                            ctx_logger.debug("Дата записи не изменилась или это не перенос.")
                    
                    # После обработки записи/переноса всегда уходим в чат поддержки
                    new_state = 'post_qualification_chat'

            # --- 15.3 Логика ПЕРВИЧНОЙ квалификации ---
            if new_state in ['forwarded_to_researcher', 'interview_scheduled_spb'] and dialogue.status != 'qualified':
                ctx_logger.info(f"🟢 Candidate qualified. Запись в календарь.")
                
                dialogue.status = 'qualified'
                meta = dict(dialogue.metadata_json or {})
                meta["interview_date"] = extracted_data.get("interview_date")
                meta["interview_time"] = extracted_data.get("interview_time")
                dialogue.metadata_json = meta
                flag_modified(dialogue, "metadata_json")

                # 1. ПРЯМОЕ ДЕЙСТВИЕ: Занимаем слот в Google Таблице
                if meta["interview_date"] and meta["interview_time"]:
                    await sheets_service.book_slot(
                        target_date=meta["interview_date"],
                        target_time=meta["interview_time"],
                        candidate_name=dialogue.candidate.full_name or "Кандидат",
                        candidate_phone=dialogue.candidate.phone_number or "",
                        sheet_name=sheet_name
                    )
                    # Планируем напоминания в БД
                    await self._schedule_interview_reminders(db, dialogue, meta["interview_date"], meta["interview_time"])

                    # 1.1. Создаём встречу в Talantix
                    if settings.services.talantix.enabled:
                        talantix_data = meta.get("talantix")
                        if talantix_data:
                            try:
                                meeting_id = await self._create_talantix_meeting(
                                    dialogue=dialogue,
                                    interview_date=meta["interview_date"],
                                    interview_time=meta["interview_time"],
                                    talantix_data=talantix_data,
                                    ctx_logger=ctx_logger,
                                )
                                if meeting_id:
                                    # Сохраняем meeting_id in metadata
                                    if "talantix" not in meta:
                                        meta["talantix"] = {}
                                    meta["talantix"]["meeting_id"] = meeting_id
                                    dialogue.metadata_json = meta
                                    flag_modified(dialogue, "metadata_json")
                                    await db.commit()
                                    ctx_logger.info(f"✅ meeting_id={meeting_id} сохранён в metadata")
                                else:
                                    ctx_logger.warning("⚠️ Talantix: встреча не создана (meeting_id=None)")
                            except Exception as e:
                                ctx_logger.error(f"❌ Ошибка создания встречи в Talantix: {e}", exc_info=True)
                        else:
                            ctx_logger.warning("⚠️ talantix данные не найдены в metadata, встреча в Talantix не создана")

                # 2. Аналитика
                await log_event(db, dialogue, 'qualified', check_duplicates=True)

                # 3. Пушим задачу на карточку в ТГ и запись в Таблицу Кандидатов
                await mq.publish("services_output", {
                    "dialogue_id": dialogue.id,
                    "type": "qualified"
                })

                # 4. Комментарий в Talantix
                if settings.services.talantix.enabled:
                    try:
                        await talantix_crm_service.notify_talantix_comment(
                            dialogue=dialogue,
                            event_type='qualified',
                            db=db
                        )
                    except Exception as e:
                        ctx_logger.error(f"Ошибка создания комментария Talantix (qualified): {e}")

                # [HH ONLY] Перемещаем отклик в папку 'interview'
                if dialogue.account.platform == 'hh' and dialogue.external_chat_id:
                    try:
                        await hh.move_response_to_folder(dialogue.account, db, dialogue.external_chat_id, 'interview')
                    except Exception as e:
                        ctx_logger.error(f"Ошибка перемещения отклика HH в 'interview': {e}")

                dialogue.current_state = 'post_qualification_chat'
                new_state = 'post_qualification_chat'
            

                
                
    


            # === 16. ОБРАБОТКА ОТКАЗОВ И ЗАВЕРШЕНИЯ ===
            if new_state in ['qualification_failed', 'declined_vacancy', 'declined_interview']:
                
                # --- 16.1 ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА ОТКАЗА (Механика "Судьи") ---
                if new_state == 'declined_vacancy':
                    ctx_logger.info("Проверка серьезности отказа кандидата через 'Судью'...")
                    
                    # 1. Сбор контекста (как в HH)
                    all_msgs = (dialogue.history or [])
                    clean_history_with_roles = []
                    for m in all_msgs:
                        content = m.get('content', '')
                        # Добавляем твою фильтрацию
                        if self._is_technical_message(content):
                            continue
                        if not str(content).startswith("[SYSTEM"):
                            role_label = "Кандидат" if m.get('role') == 'user' else "Бот"
                            clean_history_with_roles.append(f"{role_label}: {content}")
                    
                    recent_context = "\n".join(clean_history_with_roles[-20:])

                    clarification_prompt = (
                        'Проанализируй диалог и определи: действительно ли кандидат чётко отказался от вакансии? '
                        'Смотри только на реплики с пометкой "Кандидат". '
                        'Верни ответ строго в формате JSON: {"answer": "yes" или "no"} '
                        'Ответ "yes" — только если кандидат прямо сказал, что вакансия его не интересует или он отказывается. '
                        'Если кандидат задает вопросы или сомневается — верни "no".'
                    )

                    clarification_attempts = []
                    clarification_result = None
                    try:
                        clarification_result = await get_bot_response(
                            system_prompt=clarification_prompt,
                            dialogue_history=[], 
                            user_message=f"ИСТОРИЯ ДИАЛОГА (последние реплики):\n{recent_context}",
                            
                            attempt_tracker=clarification_attempts,
                            skip_instructions=True,
                            extra_context=ctx_logger.extra
                        )

                        # Логируем ретраи и токены (копия логики HH)
                        if clarification_result:
                            total_attempts = len(clarification_attempts)
                            if total_attempts > 1:
                                for i in range(total_attempts - 1):
                                    await self._log_llm_usage(db, dialogue, f"Decline_Clarification (RETRY #{i+1})")
                            
                            await self._log_llm_usage(db, dialogue, "Decline_Clarification", clarification_result.get('usage_stats'))

                    except Exception as e:
                        ctx_logger.warning(f"Ошибка при уточнении отказа: {e}. Считаем отказом по умолчанию.")
                        # Логируем провальные попытки
                        for i in range(len(clarification_attempts)):
                            await self._log_llm_usage(db, dialogue, f"Decline_Clarification (FAILED #{i+1})")
                        clarification_result = None

                    is_real_decline = True # По умолчанию — отказ
                    if clarification_result and 'parsed_response' in clarification_result:
                        is_real_decline = (clarification_result['parsed_response'].get('answer') == 'yes')

                    if not is_real_decline:
                        # Кандидат НЕ отказался → Оживляем диалог (Veto)
                        ctx_logger.info("⚠️ Судья решил: отказ ложный. Возвращаем диалог в работу.")
                        
                        system_command = {
                            'message_id': f'sys_revive_{time.time()}',
                            'role': 'user',
                            'content': (
                                '[SYSTEM COMMAND] Сейчас кандидат не отказывается от вакансии и анкетирования. '
                                'Он задал вопрос или выразил сомнение. Не ставь declined_vacancy! '
                                'Твоя задача — вежливо ответить на его вопрос/сомнение и продолжить анкету.'
                            ),
                            'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()
                        }
                        
                        # Сохраняем историю и триггерим воркер заново
                        dialogue.history = (dialogue.history or []) + [system_command]
                        await db.commit()
                        
                        
                        await mq.publish("engine_tasks", {
                            "dialogue_id": dialogue.id, 
                            "trigger": "decline_veto_retry",
                            "initial_msg_count": hh_msg_count_start
                        })
                        return 

                # --- 16.2 ОТМЕНА В КАЛЕНДАРЕ ---
                meta = dialogue.metadata_json or {}
                if meta.get("interview_date") and meta.get("interview_time"):
                    ctx_logger.info(f"🗑️ ОТМЕНА: Освобождаю слот {meta.get('interview_date')} {meta.get('interview_time')}")

                    # 1. ПРЯМОЕ ДЕЙСТВИЕ: Освобождаем слот
                    await sheets_service.release_slot(meta.get("interview_date"), meta.get("interview_time"), sheet_name)

                    # 2. Talantix: удаляем встречу
                    if settings.services.talantix.enabled:
                        talantix_data = meta.get("talantix")
                        meeting_id = talantix_data.get("meeting_id") if talantix_data else None

                        if meeting_id:
                            try:
                                ctx_logger.info(f"🗑️ Talantix: удаляю встречу meeting_id={meeting_id}")
                                await talantix_service.release_interview(interview_id=meeting_id)
                            except Exception as e:
                                ctx_logger.error(f"❌ Ошибка удаления встречи в Talantix: {e}", exc_info=True)
                        else:
                            ctx_logger.warning("⚠️ Talantix: meeting_id не найден, встреча не удалена")

                    # 3. Пушим задачу воркеру (например, отправить карточку отмены)
                    await mq.publish("services_output", {
                        "dialogue_id": dialogue.id,
                        "type": "cancelled"
                    })

                    # 4. Комментарий в Talantix
                    if settings.services.talantix.enabled:
                        try:
                            await talantix_crm_service.notify_talantix_comment(
                                dialogue=dialogue,
                                event_type='cancelled',
                                db=db
                            )
                        except Exception as e:
                            ctx_logger.error(f"Ошибка создания комментария Talantix (cancelled): {e}")

                # Отменяем напоминания в БД
                await db.execute(
                    update(InterviewReminder)
                    .where(InterviewReminder.dialogue_id == dialogue.id)
                    .where(InterviewReminder.status == 'pending')
                    .values(status='cancelled', processed_at=datetime.datetime.now(datetime.timezone.utc))
                )
                
                ctx_logger.info("Все запланированные напоминания отменены, освобожден слот.")
                
            

                # --- 16.3 ФИНАЛЬНАЯ ФИКСАЦИЯ СТАТУСА ---
                dialogue.status = 'rejected'

                # Определяем тип отказа для статистики
                stat_event_type = 'rejected_by_bot'
                if new_state in ['declined_vacancy', 'declined_interview']:
                    stat_event_type = 'rejected_by_candidate'

                await log_event(
                    db, dialogue,
                    stat_event_type,
                    event_data={"reason_state": new_state}
                )

                # Комментарий в Talantix
                if settings.services.talantix.enabled:
                    try:
                        reason_text = f"Отказ по причине: {new_state}" if new_state else "Отказ"
                        await talantix_crm_service.notify_talantix_comment(
                            dialogue=dialogue,
                            event_type='rejected',
                            db=db,
                            reason=reason_text
                        )
                    except Exception as e:
                        ctx_logger.error(f"Ошибка создания комментария Talantix (rejected): {e}")

                ctx_logger.info(f"Диалог завершен со статусом REJECTED (Тип: {stat_event_type}, Состояние: {new_state})")
                
                # [HH ONLY] Перемещаем отклик в папку 'assessment' (как просил пользователь)
                if dialogue.account.platform == 'hh' and dialogue.external_chat_id:
                    try:
                        await hh.move_response_to_folder(dialogue.account, db, dialogue.external_chat_id, 'assessment')
                    except Exception as e:
                        ctx_logger.error(f"Ошибка перемещения отклика HH в 'assessment': {e}")


            # === 17. ПОДГОТОВКА И ОТПРАВКА ОТВЕТА ===

            # Если LLM не вернула текст
            if bot_response_text is None or bot_response_text.strip() == "":
                
                # СЦЕНАРИЙ 1: ШТАТНОЕ МОЛЧАНИЕ (как в HH)
                # При завершении анкеты бот может молчать, так как мы перехватываем управление
                if new_state == 'qualification_complete':
                    ctx_logger.info("LLM промолчала на этапе 'qualification_complete' (штатно).")
                    
                    new_history = (dialogue.history or [])
                    dialogue.history = new_history[-150:]
                    dialogue.current_state = new_state
                    # Сбрасываем уровень напоминаний, так как мы "ответили" (обработали)
                    dialogue.reminder_level = 0
                    dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
                    
                    await db.commit()
                    return
                
                # СЦЕНАРИЙ 2: ОШИБОЧНОЕ МОЛЧАНИЕ
                else:
                    ctx_logger.error(f"LLM вернула пустой текст для активного стейта '{new_state}'!")
                    # Бросаем ошибку для отката транзакции и повтора
                    raise ValueError(f"Empty response forbidden for state: {new_state}")

            # ФИЗИЧЕСКАЯ ОТПРАВКА (Универсальная)
            real_id = None
            try:
            # === 17.5 ПРОВЕРКА АКТУАЛЬНОСТИ (HH ONLY) ===
                # Перед самой отправкой проверяем, не изменился ли диалог в HH,
                # пока мы думали (LLM + Audit). 
                # Сверяем общее количество сообщений с тем, что было на старте.
                # Напоминалки и дожимы пропускаем всегда.
                if dialogue.account.platform == 'hh' and dialogue.external_chat_id and not is_reminder:
                    try:
                        from app.connectors.hh.client import hh
                        status_data = await hh.get_negotiation_status(dialogue.account, db, dialogue.external_chat_id)
                        
                        if status_data and status_data.get("counters"):
                            hh_msg_count_now = status_data["counters"].get("messages", 0)
                            
                            # ПРОВЕРКА: Делаем Rollback только если мы получили валидный счетчик на старте (> 0)
                            if hh_msg_count_start > 0 and hh_msg_count_now > hh_msg_count_start:
                                ctx_logger.warning(
                                    f"🛑 ПРЕРЫВАНИЕ (END): В HH {hh_msg_count_now} сообщений, а было {hh_msg_count_start}. "
                                    f"Контекст устарел или другой воркер уже ответил. Делаю ROLLBACK."
                                )
                                await db.rollback()
                                return
                                
                    except Exception as e:
                        # Если проверка упала — игнорируем, чтобы не "заткнуть" бота
                        ctx_logger.error(f"⚠️ Ошибка проверки актуальности HH (END): {e}")
                elif is_reminder:
                    ctx_logger.debug(f"🔔 Режим напоминания/дожима. Пропускаю проверку END.")

                connector = get_connector(dialogue.account.platform)
                
                # Отправляем и ловим ID
                send_result = await connector.send_message(
                    account=dialogue.account,
                    db=db,
                    chat_id=dialogue.external_chat_id,
                    text=bot_response_text
                )
                
                if isinstance(send_result, dict):
                    real_id = send_result.get("id")

                ctx_logger.info(f"📤 Сообщение отправлено. ID: {real_id}")
                
                
            except Exception as e:
                # 3. Обработка ошибок
                error_str = str(e).lower()
                
                # Ошибки "Чат закрыт" или "Заблокировано" (общие для большинства API)
                if any(code in error_str for code in ["403", "404", "forbidden", "not found"]):
                    ctx_logger.warning(f"Ошибка API ({dialogue.account.platform}). Закрываем диалог. Error: {e}")
                    dialogue.status = 'closed'
                    await db.commit()
                    return
                else:
                    # Временные ошибки (500, таймаут) — возвращаем в очередь через rollback
                    ctx_logger.error(f"❌ Сбой сети/API {dialogue.account.platform}: {e}")
                    await db.rollback()
                    raise e # Бросаем ошибку, чтобы воркер сделал requeue (как мы настраивали)

            # === 18. ФИНАЛЬНОЕ СОХРАНЕНИЕ ИСТОРИИ ===

            # Создаем запись ответа бота (Формат как в HH, но с UTC)
            bot_msg_entry = {
                # Используем ID от Авито, чтобы избежать дублей при синхронизации
                'message_id': str(real_id) if real_id else f'bot_{time.time()}',
                'role': 'assistant',
                'content': bot_response_text,
                'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'state': new_state,
                'extracted_data': extracted_data
            }

            # Склеиваем: Старая история + Новые сообщения юзера + Ответ бота
            # Это гарантирует, что история в БД всегда будет полной и последовательной
            final_history = (dialogue.history or []) + [bot_msg_entry]
            
            # Ограничиваем размер (150 как в HH)
            dialogue.history = final_history[-150:]
            
            dialogue.current_state = new_state
            dialogue.status = 'in_progress' if dialogue.status == 'new' else dialogue.status
            dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
            dialogue.reminder_level = 0 # Сбрасываем напоминания после успешного ответа

            # Финальный коммит (с предварительным flush как в HH)
            await db.flush()
            await db.commit()
            
            ctx_logger.info(
                f"✅ Диалог {dialogue.external_chat_id} успешно обработан. Стейт: {new_state}",
                extra={"action": "dialogue_processed_success", "new_state": new_state}
            )

        except Exception as e:
            # Глобальный перехват ошибок внутри диалога
            ctx_logger.error(
                f"💥 Критическая ошибка обработки диалога {dialogue_id}: {e}", 
                exc_info=True,
                extra={"action": "process_dialogue_critical_error"}
            )
            await mq.publish("tg_alerts", {
                "type": "system",
                "text": f"🧠 **ENGINE RETRY**\nДиалог: `{dialogue.id}`\nОшибка: `{str(e)}`\n*Задача возвращена в очередь.*",
                "alert_type": "admin_only"
            })
            if db and db.is_active:
                await db.rollback()
            raise # Пробрасываем воркеру, чтобы он сделал nack (сообщение вернется в очередь)

        finally:
            # === 3. ОСВОБОЖДЕНИЕ БЛОКИРОВКИ ===
            await release_lock(lock_key)
            duration = time.monotonic() - dialogue_processing_start_time
            ctx_logger.debug(f"🏁 Обработка завершена за {duration:.2f} сек. Lock снят.")
     

# Глобальный экземпляр
dispatcher = Engine()