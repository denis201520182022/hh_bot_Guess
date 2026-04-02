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
from app.db.models import Dialogue, Candidate, JobContext, Account, LlmLog, AnalyticsEvent # Добавили AnalyticsEvent
from app.core.rabbitmq import mq
from typing import Dict, Any, List, Optional
from decimal import Decimal
from app.db.models import InterviewReminder, InterviewFollowup
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.services.sheets import sheets_service

from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal
from sqlalchemy import select, update, delete # Добавить delete
from app.db.models import Dialogue, Candidate, JobContext, Account, LlmLog, AnalyticsEvent # Добавить AnalyticsEvent
from app.connectors import get_connector
from app.connectors.hh import hh_connector
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

    def _calculate_age(self, birth_date_str: str) -> Optional[int]:
        """Вычисляет количество полных лет на основе даты рождения YYYY-MM-DD."""
        try:
            birth_date = datetime.datetime.strptime(birth_date_str, "%Y-%m-%d").date()
            today = datetime.date.today()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            return age
        except (ValueError, TypeError):
            return None
    
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
    
    async def _get_human_slots_block(self) -> str:
        """Формирует текстовый блок со свободными слотами для промпта."""
        all_slots = await sheets_service.get_all_slots_map()
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
        Проверка строго по 3 критериям.
        """
        # --- Критерий 1: Возраст (30-55) ---
        age_str = profile.get("age")
        if age_str is not None:
            try:
                age = int(age_str)
                if not (30 <= age <= 55):
                    return False, f"age_out_of_range_{age}"
            except (ValueError, TypeError):
                pass # Если LLM вернула невалидный возраст, просто игнорируем его

        # --- Критерий 2: Гражданство и Патент (Уточненная логика) ---
        citizenship = str(profile.get("citizenship", "")).strip().lower()
        has_patent = str(profile.get("has_patent", "")).strip().lower()
        
        # Проверяем, является ли гражданство РФ (учитываем разные написания)
        is_rf = any(x in citizenship for x in ["россия", "рф", "российская", "russia"])

        # Если указано гражданство НЕ РФ
        if citizenship and not is_rf:
            # ОТКАЗЫВАЕМ ТОЛЬКО ЕСЛИ:
            # Кандидат прямо сказал, что патента НЕТ
            if has_patent == "нет":
                return False, "non_rf_no_patent"
            
            # Если в поле патента "да" или там пока пусто (None/"") — НЕ отказываем.
            # Если пусто, бот просто пойдет уточнять дальше по сценарию.

        # --- Критерий 3: Судимость (Проверяем маркер "violent") ---
        criminal_record = str(profile.get("criminal_record", "")).lower()
        if criminal_record == "violent":
            return False, "violent_criminal_record"

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


    async def _assemble_dynamic_prompt(self, prompt_library: dict, dialogue_state: str, user_message: str, vacancy_description: str) -> str:
        """Сборка системного промпта из блоков библиотеки"""
        required_blocks = ['#ROLE_AND_STYLE#']
        
        state_map = {
            'initial': ['#QUALIFICATION_RULES#', '#FAQ#'],
            'awaiting_questions': ['#QUALIFICATION_RULES#', '#FAQ#', '#DECLINED_VAC#'],
            'awaiting_phone': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],
            'awaiting_fio': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],
            
            'awaiting_citizenship': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],
            'clarifying_citizenship': ['#QUALIFICATION_RULES#', '#CLARI#', '#DECLINED_VAC#'],
            'awaiting_age': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],
            'clarifying_anything': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],
            'qualification_complete': ['#QUALIFICATION_RULES#', '#DECLINED_VAC#'],

            'init_scheduling_spb': ['#SCHEDULING_ALGORITHM#'],
            'scheduling_spb_day': ['#SCHEDULING_ALGORITHM#'],
            'scheduling_spb_time': ['#SCHEDULING_ALGORITHM#'],
            'interview_scheduled_spb': ['#SCHEDULING_ALGORITHM#', '#FAQ#'],

            'call_later': ['#QUALIFICATION_RULES#', '#FAQ#'],
            'clarifying_declined_vacancy': ['#QUALIFICATION_RULES#'],
            'post_qualification_chat': ['#POSTCVAL#', '#FAQ#']
        }
        
        required_blocks.extend(state_map.get(dialogue_state, ['#QUALIFICATION_RULES#']))
        
        # Убираем дубли и собираем текст
        final_keys = list(dict.fromkeys(required_blocks))
        prompt_pieces = [prompt_library.get(key, '') for key in final_keys]
        
        # Определяем состояния, для которых нужен календарь
        SCHEDULING_STATES = ['init_scheduling_spb', 'scheduling_spb_day', 'scheduling_spb_time', 'post_qualification_chat', 'interview_scheduled_spb']

        # Если текущее состояние требует календаря, генерируем и добавляем его
        if dialogue_state in SCHEDULING_STATES:
            # 1. Добавляем "Человеческий" список слотов (твоя просьба)
            human_slots = await self._get_human_slots_block()
            prompt_pieces.append(human_slots)

            # 2. Добавляем Динамический календарь (технический блок для выбора дат)
            all_slots = await sheets_service.get_all_slots_map()
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
            except Exception as e:
                ctx_logger.error(f"💥 Критическая ошибка обработки диалога: {e}", exc_info=True)
                # Тут можно добавить отправку алерта в Sentry/Telegram
                raise e
            finally:
                duration = time.monotonic() - start_time
                ctx_logger.info(f"🏁 Обработка завершена за {duration:.2f} сек.")


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
            raise Exception("Dialogue is locked by another worker.")
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
                    selectinload(Dialogue.followups)    # InterviewFollowup
                )
                .with_for_update()      # Блокируем строку от других воркеров
            )
            
            result = await db.execute(stmt)
            dialogue = result.scalar_one_or_none()

            # Если диалог занят другим процессом или не найден
            if not dialogue:
                ctx_logger.debug(f"Dialogue {dialogue_id} is locked or not found. Skipping.")
                return

            account = dialogue.account
            if not account:
                ctx_logger.error(f"Account for dialogue {dialogue_id} not found")
                return

            # === 3. ОБНОВЛЕНИЕ КОНТЕКСТА ЛОГГЕРА ===
            # Теперь логгер знает все детали, как в референсе
            ctx_logger.extra.update({
                "external_chat_id": dialogue.external_chat_id,  # Аналог hh_response_id
                "account_name": account.name,                   # Аналог recruiter_name
                "vacancy_id": dialogue.vacancy_id,
                "vacancy_title": dialogue.vacancy.title if dialogue.vacancy else "Unknown",
                "candidate_id": dialogue.candidate_id,
                "state": dialogue.current_state
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
                "state": dialogue.current_state
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

                if extracted_fio:
                    # Записываем ФИО, если оно найдено регуляркой
                    dialogue.candidate.full_name = extracted_fio
                    ctx_logger.info(f"👤 Извлечено ФИО из текста: {extracted_fio}")

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
                relevant_vacancy_desc = dialogue.vacancy.description_data.get("text", "")

            # Собираем системный промпт из блоков (#ROLE#, #FAQ# и т.д.)
            system_prompt = await self._assemble_dynamic_prompt(
                prompt_library,
                dialogue.current_state,
                combined_masked_message.lower(),
                relevant_vacancy_desc
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
                'awaiting_fio',
                'awaiting_citizenship', 
                'clarifying_citizenship',
                'awaiting_age',
                'awaiting_experience',
                'awaiting_readiness',
                'awaiting_medbook',
                'awaiting_criminal',
                'clarifying_anything',
                'clarifying_declined_vacancy',
                'qualification_complete',
                
                
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
                        f"Проанализируй диалог заново и выбери корректное состояние СТРОГО из разрешенного списка."
                    ),
                    'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()
                }

                # 2. Сохраняем историю и добавляем системную команду в конец
                # В нашей архитектуре нет pending_messages, команда кладется прямо в историю.
                dialogue.history = (dialogue.history or []) + [hallucination_corr_cmd]
                dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)

                # 3. Фиксируем изменения в базе
                await db.commit()
                
                # 4. Отправляем задачу на переобработку с новой системной командой
                await mq.publish("engine_tasks", {"dialogue_id": dialogue.id, "trigger": "state_correction_retry"})
                
                ctx_logger.info(f"Отправлено на исправление галлюцинации стейта: {new_state}")
                return # Обязательно выходим, чтобы текущая обработка прекратилась
            # --- [END] ВАЛИДАЦИЯ СТАТУСА ---


            # === 12. ВАЛИДАЦИЯ ДАТЫ И ВРЕМЕНИ (АУДИТ + РЕГЛАМЕНТ + СЛОТЫ) ===
            DATE_CRITICAL_STATES = ['init_scheduling_spb', 'scheduling_spb_day', 'scheduling_spb_time', 'interview_scheduled_spb']
            
            

            # Список ключевых слов (как в HH)
            TIME_KEYWORDS = [
                "сегодня", "завтра", "послезавтра", "понедельник", "вторник", "сред", "четверг", 
                "пятниц", "суббот", "воскресен", "январ", "феврал", "март", "апрел", "май", "июн", 
                "июл", "август", "сентябр", "октябр", "ноябр", "декабр", "число", "время", "числа", "числ", "03", "04"
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
                    # --- 12.1 УМНЫЙ АУДИТ ДАТЫ (Smart Model) ---

                    # Берем сохраненную дату из метаданных (аналог interview_datetime_utc в HH)
                    stored_meta = dialogue.metadata_json or {}
                    stored_date = stored_meta.get("interview_date")

                    run_audit = True
                    # Экономим деньги: если дата совпадает с сохраненной и юзер не пишет про время -> пропускаем
                    if stored_date == interview_date and not has_time_keywords:
                        ctx_logger.debug("Дата совпадает с сохраненной и нет новых триггеров. Пропуск аудита.")
                        run_audit = False
                    elif stored_date == interview_date: 
                        ctx_logger.info("Дата совпадает, но найдены временные триггеры. ПРИНУДИТЕЛЬНЫЙ АУДИТ.")

                    if run_audit:
                        ctx_logger.info(f"🔍 Запуск аудита даты: {interview_date}")
                        full_hist = (dialogue.history or [])
                        calendar_ctx = self._generate_calendar_context_2() 
                        
                        verified_date, audit_reason = await self._verify_date_audit(db, dialogue, interview_date, full_hist, calendar_ctx, ctx_logger.extra) 
                        ctx_logger.info(verified_date, ' ОБЪЯСНЕНИЕ МОДЕЛИ ', audit_reason)
                        # Если аудитор не согласен
                        if verified_date != interview_date and verified_date != "none":
                            ctx_logger.warning(f"🚨 ГАЛЛЮЦИНАЦИЯ ДАТЫ! LLM: {interview_date}, Аудитор: {verified_date}")
                            
                            # Вычисляем день недели для сообщения
                            try:
                                v_date_obj = datetime.datetime.strptime(verified_date, '%Y-%m-%d')
                                weekdays_ru = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
                                v_weekday = weekdays_ru[v_date_obj.weekday()]
                            except:
                                v_weekday = "указанный день"

                            await mq.publish("tg_alerts", {
                                "type": "hallucination",
                                "dialogue_id": dialogue.id,
                                "external_chat_id": dialogue.external_chat_id,
                                "user_said": combined_masked_message, # Что написал юзер последним
                                "llm_suggested": interview_date,      # Что придумал бот
                                "corrected_val": verified_date,       # Как исправил аудитор
                                "reasoning": audit_reason,            # Обоснование от GPT-4o
                                "history_text": self._get_history_as_text(dialogue) # Текст истории
                            })

                            correction_msg = (
                                f"[SYSTEM COMMAND] В прошлом шаге ты ошибся и предложил дату {interview_date}. "
                                f"На самом деле пользователь выбрал {v_weekday} ({verified_date}) согласно календарю. "
                                f"Сгенерируй ответ заново, подтвердив ПРАВИЛЬНУЮ дату ({v_weekday}, {verified_date}). "
                                f"ОБЯЗАТЕЛЬНО обнови поле 'interview_date' в JSON на '{verified_date}'. Если запись на этот день недоступна, то предложи выбрать другой день."
                            )
                            
                            sys_msg = {
                                "role": "user", 
                                "content": correction_msg, 
                                "message_id": f"sys_audit_{time.time()}",
                                "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
                            }
                            
                            # Сохраняем и перезапускаем
                            dialogue.history = (dialogue.history or []) + [sys_msg]
                            # В HH мы клали user_entries_to_history в pending, но здесь pending нет, поэтому пишем сразу в историю
                            # И важно обновить last_message_at, чтобы не потеряться
                            dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
                            await db.commit()
                            
                            
                            await mq.publish("engine_tasks", {"dialogue_id": dialogue.id, "trigger": "system_audit_retry"})
                            ctx_logger.info(f"♻️ Отправлено на исправление даты ({v_weekday}).")
                            return 

                        # Если аудитор подтвердил или исправил на валидную дату
                        if verified_date != "none":
                            interview_date = verified_date

                    if interview_date:
                        # --- ШАГ 2: ПОДСКАЗКА ДНЯ НЕДЕЛИ И РЕГЛАМЕНТА (HINT) ---
                        # Если дата подтверждена, проверяем, что на неё есть в Google Таблице
                        try:
                            # 1. Получаем текущее время в МСК для сравнения (логика "Сегодня")
                            now_msk = datetime.datetime.now(MOSCOW_TZ)
                            today_str = now_msk.strftime('%Y-%m-%d')
                            current_hour = now_msk.hour

                            # 2. Запрашиваем РЕАЛЬНЫЕ свободные слоты из Google Sheets
                            
                            available_slots = await sheets_service.get_available_slots(interview_date)

                            # 3. Применяем фильтрацию для "Сегодня" (как в HH)
                            if interview_date == today_str:
                                # Оставляем только те слоты, которые минимум на 1 час позже текущего времени
                                available_slots = [s for s in available_slots if int(s.split(':')[0]) > current_hour]

                            # 4. Вычисляем день недели для текста команды
                            v_date_obj = datetime.datetime.strptime(interview_date, '%Y-%m-%d')
                            weekday_idx = v_date_obj.weekday()
                            weekdays_ru = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
                            correct_weekday = weekdays_ru[weekday_idx]

                            # 5. Формируем текст инструкции (Системная команда)
                            hint_content = None

                            if weekday_idx == 6: # Воскресенье (даже если в таблице есть строки, мы их игнорим по логике HH)
                                hint_content = (
                                    f"[SYSTEM COMMAND] Внимание!!! {interview_date} это {correct_weekday}!!! "
                                    f"По воскресеньям собеседования не проводятся. Запись невозможна. Предложи другой день."
                                )
                            elif not available_slots: # Если в таблице нет слотов "Свободно" или на сегодня всё вышло
                                if interview_date == today_str:
                                    time_now = now_msk.strftime('%H:%M')
                                    hint_content = (
                                        f"[SYSTEM COMMAND] Внимание!!! На сегодня ({interview_date}) запись уже окончена "
                                        f"(сейчас {time_now}). Предложи кандидату выбрать другой день (завтра или ближайший будний)."
                                    )
                                else:
                                    hint_content = (
                                        f"[SYSTEM COMMAND] Внимание!!! На {interview_date} ({correct_weekday}) нет свободных мест "
                                        f"в графике. Ты ОБЯЗАНА сообщить об этом и предложить выбрать любой другой свободный день."
                                    )
                            else:
                                # Если слоты есть, даем боту их список (как в HH)
                                slots_str = ", ".join(available_slots)
                                hint_content = (
                                    f"[SYSTEM COMMAND] Внимание!!! На {interview_date} ({correct_weekday}) строго разрешены "
                                    f"только следующие слоты: {slots_str}. "
                                    f"Ты ОБЯЗАНА перечислить ВСЕ эти варианты ({slots_str}) в своем ответе, "
                                    f"чтобы кандидат мог выбрать один из них."
                                )

                            # 6. Проверяем историю на дубли (анти-луп из HH)
                            history_to_check = (dialogue.history or [])[-5:]
                            already_hinted = any(hint_content == m.get('content') for m in history_to_check)

                            if hint_content and not already_hinted:
                                ctx_logger.info(f"[{dialogue.external_chat_id}] Добавляю регламент из Google Sheets для {interview_date}")
                                
                                hint_cmd = {
                                    'message_id': f'sys_hint_{time.time()}',
                                    'role': 'user',
                                    'content': hint_content,
                                    'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()
                                }
                                
                                # Сохраняем и вызываем перегенерацию
                                dialogue.history = (dialogue.history or []) + [hint_cmd]
                                await db.commit()
                                
                                
                                await mq.publish("engine_tasks", {"dialogue_id": dialogue.id, "trigger": "slot_hint_retry"})
                                return 

                        except Exception as e:
                            ctx_logger.error(f"Ошибка в этапе Hint (Google Sheets): {e}")
                            await mq.publish("tg_alerts", {
                                "type": "system",
                                "text": f"🚨 **СБОЙ GOOGLE SHEETS:** Не удалось получить слоты для диалога `{dialogue.id}`. Проверьте таблицу!",
                                "alert_type": "admin_only"
                            })
                            # Здесь я бы советовал делать raise e, чтобы задача ушла в ретрай, 
                            # если тебе важно, чтобы бот видел регламент
                            raise e

            # =====================================================================
            # [START] ШАГ 3: ЖЕСТКАЯ ВАЛИДАЦИЯ ВРЕМЕНИ (TIME ENFORCEMENT)
            # =====================================================================
            if new_state in DATE_CRITICAL_STATES and interview_date and interview_time:
                try:
                    # 1. Получаем свежий список слотов для этой даты
                    available_slots = await sheets_service.get_available_slots(interview_date)
                    
                    # 2. Фильтр "Сегодня"
                    now_msk = datetime.datetime.now(MOSCOW_TZ)
                    if interview_date == now_msk.strftime('%Y-%m-%d'):
                        available_slots = [s for s in available_slots if int(s.split(':')[0]) > now_msk.hour]

                    # 3. СРАВНЕНИЕ: Проверяем, входит ли время от LLM в список разрешенных
                    clean_time = interview_time.strip()
                    
                    if clean_time not in available_slots:
                        ctx_logger.warning(f"🚨 МОДЕЛЬ ВЫБРАЛА ЗАНЯТОЕ ВРЕМЯ! Выбрано: {clean_time}, Свободно: {available_slots}")

                        error_msg = f"На дату {interview_date} доступно только время: {', '.join(available_slots)}. Слот {clean_time} недоступен или уже занят."
                        
                        time_corr_cmd = {
                            'message_id': f'sys_time_corr_{time.time()}',
                            'role': 'user',
                            'content': (
                                f"[SYSTEM COMMAND] {error_msg} Предложи выбрать из реально свободных слотов: "
                                f"{', '.join(available_slots) if available_slots else 'другой день'}. "
                                f"ОБЯЗАТЕЛЬНО обнови поле 'interview_time' в JSON на null."
                            ),
                            'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()
                        }

                        dialogue.history = (dialogue.history or []) + [time_corr_cmd]
                        await db.commit()
                        
                       
                        await mq.publish("engine_tasks", {"dialogue_id": dialogue.id, "trigger": "time_enforce_retry"})
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


                

                # --- 13.1 ОБРАБОТКА ДАТЫ РОЖДЕНИЯ ---
                raw_birth_date = extracted_data.get("birth_date")
                if raw_birth_date:
                    allowed_age_states = ['awaiting_age', 'clarifying_anything']
                    
                    if current_state_at_update in allowed_age_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("birth_date"):
                            ctx_logger.debug(f"Защита: поле birth_date уже заполнено, пропускаем")
                        else:
                            calculated_age = self._calculate_age(raw_birth_date)
                            if calculated_age is not None:
                                profile["birth_date"] = raw_birth_date
                                profile["age"] = calculated_age # Для фильтра 30-55
                                changed = True
                                ctx_logger.info(f"✅ Дата рождения {raw_birth_date} принята. Возраст: {calculated_age}")
                            else:
                                ctx_logger.warning(f"⚠️ Некорректный формат даты от LLM: {raw_birth_date}")

                # --- 13.2 ОБРАБОТКА ГРАЖДАНСТВА (Специфика: Только РФ vs Остальные) ---
                raw_citizenship = extracted_data.get("citizenship")
                if raw_citizenship:
                    allowed_cit_states = ['awaiting_citizenship', 'clarifying_citizenship', 'clarifying_anything']
                    
                    if current_state_at_update in allowed_cit_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("citizenship"):
                            ctx_logger.debug(f"Защита: поле citizenship уже заполнено, пропускаем")
                        else:
                            
                            cit_low = str(raw_citizenship).lower()
                            
                            # Простая проверка на РФ
                            is_rf = any(x in cit_low for x in ["россия", "рф", "российская", "russia"])
                            
                            if is_rf:
                                profile["citizenship"] = "РФ"
                                changed = True
                            else:
                                # Это иностранец. Пишем как есть.
                                profile["citizenship"] = raw_citizenship
                                changed = True
                                
                                # Проверяем, есть ли уже информация о патенте
                                has_patent_info = extracted_data.get("has_patent")
                                
                                # Если патента нет в extracted_data и мы не в режиме уточнения
                                if not has_patent_info and current_state_at_update != 'clarifying_citizenship':
                                    ctx_logger.info(f"🌍 Гражданство '{raw_citizenship}' (не РФ). Требуется уточнение патента.")
                                    
                                    # Формируем команду на уточнение
                                    correction_msg = (
                                        f"[SYSTEM COMMAND] Кандидат сообщил гражданство {raw_citizenship} (не РФ). "
                                        f"Ты ОБЯЗАНА уточнить, есть ли у него действующий патент для работы. "
                                        f"Установи стейт 'clarifying_citizenship' и задай этот вопрос."
                                    )
                                    
                                    sys_msg = {
                                        "role": "user", 
                                        "content": correction_msg, 
                                        "message_id": f"sys_cit_check_{time.time()}",
                                        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
                                    }
                                    
                                    # Сохраняем профиль (гражданство мы записали) и уходим на ретрай
                                    dialogue.candidate.profile_data = profile


                                    dialogue.history = (dialogue.history or []) + [sys_msg]
                                    dialogue.current_state = "clarifying_citizenship" # Форсируем стейт
                                    await db.commit()
                                    
                                    
                                    await mq.publish("engine_tasks", {"dialogue_id": dialogue.id, "trigger": "citizenship_refine"})
                                    return

                    else:
                        ctx_logger.debug(f"Игнорируем гражданство {raw_citizenship}: стейт {current_state_at_update} не разрешает.")
                
                # Записываем ответ про патент, если он пришел (обычно в стейте clarifying_citizenship)
                if extracted_data.get("has_patent"):
                    # Добавь ту же проверку, что и для гражданства!
                    allowed_cit_states = ['awaiting_citizenship', 'clarifying_citizenship', 'clarifying_anything']
                    
                    if current_state_at_update in allowed_cit_states:
                        if current_state_at_update == 'clarifying_anything' and profile.get("has_patent"):
                            ctx_logger.debug("Защита: патент уже есть, не перезаписываем")
                        else:
                            profile["has_patent"] = extracted_data["has_patent"]
                            changed = True
                    else:
                        ctx_logger.debug(f"Игнорируем патент: стейт {current_state_at_update} не разрешает.")

                # --- 13.3 ОСТАЛЬНЫЕ ПОЛЯ (Маппинг стейтов как в HH) ---
                
                # ФИО и Телефон (Колонки) - пишем всегда, если их нет (защита от перезаписи)
                if extracted_data.get("full_name") and not dialogue.candidate.full_name:
                    dialogue.candidate.full_name = extracted_data["full_name"]
                
                if extracted_data.get("phone") and not dialogue.candidate.phone_number:
                    dialogue.candidate.phone_number = extracted_data["phone"]

                # Остальные поля (JSONB) - строго по стейтам
                mapping = {
                    "city": ["awaiting_city", "clarifying_anything"],
                    "experience": ["awaiting_experience", "clarifying_anything"],
                    "readiness_date": ["awaiting_readiness", "clarifying_anything"],
                    "has_medbook": ["awaiting_medbook", "clarifying_anything"],
                    "criminal_record": ["awaiting_criminal", "clarifying_anything"]
                }
                
                for field_key, allowed_states in mapping.items():
                    val = extracted_data.get(field_key)
                    if val:
                        if current_state_at_update in allowed_states:
                            # ДОБАВИТЬ ЭТУ ПРОВЕРКУ:
                            if current_state_at_update == 'clarifying_anything' and profile.get(field_key):
                                ctx_logger.debug(f"Защита: {field_key} уже заполнено, пропускаем")
                                continue # Пропускаем запись этого поля
                                
                            profile[field_key] = val
                            changed = True
                        else:
                             ctx_logger.debug(f"Игнорируем {field_key}='{val}': стейт {current_state_at_update} не разрешает.")
                if changed:
                    dialogue.candidate.profile_data = profile
                    is_ok = True 
                    reason = None
                    # ПРОВЕРЯЕМ НА ОТКАЗ ТОЛЬКО ЕСЛИ МЫ ЕЩЕ НЕ В ПРОЦЕССЕ ЗАПИСИ
                    SCHEDULING_STATES = ['init_scheduling_spb', 'scheduling_spb_day', 'scheduling_spb_time', 'interview_scheduled_spb']
                    
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




            # === 14. БЛОК КВАЛИФИКАЦИИ И ПРИНЯТИЯ РЕШЕНИЙ ===

            # ==========================================================================================
            # БЛОК ВАЛИДАЦИИ И ПРИНЯТИЯ РЕШЕНИЙ
            # ==========================================================================================
            
            # Проверяем условия, только если LLM пытается завершить анкету (new_state == 'qualification_complete')
            if dialogue.status not in ['qualified', 'rejected'] and new_state == 'qualification_complete':
                
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
                        
                        
                        await mq.publish("engine_tasks", {"dialogue_id": dialogue.id, "trigger": "force_phone_retry"})
                        return

                # --- 14.2 ПРОВЕРКА ПОЛНОТЫ АНКЕТЫ (Динамический LLM Recovery) ---
                profile = dialogue.candidate.profile_data or {}
                
                # 1. Собираем карту только РЕАЛЬНО отсутствующих данных
                missing_data_map = {}
                
                if not dialogue.candidate.phone_number: 
                    missing_data_map["phone"] = "Номер телефона"
                if not profile.get("birth_date"): 
                    missing_data_map["birth_date"] = "Полная дата рождения (день, месяц, год)"
                if not profile.get("citizenship"): 
                    missing_data_map["citizenship"] = "Гражданство собеседника(страна)"
                if not profile.get("experience"): 
                    missing_data_map["experience"] = "Опыт работы (описание, или отсутствие опыта просто 'нет' тогда поставь)"
                if not profile.get("readiness_date"): 
                    missing_data_map["readiness_date"] = "Когда готов выйти на работу (вахту)"
                if not profile.get("has_medbook"): 
                    missing_data_map["has_medbook"] = "Наличие медкнижки (да/нет)"
                if not profile.get("criminal_record"): 
                    missing_data_map["criminal_record"] = "Судимость (<Описание судимости. Если это преступление против личности (убийство, разбой, насилие, тяжкие телесные), верни строго 'violent'. Если судимости нет, верни 'нет’. В остальных случаях опиши кратко (например, 'экономическая').>)"

                # Проверка патента для иностранцев
                cit_val = str(profile.get("citizenship", "")).lower()
                is_rf = any(x in cit_val for x in ["россия", "рф", "российская", "russia"])
                if profile.get("citizenship") and not is_rf and not profile.get("has_patent"):
                    missing_data_map["has_patent"] = "Наличие патента (да/нет)"

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
                    # Пример: "age": <значение или null>, "experience": <значение или null>
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
                        # Используем Smart-модель (gpt-4o) для высокой точности экстракции
                        recovery_response = await get_bot_response(
                            system_prompt=recovery_prompt,
                            dialogue_history=[],
                            user_message=f"ИСТОРИЯ ДИАЛОГА ДЛЯ АНАЛИЗА:\n{recent_history_text}",
                            attempt_tracker=recovery_attempts,
                            extra_context=ctx_logger.extra
                        )

                        if recovery_response:
                            # Логируем стоимость и токены (включая скрытые ретраи)
                            await self._log_llm_usage(db, dialogue, "Data_Recovery_Audit", recovery_response.get("usage_stats"), model_name="gpt-4o-mini")
                            
                            extracted_data = recovery_response.get('parsed_response', {})
                            is_profile_updated = False

                            # Обрабатываем то, что удалось спасти
                            for key in list(missing_data_map.keys()):
                                val = extracted_data.get(key)
                                if val is not None and str(val).lower() != 'null':
                                    if key == "phone":
                                        dialogue.candidate.phone_number = str(val)
                                        ctx_logger.info(f"✨ Recovery спас телефон: {val}")
                                    elif key == "birth_date":
                                        calculated_age = self._calculate_age(str(val))
                                        if calculated_age is not None: # БЫЛО: if calculated_age
                                            profile["birth_date"] = str(val)
                                            profile["age"] = calculated_age
                                            is_profile_updated = True
                                            ctx_logger.info(f"✨ Recovery спас дату рождения: {val}")
                                    else:
                                        profile[key] = val
                                        ctx_logger.info(f"✨ Recovery спас поле {key}: {val}")
                                    
                                    # Удаляем из списка "недостающих", чтобы бот не спрашивал
                                    missing_data_map.pop(key)
                                    is_profile_updated = True

                            if is_profile_updated:
                                dialogue.candidate.profile_data = profile
                                await db.flush()

                    except Exception as e:
                        ctx_logger.error(f"❌ Ошибка в блоке Recovery: {e}")

                # 5. ФИНАЛЬНЫЙ ВЕРДИКТ: Если данные все еще нужны
                if missing_data_map:
                    missing_human_names = ", ".join(missing_data_map.values())
                    ctx_logger.warning(f"⚠️ Recovery не помог. Не хватает: {missing_human_names}")
                    
                    sys_cmd_content = (
                        f"[SYSTEM COMMAND] Анкета не завершена. Тебе НЕОБХОДИМО уточнить следующие данные: {missing_human_names}. "
                        f"Прямо сейчас задай вопрос кандидату, чтобы узнать эти сведения. "
                        f"Используй стейт clarifying_anything для уточнения этих сведений"
                        f"ЗАПРЕЩЕНО переходить в 'qualification_complete', пока эти поля пусты."
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
                    
                    await mq.publish("engine_tasks", {"dialogue_id": dialogue.id, "trigger": "data_fix_retry"})
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

                    ПРАВИЛА ГРАЖДАНСТВА:
                    1. Если Россия (РФ, Российская федерация) -> в "citizenship" верни "РФ".
                    2. Если любая другая страна -> в "citizenship" верни название страны.

                    Правило даты рождения:
                    1. в "birth_date" верни ПОЛНУЮ дату рождения в формате YYYY-MM-DD. 
                       Если кандидат назвал только возраст, верни null.

                    ПРАВИЛА СУДИМОСТИ:
                    1. В "criminal_record" верни "нет" — если нет судимости или она экономическая.
                    2. В "criminal_record" верни "violent" — если преступление против личности (убийство, насилие, разбой, тяжкие телесные)

                    Верни ответ ТОЛЬКО в формате JSON:
                    {
                        "birth_date": "YYYY-MM-DD или null",
                        "citizenship": "<строка>",
                        "has_patent": "<да/нет/none>",
                        "criminal_record": "<нет / против личности>",
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
                        v_birth_date = v_data.get('birth_date')
                        v_cit = v_data.get('citizenship')
                        v_patent = v_data.get('has_patent')
                        v_criminal = v_data.get('criminal_record')

                        # Сравниваем аудит с тем, что у нас в БД
                        db_birth_date = profile.get("birth_date")
                        db_cit = profile.get("citizenship")
                        db_patent = profile.get("has_patent")

                        if v_birth_date is not None or v_cit is not None:
                            # Логика сравнения
                            is_age_ok = (db_birth_date == v_birth_date)
                            is_cit_ok = (str(db_cit).lower() == str(v_cit).lower())
                            
                        if not is_age_ok or not is_cit_ok:
                            ctx_logger.warning(f"🚨 РАССИНХРОН АУДИТА! БД: {db_birth_date}/{db_cit}, Аудит: {v_birth_date}/{v_cit}")
                            # Отправляем алерт верификации
                            await mq.publish("tg_alerts", {
                                "type": "verification",
                                "dialogue_id": dialogue.id,
                                "external_chat_id": dialogue.external_chat_id,
                                "db_data": {
                                    "birth_date": db_birth_date, 
                                    "citizenship": db_cit, 
                                    "patent": profile.get("has_patent")
                                },
                                "llm_data": {
                                    "birth_date": v_birth_date, 
                                    "citizenship": v_cit, 
                                    "patent": v_patent
                                },
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
                        "trigger": "start_scheduling_trigger"
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
                            await sheets_service.release_slot(old_date, old_time)
                            
                            # 2. ПРЯМОЕ ДЕЙСТВИЕ: Занимаем новый слот
                            await sheets_service.book_slot(
                                target_date=interview_date, 
                                target_time=interview_time, 
                                candidate_name=dialogue.candidate.full_name or "Кандидат"
                            )
                            
                            # 3. Пушим задачу на уведомление в RabbitMQ
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

                            # Обновляем метаданные
                            meta["interview_date"] = interview_date
                            meta["interview_time"] = interview_time
                            dialogue.metadata_json = meta
                            
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

                # 1. ПРЯМОЕ ДЕЙСТВИЕ: Занимаем слот в Google Таблице
                if meta["interview_date"] and meta["interview_time"]:
                    await sheets_service.book_slot(
                        target_date=meta["interview_date"],
                        target_time=meta["interview_time"],
                        candidate_name=dialogue.candidate.full_name or "Кандидат"
                    )
                    # Планируем напоминания в БД
                    await self._schedule_interview_reminders(db, dialogue, meta["interview_date"], meta["interview_time"])

                # 2. Аналитика
                await log_event(db, dialogue, 'qualified', check_duplicates=True)

                # 3. Пушим задачу на карточку в ТГ и запись в Таблицу Кандидатов
                await mq.publish("services_output", {
                    "dialogue_id": dialogue.id, 
                    "type": "qualified"
                })

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
                        
                        
                        await mq.publish("engine_tasks", {"dialogue_id": dialogue.id, "trigger": "decline_veto_retry"})
                        return 

                # --- 16.2 ОТМЕНА В КАЛЕНДАРЕ ---
                meta = dialogue.metadata_json or {}
                if meta.get("interview_date") and meta.get("interview_time"):
                    ctx_logger.info(f"🗑️ ОТМЕНА: Освобождаю слот {meta.get('interview_date')} {meta.get('interview_time')}")
                    
                    # 1. ПРЯМОЕ ДЕЙСТВИЕ: Освобождаем слот
                    await sheets_service.release_slot(meta.get("interview_date"), meta.get("interview_time"))
                    
                    # 2. Пушим задачу воркеру (например, отправить карточку отмены)
                    await mq.publish("services_output", {
                        "dialogue_id": dialogue.id,
                        "type": "cancelled"
                    })

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
                
                ctx_logger.info(f"Диалог завершен со статусом REJECTED (Тип: {stat_event_type}, Состояние: {new_state})")


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