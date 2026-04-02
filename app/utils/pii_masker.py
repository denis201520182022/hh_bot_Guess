import re
from typing import Tuple, Optional

# --- РЕГУЛЯРНЫЕ ВЫРАЖЕНИЯ ---
# Паттерн для поиска ФИО (Фамилия Имя Отчество с большой буквы)
FIO_PATTERN = re.compile(
    r'\b([А-ЯЁ][а-яё]+(?:-[А-ЯЁ][а-яё]+)?)\s+([А-ЯЁ][а-яё]+)\s+(([А-ЯЁ][а-яё]+))?\b'
)

# Паттерн для поиска телефонов
PHONE_PATTERN = re.compile(
    r'(?:\+7|8)?[ \-.(]*(\d{3})[ \-.)]*(\d{3})[ \-.]*(\d{2})[ \-.]*(\d{2})\b'
)

# Токены маскировки
FIO_MASK_TOKEN = "[ФИО ЗАМАСКИРОВАНО]"
PHONE_MASK_TOKEN = "[ТЕЛЕФОН ЗАМАСКИРОВАН]"

# --- СПИСОК ИСКЛЮЧЕНИЙ (Географические и гос. названия) ---
# Эти фразы НЕ будут восприниматься как ФИО, даже если подходят под паттерн
FIO_EXCLUSIONS = {
    "российская федерация",
    "российская феберация", # Учел твою опечатку на всякий случай
    "беларусская республика",
    "белорусская республика",
    "беларуская республика",
    "республика беларусь",
    "республика казахстан",
    "кыргызская республика",
    "луганская народная",
    "донецкая народная",
    "советский союз",
    "чеченская республика"
}


def extract_and_mask_pii(text: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Извлекает ФИО и номер телефона, а затем полностью заменяет их на токены в тексте.
    Возвращает: (замаскированный_текст, извлеченное_фио, извлеченный_телефон)
    """
    if not text:
        return "", None, None

    extracted_fio = None
    extracted_phone = None
    masked_text = text
    
    # --- 1. Извлечение и маскировка телефона ---
    phone_match = PHONE_PATTERN.search(masked_text)
    if phone_match:
        full_phone_digits = "".join(filter(str.isdigit, phone_match.group(0)))
        
        if len(full_phone_digits) == 11 and full_phone_digits.startswith('8'):
            extracted_phone = '7' + full_phone_digits[1:]
        elif len(full_phone_digits) == 10:
            extracted_phone = '7' + full_phone_digits
        else:
            extracted_phone = full_phone_digits

        masked_text = PHONE_PATTERN.sub(PHONE_MASK_TOKEN, masked_text, count=1)

    # --- 2. Извлечение и маскировка ФИО ---
    # Используем finditer, чтобы если первое совпадение — это "Россия", мы могли поискать ФИО дальше
    for fio_match in FIO_PATTERN.finditer(masked_text):
        potential_fio = fio_match.group(0).strip()
        
        # ПРОВЕРКА ИСКЛЮЧЕНИЙ
        if potential_fio.lower() in FIO_EXCLUSIONS:
            continue  # Пропускаем это совпадение, ищем дальше
            
        # Если не в списке исключений — маскируем
        extracted_fio = potential_fio
        masked_text = masked_text.replace(potential_fio, FIO_MASK_TOKEN, 1)
        break # Берем первое подходящее ФИО

    return masked_text, extracted_fio, extracted_phone