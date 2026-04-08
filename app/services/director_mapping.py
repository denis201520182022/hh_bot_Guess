# app/services/director_mapping.py
"""
Сопоставление адресов вакансий с директорами.

ФОРМАТ АДРЕСА: берётся из HH API → full_raw_data.address.raw
Пример: "Санкт-Петербург, Невский проспект, 114-116"

КАК ДОБАВИТЬ НОВОГО ДИРЕКТОРА:
1. Убедись что директор есть в БД (таблица directors)
2. Добавь запись в DIRECTOR_ADDRESS_MAP
   - Ключ: подстрока из address.raw (регистронезависимое вхождение)
   - Значение: имя директора (должно точно совпадать с Director.name в БД)

ПРИОРИТЕТ:
  Сопоставление идёт по ПОЛНОМУ СОВПАДЕНИЮ ключа в address.raw.
  Если несколько ключей могут подойти — берётся ПЕРВЫЙ найденный (по порядку в словаре).
  Для точных матчей (конкретный дом) ставь их ПЕРЕД общими (улица, город).
"""

from typing import Optional


# === МАППИНГ АДРЕС → ИМЯ ДИРЕКТОРА ===
# КЛЮЧ: фрагмент address.raw (case-insensitive поиск)
# ЗНАЧЕНИЕ: имя директора (точно как в БД, поле Director.name)
DIRECTOR_ADDRESS_MAP: dict[str, str] = {
    # === САНКТ-ПЕТЕРБУРГ ===
    # Пример:
    # "санкт-петербург, невский проспект, 114-116": "Иванов Иван Иванович",

    # === МОСКВА ===
    # "москва, тверская улица, 10": "Петрова Мария Сергеевна",
}


def resolve_director_name(address_raw: str) -> Optional[str]:
    """
    Определяет имя директора по сырой строке адреса.

    Args:
        address_raw: raw адрес из HH (например "Санкт-Петербург, Невский проспект, 114-116")

    Returns:
        Имя директора или None если не найден
    """
    if not address_raw:
        return None

    address_lower = address_raw.lower()

    for address_key, director_name in DIRECTOR_ADDRESS_MAP.items():
        if address_key.lower() in address_lower:
            return director_name

    return None
