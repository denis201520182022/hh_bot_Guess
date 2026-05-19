import sys
import os
import asyncio
import datetime
import logging
import argparse

# Позволяет импортировать модули из app/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import AsyncSessionLocal
from app.services.talantix_service import talantix_service

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_talantix_meeting")

async def create_meeting(person_id: int, vacancy_id: int, date_str: str, time_str: str, manager_id: int = None):
    """Только создание встречи в календаре"""
    logger.info(f"📅 Попытка создания встречи: person={person_id}, vacancy={vacancy_id}, date={date_str} {time_str}")

    # 1. Конвертируем в timestamp (MSK)
    try:
        moscow_tz = datetime.timezone(datetime.timedelta(hours=3))
        dt_naive = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        dt_msk = dt_naive.replace(tzinfo=moscow_tz)
        start_ts = int(dt_msk.timestamp() * 1000)
        end_ts = start_ts + 30 * 60 * 1000 # 30 минут
    except ValueError as e:
        logger.error(f"❌ Ошибка формата даты/времени: {e}")
        return

    # 2. Подготовка менеджеров
    manager_ids = [manager_id] if manager_id else []
    
    if not manager_ids:
        logger.info("🔍 manager_id не указан, пробую найти первого доступного ADMIN...")
        async with AsyncSessionLocal() as db:
            from app.db.models import Account
            from sqlalchemy import select
            account = await db.execute(select(Account).where(Account.platform == "talantix_calend", Account.is_active == True))
            acc = account.scalar_one_or_none()
            if acc:
                all_managers = await talantix_service.calend_client.get_managers(roles=["ADMIN"], account=acc, db=db)
                if all_managers:
                    manager_ids = [all_managers[0]["id"]]
                    logger.info(f"✅ Найден менеджер: {manager_ids[0]}")

    if not manager_ids:
        logger.error("❌ Не удалось найти ни одного менеджера для создания встречи")
        return

    # 3. Вызов сервиса бронирования
    meeting_id = await talantix_service.book_interview(
        start_date=start_ts,
        end_date=end_ts,
        person_ids=[person_id],
        vacancy_ids=[vacancy_id],
        manager_ids=manager_ids,
        title="Тестовое собеседование",
        comment="Создано через прямой тестовый скрипт"
    )

    if meeting_id:
        logger.info(f"🚀 УСПЕХ: Встреча создана! meeting_id: {meeting_id}")
        print(f"\nMEETING_ID: {meeting_id}\n")
    else:
        logger.error("❌ Ошибка: Встреча не была создана (проверьте логи сервиса)")

async def cancel_meeting(meeting_id: int):
    """Только отмена встречи в календаре"""
    logger.info(f"🗑️ Попытка отмены встречи: meeting_id={meeting_id}")
    
    success = await talantix_service.release_interview(meeting_id)
    
    if success:
        logger.info(f"✅ УСПЕХ: Встреча {meeting_id} отменена")
    else:
        logger.error(f"❌ Ошибка: Не удалось отменить встречу {meeting_id}")

async def main():
    parser = argparse.ArgumentParser(description="Прямое управление встречами в Talantix")
    subparsers = parser.add_subparsers(dest="command", help="Команда: create или cancel")

    # Команда create
    create_parser = subparsers.add_parser("create", help="Создать встречу")
    create_parser.add_argument("--person", type=int, required=True, help="ID кандидата (person_id)")
    create_parser.add_argument("--vacancy", type=int, required=True, help="ID вакансии")
    create_parser.add_argument("--date", required=True, help="Дата (YYYY-MM-DD)")
    create_parser.add_argument("--time", required=True, help="Время (HH:MM)")
    create_parser.add_argument("--manager", type=int, help="ID менеджера (необязательно)")

    # Команда cancel
    cancel_parser = subparsers.add_parser("cancel", help="Отменить встречу")
    cancel_parser.add_argument("--id", type=int, required=True, help="ID встречи (meeting_id)")

    args = parser.parse_args()

    if args.command == "create":
        await create_meeting(args.person, args.vacancy, args.date, args.time, args.manager)
    elif args.command == "cancel":
        await cancel_meeting(args.id)
    else:
        parser.print_help()

if __name__ == "__main__":
    asyncio.run(main())
