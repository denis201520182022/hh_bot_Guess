import sys
import os
import asyncio
import datetime
import uuid
import json

# Позволяет импортировать модули из app/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.db.models import Account, Dialogue, JobContext, Candidate, AppSettings
from app.core.rabbitmq import mq
from app.core.config import settings
import aio_pika


async def listen_bot_responses(chat_id: str):
    """Слушает ответы бота из RabbitMQ и выводит в консоль"""
    try:
        connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        channel = await connection.channel()
        queue = await channel.declare_queue("console_output", durable=True)
        
        async for message in queue:
            async with message.process():
                data = json.loads(message.body.decode())
                if data.get("chat_id") == chat_id:
                    print(f"\n🤖 [BOT]: {data.get('text')}")
                    print("Вы: ", end="", flush=True)
    except Exception as e:
        print(f"Ошибка слушателя: {e}")


async def setup_test_data(chat_id: str):
    """Инициализирует тестовые данные (Account, Vacancy, Candidate, Dialogue) в БД"""
    async with AsyncSessionLocal() as db:
        # Настройка биллинга, если нет
        app_settings = await db.scalar(select(AppSettings).filter_by(id=1))
        if not app_settings:
            app_settings = AppSettings(id=1, balance=100000.0)
            db.add(app_settings)
        else:
            if app_settings.balance < 1000:
                app_settings.balance = 100000.0
        
        # 1. Аккаунт
        account = await db.scalar(select(Account).filter_by(platform="console"))
        if not account:
            account = Account(name="Test Console", platform="console", is_active=True, auth_data={})
            db.add(account)
            await db.flush()

        # 2. Вакансия
        job = await db.scalar(select(JobContext).filter_by(account_id=account.id, external_id="console_vac_1"))
        if not job:
            job = JobContext(
                account_id=account.id, 
                external_id="console_vac_1",
                is_active=True,
                title="Тестовая вакансия",
                city="Тестоград",
                description_data={"description_text": "Крутая вакансия для тестирования"}
            )
            db.add(job)
            await db.flush()

        # 3. Кандидат
        candidate = await db.scalar(select(Candidate).filter_by(platform_user_id="console_user_1"))
        if not candidate:
            candidate = Candidate(
                platform_user_id="console_user_1",
                full_name="Тестер Консолевич",
                profile_data={"age": "25", "city": "Тестоград"}
            )
            db.add(candidate)
            await db.flush()

        # 4. Диалог
        dialogue = await db.scalar(select(Dialogue).filter_by(external_chat_id=chat_id))
        if not dialogue:
            dialogue = Dialogue(
                external_chat_id=chat_id,
                account_id=account.id,
                candidate_id=candidate.id,
                vacancy_id=job.id,
                status='new',
                current_state='initial',
                history=[]
            )
            db.add(dialogue)
            
        await db.commit()
        return account, job, candidate, dialogue


async def chat_loop(chat_id: str):
    """Интерактивный цикл чата"""
    print("\nВводите сообщения. Для выхода введите 'quit' или 'exit'.\n")
    
    while True:
        text = await asyncio.to_thread(input, "Вы: ")
        
        # Очистка от битых суррогатных символов (которые может вернуть консоль Windows)
        import re
        text = re.sub(r'[\ud800-\udfff]', '', text)
        
        if text.strip().lower() in ['quit', 'exit']:
            print("Завершение работы...")
            # Выход из программы
            os._exit(0)
            
        if not text.strip():
            continue

        async with AsyncSessionLocal() as db:
            dialogue = await db.scalar(select(Dialogue).filter_by(external_chat_id=chat_id))
            if not dialogue:
                print("Диалог не найден!")
                continue

            # Добавляем сообщение в историю
            new_msg = {
                "message_id": str(uuid.uuid4()),
                "role": "user",
                "content": text,
                "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            
            history = list(dialogue.history or [])
            history.append(new_msg)
            dialogue.history = history
            dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
            await db.commit()

            # Отправляем триггер в engine
            await mq.publish("engine_tasks", {
                "dialogue_id": dialogue.id,
                "trigger": "console_test"
            })
            
            # Ждем ответ... выведет слушатель `listen_bot_responses`


async def main():
    chat_id = "console_chat_" + str(uuid.uuid4())[:8]
    print(f"🚀 Инициализация тестовой сессии: {chat_id}")
    
    await mq.connect()
    
    try:
        await setup_test_data(chat_id)
        
        # Запускаем фоновый слушатель ответов
        asyncio.create_task(listen_bot_responses(chat_id))
        
        # Запускаем ввод пользователя
        await chat_loop(chat_id)
        
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        await mq.close()


if __name__ == "__main__":
    asyncio.run(main())
