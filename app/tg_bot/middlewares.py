# tg_bot/middlewares.py

from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from sqlalchemy.ext.asyncio import async_sessionmaker

class DbSessionMiddleware(BaseMiddleware):
    def __init__(self, session_pool: async_sessionmaker):
        super().__init__()
        self.session_pool = session_pool

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # Открываем асинхронную сессию
        async with self.session_pool() as session:
            # Кладем сессию в data с ключом "session".
            # Это позволит использовать аргумент session: AsyncSession в хендлерах.
            data["session"] = session
            return await handler(event, data)