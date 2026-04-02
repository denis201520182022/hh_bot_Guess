# tg_bot/filters.py

from aiogram.filters import BaseFilter
from aiogram.types import Message
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import TelegramUser

class AdminFilter(BaseFilter):
    """
    Фильтр, проверяющий, является ли пользователь администратором.
    Требует наличия 'session' в аргументах (через Middleware).
    """
    async def __call__(self, message: Message, session: AsyncSession) -> bool:
        if not message.from_user:
            return False
            
        # Ищем пользователя в БД по ID
        stmt = select(TelegramUser).where(TelegramUser.telegram_id == message.from_user.id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()
        
        # Проверяем роль
        return user is not None and user.role == 'admin'