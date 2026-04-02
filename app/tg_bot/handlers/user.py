# tg_bot/handlers/user.py

from aiogram import Router, F
from aiogram.types import Message
from aiogram.utils.formatting import Text, Bold
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AppSettings
from app.tg_bot.filters import AdminFilter

router = Router()

# Применяем фильтр: хендлеры сработают, только если юзер НЕ админ
router.message.filter(~AdminFilter()) 


@router.message(F.text == "⚙️ Баланс и Тариф")
async def user_balance_status(message: Message, session: AsyncSession):
    """
    Показывает баланс и краткую статистику (без напоминаний/фолоу-апов).
    """
    # Делаем выборку настроек (предполагаем, что запись с id=1 всегда существует)
    stmt = select(AppSettings).where(AppSettings.id == 1)
    result = await session.execute(stmt)
    settings = result.scalar_one_or_none()
    
    if not settings:
        await message.answer("❌ Не удалось загрузить данные о балансе (нет записи id=1).")
        return
        
    # В новой БД статистика и цены лежат в JSONB полях
    stats = settings.stats or {}
    costs = settings.costs or {}
    
    content = Text(
        Bold("💰 Текущий баланс системы:"), "\n\n",
        "Доступно: ", Bold(f"{settings.balance:.2f}"), " руб.\n\n",
        
        Bold("📈 Статистика расходов:"), "\n",
        "- Всего потрачено: ", Bold(f"{stats.get('total_spent', 0):.2f}"), " руб.\n",
        "- На диалоги: ", Bold(f"{stats.get('spent_on_dialogues', 0):.2f}"), " руб.\n\n",
        
        "ℹ️ ", Bold("Тарифы:"), "\n",
        "- Обработка нового отклика: ", Bold(f"{costs.get('dialogue', 0):.2f}"), " руб."
    )
    
    await message.answer(**content.as_kwargs())