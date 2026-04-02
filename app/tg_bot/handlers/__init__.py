# tg_bot/handlers/__init__.py

from aiogram import Router
from . import common, admin, user

# Создаем главный роутер
router = Router()

# Порядок регистрации важен:
# 1. common - обычно содержит общие команды (/start, /help) и отмену состояний (FSM)
# 2. admin - команды, доступные только админам
# 3. user - команды для остальных (или fallback)

router.include_router(common.router)
router.include_router(admin.router)
router.include_router(user.router)