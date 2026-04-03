# tg_bot/handlers/admin.py

import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified
from aiogram.utils.formatting import Text, Bold, Italic, Code
import io
import datetime
from aiogram.types import BufferedInputFile
from sqlalchemy.orm.attributes import flag_modified
from app.db.models import (
    Account, 
    JobContext, 
    Candidate, 
    SearchStatus,
    SearchStat
)
from sqlalchemy.orm import selectinload, joinedload 
from aiogram.utils.keyboard import InlineKeyboardBuilder
from app.db.models import TelegramUser, Account, AppSettings, Dialogue
from app.tg_bot.filters import AdminFilter
from app.tg_bot.keyboards import (
    create_management_keyboard,
    role_choice_keyboard,
    cancel_fsm_keyboard,
    limits_menu_keyboard,
    admin_keyboard,
    platform_choice_keyboard
)

logger = logging.getLogger(__name__)
router = Router()

# Применяем фильтр админа на весь роутер
router.message.filter(AdminFilter())
router.callback_query.filter(AdminFilter())

# --- FSM Состояния ---

class UserManagement(StatesGroup):
    add_id = State()
    add_name = State()
    add_role = State()
    del_id = State()

class AccountManagement(StatesGroup):
    """Управление аккаунтами (универсально: Avito + HH)"""
    # Общие состояния
    add_platform = State()
    add_name = State()
    add_tg_chat_id = State()
    
    # Для Avito
    add_avito_client_id = State()
    add_avito_client_secret = State()
    
    # Для HH
    add_hh_manager_id = State()
    add_hh_refresh_token = State()
    add_hh_access_token = State()
    add_hh_expires_in = State()
    
    # Обновление
    update_id = State()
    update_name = State()
    update_tg_chat_id = State()
    update_avito_client_id = State()
    update_avito_client_secret = State()
    update_hh_manager_id = State()
    update_hh_refresh_token = State()
    update_hh_access_token = State()
    update_hh_expires_in = State()
    del_id = State()

class SettingsManagement(StatesGroup):
    set_balance = State()
    set_cost_dialogue = State()
    set_search_balance = State()

# --- Обработчики отмены ---

@router.message(Command("cancel"))
@router.message(F.text.casefold() == "отмена")
async def cancel_command_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Нет активных действий для отмены.")
        return
    await state.clear()
    await message.answer("Действие отменено.", reply_markup=admin_keyboard)

@router.callback_query(F.data == "cancel_fsm")
async def cancel_callback_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Действие отменено.")
    await callback.answer()

# --- УПРАВЛЕНИЕ БАЛАНСОМ И ТАРИФАМИ ---


# tg_bot/handlers/admin.py (ВАШ ФАЙЛ)

# 1. Основное меню Баланса и Тарифов
@router.message(F.text == "⚙️ Баланс и Тариф")
async def limits_menu(message: Message, session: AsyncSession):
    settings = await session.get(AppSettings, 1)
    if not settings:
        await message.answer("❌ Не удалось загрузить настройки.")
        return

    # Краткий статус поиска для раздела "Поиск резюме"
    status_stmt = select(SearchStatus).options(joinedload(SearchStatus.account))
    statuses = (await session.execute(status_stmt)).scalars().all()

    search_status_text = ""
    if statuses:
        for s in statuses:
            icon = "🟢 ВКЛ" if s.is_enabled else "🔴 ВЫКЛ"
            search_status_text += f"• {s.account.name}: {icon}\n"
    else:
        search_status_text = "<i>Аккаунты не настроены</i>"

    stats = settings.stats or {}
    costs = settings.costs or {}

    content = Text(
        Bold("📊 Баланс и Тарифы"), "\n\n",
        "Текущий баланс: ", Bold(f"{settings.balance:.2f}"), " руб.\n",
        "💰 ", Bold("Тарифы:"), "\n",
        "Новый диалог:  ", Bold(f"{costs.get('dialogue', 0):.2f}"), " руб.\n\n",

        Bold("🔍 Режим поиска резюме: "), "\n",
        search_status_text, "\n",
        
        "🔔 Порог когда придет оповещение о низком балансе: ", Bold(f"{settings.low_balance_threshold:.2f}"), " руб."
    )

    # Клавиатура с тремя кнопками
    kb = InlineKeyboardBuilder()
    kb.button(text="⚙️ Установить баланс", callback_data="set_limit")
    kb.button(text="💰 Установить тарифы", callback_data="set_tariff")
    kb.button(text="🔎 Управление поиском", callback_data="manage_search")
    kb.adjust(1)
    
    await message.answer(**content.as_kwargs(), reply_markup=kb.as_markup())


# 2. Меню управления поиском (вызывается кнопкой "🔎 Управление поиском")
@router.callback_query(F.data == "manage_search")
async def manage_search_menu(callback: CallbackQuery, session: AsyncSession):
    # Получаем все аккаунты и их статусы
    # Сначала найдем все аккаунты Авито
    acc_stmt = select(Account).where(Account.platform == 'avito')
    accounts = (await session.execute(acc_stmt)).scalars().all()
    
    # Получаем существующие статусы
    status_stmt = select(SearchStatus)
    statuses = (await session.execute(status_stmt)).scalars().all()
    status_map = {s.account_id: s.is_enabled for s in statuses}

    sheet_url = "https://docs.google.com/spreadsheets/d/1njb64bZ2mT0S7lQgSGFRN5HD9fzMKnJ--6u-kWIC7es/edit#gid=77096499"
    
    text = (
        "<b>🔎 Управление поиском резюме</b>\n\n"
        f"📍 Настроить параметры и квоты можно в <a href='{sheet_url}'>Google Таблице</a>\n\n"
        "Нажмите на кнопку аккаунта, чтобы включить или выключить поиск:"
    )

    kb = InlineKeyboardBuilder()
    for acc in accounts:
        is_on = status_map.get(acc.id, False)
        btn_icon = "✅" if is_on else "❌"
        kb.button(text=f"{btn_icon} {acc.name}", callback_data=f"toggle_search_{acc.id}")
    
    kb.button(text="⬅️ Назад", callback_data="back_to_limits")
    kb.adjust(1)

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup(), disable_web_page_preview=True)
    await callback.answer()


# 3. Обработчик переключения (рубильник)
@router.callback_query(F.data.startswith("toggle_search_"))
async def toggle_search_status(callback: CallbackQuery, session: AsyncSession):
    acc_id = int(callback.data.split("_")[-1])
    
    # Ищем статус в БД
    stmt = select(SearchStatus).filter_by(account_id=acc_id)
    status = (await session.execute(stmt)).scalar_one_or_none()
    
    if not status:
        # Если записи нет — создаем (по умолчанию выключен, нажатие включает)
        status = SearchStatus(account_id=acc_id, is_enabled=True)
        session.add(status)
    else:
        # Инвертируем состояние
        status.is_enabled = not status.is_enabled
    
    await session.commit()
    
    # Обновляем это же меню, чтобы кнопка сразу изменилась
    await manage_search_menu(callback, session)
    await callback.answer("Статус поиска изменен")


# 4. Возврат в основное меню
@router.callback_query(F.data == "back_to_limits")
async def back_to_limits(callback: CallbackQuery, session: AsyncSession):
    # Просто вызываем функцию основного меню, но передаем callback.message
    await callback.message.delete() # Удаляем старое, чтобы не дублировать
    await limits_menu(callback.message, session)
    await callback.answer()



@router.callback_query(F.data == "set_limit")
async def start_set_balance(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SettingsManagement.set_balance)
    await callback.message.answer("Введите новую сумму общего баланса в рублях (например: 5000):")
    await callback.answer()

@router.message(SettingsManagement.set_balance)
async def process_set_balance(message: Message, state: FSMContext, session: AsyncSession):
    try:
        new_balance = float(message.text.replace(',', '.'))
        if new_balance < 0: raise ValueError
    except (ValueError, TypeError):
        await message.answer("❌ Сумма должна быть числом. Попробуйте еще раз.")
        return

    stmt = select(AppSettings).where(AppSettings.id == 1)
    result = await session.execute(stmt)
    settings = result.scalar_one()
    
    settings.balance = new_balance
    
    if new_balance >= settings.low_balance_threshold:
        settings.low_limit_notified = False

    await session.commit()
    await state.clear()
    await message.answer(f"✅ Баланс обновлен: {new_balance:.2f} руб.", reply_markup=admin_keyboard)

@router.callback_query(F.data == "set_tariff")
async def start_set_cost_dialogue(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SettingsManagement.set_cost_dialogue)
    await callback.message.answer("Введите стоимость создания ОДНОГО ДИАЛОГА (в рублях):")
    await callback.answer()

@router.message(SettingsManagement.set_cost_dialogue)
async def process_set_cost_dialogue(message: Message, state: FSMContext, session: AsyncSession):
    try:
        val = float(message.text.replace(',', '.'))
        
        stmt = select(AppSettings).where(AppSettings.id == 1)
        result = await session.execute(stmt)
        settings = result.scalar_one()
        
        # Обновляем значение в JSONB словаре
        if settings.costs is None: settings.costs = {}
        settings.costs["dialogue"] = val
        
        # Сообщаем SQLAlchemy, что JSON поле изменилось
        flag_modified(settings, "costs")
        
        await session.commit()
        await state.clear()
        await message.answer(f"✅ Тариф обновлен: {val:.2f} руб. за диалог.", reply_markup=admin_keyboard)
    except Exception as e:
        logger.error(f"Ошибка при смене тарифа: {e}")
        await message.answer("❌ Ошибка в числе. Попробуйте еще раз.")







# --- 1. УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ ---

@router.message(F.text == "👤 Управление пользователями")
async def user_management_menu(message: Message, session: AsyncSession):
    stmt = select(TelegramUser)
    result = await session.execute(stmt)
    users = result.scalars().all()
    
    content_parts = [Bold("👥 Список пользователей:"), "\n\n"]
    if not users:
        content_parts.append(Italic("В системе пока нет пользователей."))
    else:
        for u in users:
            role_emoji = "✨" if u.role == 'admin' else "🧑‍💻"
            content_parts.extend([
                f"{role_emoji} ", Bold(u.username), 
                " (ID: ", Code(str(u.telegram_id)), ") - Роль: ", Italic(u.role), "\n"
            ])
            
    content_parts.append("\nВыберите действие:")
    content = Text(*content_parts)
    
    # Клавиатура управления (кнопки "Добавить" и "Удалить")
    await message.answer(
        **content.as_kwargs(), 
        reply_markup=create_management_keyboard([], "add_user", "del_user")
    )

@router.callback_query(F.data == "add_user")
async def start_add_user(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserManagement.add_id)
    content = Text("Введите Telegram ID нового пользователя.")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
    await callback.answer()

@router.message(UserManagement.add_id)
async def process_add_user_id(message: Message, state: FSMContext, session: AsyncSession):
    if not message.text or not message.text.isdigit():
        content = Text("❌ ID должен быть числом. Попробуйте еще раз.")
        await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
        return
    
    user_id = int(message.text)
    
    # Проверка на существование
    stmt = select(TelegramUser).where(TelegramUser.telegram_id == user_id)
    result = await session.execute(stmt)
    if result.scalar_one_or_none():
        content = Text("⚠️ Пользователь с ID ", Code(str(user_id)), " уже существует. Действие отменено.")
        await message.answer(**content.as_kwargs())
        await state.clear()
        return
        
    await state.update_data(user_id=user_id)
    await state.set_state(UserManagement.add_name)
    content = Text("Отлично. Теперь введите имя пользователя (например, ", Code("Иван Рекрутер"), ").")
    await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)

@router.message(UserManagement.add_name)
async def process_add_user_name(message: Message, state: FSMContext):
    if not message.text:
        content = Text("❌ Имя не может быть пустым. Попробуйте еще раз.")
        await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
        return
        
    await state.update_data(user_name=message.text)
    await state.set_state(UserManagement.add_role)
    await message.answer("Имя принято. Теперь выберите роль:", reply_markup=role_choice_keyboard)

@router.callback_query(UserManagement.add_role)
async def process_add_user_role(callback: CallbackQuery, state: FSMContext, session: AsyncSession):
    role = "admin" if callback.data == "set_role_admin" else "user"
    user_data = await state.get_data()
    
    new_user = TelegramUser(
        telegram_id=user_data['user_id'], 
        username=user_data['user_name'], 
        role=role
    )
    session.add(new_user)
    await session.commit()
    
    await state.clear()
    logger.info(f"Админ {callback.from_user.id} добавил пользователя {user_data['user_id']} с ролью {role}")
    
    content = Text(
        "✅ ", Bold("Успех!"), " Пользователь ", 
        Bold(user_data['user_name']), " добавлен с ролью ", Italic(role), "."
    )
    await callback.message.edit_text(**content.as_kwargs())

@router.callback_query(F.data == "del_user")
async def start_del_user(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserManagement.del_id)
    content = Text("Введите Telegram ID пользователя для удаления.")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
    await callback.answer()

@router.message(UserManagement.del_id)
async def process_del_user_id(message: Message, state: FSMContext, session: AsyncSession):
    if not message.text or not message.text.isdigit():
        content = Text("❌ ID должен быть числом. Попробуйте еще раз.")
        await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
        return
        
    user_id_to_delete = int(message.text)
    
    # Не даем удалить самого себя
    if message.from_user.id == user_id_to_delete:
        await message.answer("🤔 Вы не можете удалить самого себя. Действие отменено.")
        await state.clear()
        return
        
    stmt = select(TelegramUser).where(TelegramUser.telegram_id == user_id_to_delete)
    result = await session.execute(stmt)
    user_to_delete = result.scalar_one_or_none()
    
    if not user_to_delete:
        content = Text("⚠️ Пользователь с ID ", Code(str(user_id_to_delete)), " не найден. Действие отменено.")
        await message.answer(**content.as_kwargs())
        await state.clear()
        return
        
    deleted_username = user_to_delete.username
    deleted_id = user_to_delete.telegram_id
    
    await session.delete(user_to_delete)
    await session.commit()
    
    await state.clear()
    logger.info(f"Админ {message.from_user.id} удалил пользователя {deleted_id}")
    
    content = Text("✅ Пользователь ", Bold(deleted_username), " (ID: ", Code(str(deleted_id)), ") был удален.")
    await message.answer(**content.as_kwargs())


# --- 3. УПРАВЛЕНИЕ АККАУНТАМИ (UNIVERSAL: AVITO + HH) ---

@router.message(F.text == "🏢 Управление аккаунтами")
async def account_management_menu(message: Message, session: AsyncSession):
    """Список подключенных аккаунтов (Avito + HH)"""
    stmt = select(Account)
    result = await session.execute(stmt)
    accounts = result.scalars().all()

    content_parts = [Bold("🏢 Подключенные аккаунты:"), "\n\n"]
    if not accounts:
        content_parts.append(Italic("Список пуст. Добавьте свой первый аккаунт."))
    else:
        for acc in accounts:
            platform_icon = "🟠" if acc.platform == "avito" else "🔴" if acc.platform == "hh" else "⚪"
            content_parts.extend(["- ", Bold(f"{platform_icon} {acc.name}"),
                                  f" (Платформа: {acc.platform})",
                                  " ID: ", Code(str(acc.id)), "\n"])

    content_parts.append("\nВыберите действие:")
    content = Text(*content_parts)

    await message.answer(
        **content.as_kwargs(),
        reply_markup=create_management_keyboard([], "add_account", "del_account")
    )

# --- ДОБАВЛЕНИЕ АККАУНТА ---

@router.callback_query(F.data == "add_account")
async def start_add_account(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AccountManagement.add_platform)
    content = Text("Шаг 1/6: Выберите платформу для аккаунта.")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=platform_choice_keyboard)
    await callback.answer()

@router.callback_query(AccountManagement.add_platform, F.data.startswith("set_platform_"))
async def process_add_acc_platform(callback: CallbackQuery, state: FSMContext):
    platform = callback.data.split("_")[-1]
    await state.update_data(platform=platform)
    await state.set_state(AccountManagement.add_name)
    platform_name = "Avito 🟠" if platform == "avito" else "HeadHunter 🔴"
    content = Text(f"Шаг 2/6: Введите название аккаунта (например: ", Italic(f"{platform_name} Основной"), ").")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
    await callback.answer()

@router.message(AccountManagement.add_name)
async def process_add_acc_name(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Название не может быть пустым.")
        return
    await state.update_data(name=message.text)
    
    data = await state.get_data()
    platform = data.get('platform', 'avito')
    
    if platform == 'avito':
        await state.set_state(AccountManagement.add_avito_client_id)
        await message.answer("Шаг 3/6: Введите ваш Avito ", Bold("Client ID"), ".")
    else:  # hh
        await state.set_state(AccountManagement.add_hh_manager_id)
        await message.answer("Шаг 3/6: Введите ID рекрутера (manager id) с hh.ru.")

# ========== AVITO: Client ID ==========
@router.message(AccountManagement.add_avito_client_id)
async def process_add_avito_client_id(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Client ID не может быть пустым.")
        return
    await state.update_data(avito_client_id=message.text.strip())
    await state.set_state(AccountManagement.add_avito_client_secret)
    await message.answer("Шаг 4/6: Введите ваш Avito ", Bold("Client Secret"), ".")

@router.message(AccountManagement.add_avito_client_secret)
async def process_add_avito_client_secret(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Client Secret не может быть пустым.")
        return
    await state.update_data(avito_client_secret=message.text.strip())
    await state.set_state(AccountManagement.add_tg_chat_id)
    await message.answer("Шаг 5/6: Введите ID Telegram-чата для уведомлений (начинается с -100...).")

# ========== HH: Manager ID, Tokens ==========
@router.message(AccountManagement.add_hh_manager_id)
async def process_add_hh_manager_id(message: Message, state: FSMContext):
    if not message.text or not message.text.isdigit():
        await message.answer("❌ ID должен быть числом. Попробуйте еще раз.")
        return
    await state.update_data(hh_manager_id=message.text.strip())
    await state.set_state(AccountManagement.add_hh_refresh_token)
    await message.answer("Шаг 4/6: Введите REFRESH TOKEN, полученный от hh.ru.")

@router.message(AccountManagement.add_hh_refresh_token)
async def process_add_hh_refresh_token(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Токен не может быть пустым.")
        return
    await state.update_data(hh_refresh_token=message.text.strip())
    await state.set_state(AccountManagement.add_hh_access_token)
    await message.answer("Шаг 5/6: Введите ACCESS TOKEN.")

@router.message(AccountManagement.add_hh_access_token)
async def process_add_hh_access_token(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Токен не может быть пустым.")
        return
    await state.update_data(hh_access_token=message.text.strip())
    await state.set_state(AccountManagement.add_hh_expires_in)
    await message.answer("Шаг 6/6: Введите время жизни токена в секундах (expires_in), например 7200.")

@router.message(AccountManagement.add_hh_expires_in)
async def process_add_hh_expires_in(message: Message, state: FSMContext):
    if not message.text or not message.text.isdigit():
        await message.answer("❌ Время жизни должно быть числом. Попробуйте еще раз.")
        return
    await state.update_data(hh_expires_in=int(message.text))
    await state.set_state(AccountManagement.add_tg_chat_id)
    await message.answer("Шаг 7/7: Введите ID Telegram-чата для уведомлений (начинается с -100...).")

# ========== ОБЩИЙ ФИНАЛ: Telegram Chat ID ==========
@router.message(AccountManagement.add_tg_chat_id)
async def process_add_tg_chat_final(message: Message, state: FSMContext, session: AsyncSession):
    chat_id_str = message.text.strip()
    if not chat_id_str.lstrip('-').isdigit():
        await message.answer("❌ ID чата должен быть числом.")
        return

    if not chat_id_str.startswith('-'):
        chat_id_str = f"-{chat_id_str}"

    data = await state.get_data()
    platform = data.get('platform', 'avito')

    # Формируем auth_data в зависимости от платформы
    if platform == 'avito':
        auth_data = {
            "client_id": data['avito_client_id'],
            "client_secret": data['avito_client_secret'],
            "access_token": None,
            "refresh_token": None
        }
    else:  # hh
        expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=data['hh_expires_in'])
        auth_data = {
            "manager_id": data['hh_manager_id'],
            "access_token": data['hh_access_token'],
            "refresh_token": data['hh_refresh_token'],
            "expires_at": expires_at.isoformat()
        }

    # Создаем новый аккаунт
    new_account = Account(
        platform=platform,
        name=data['name'],
        auth_data=auth_data,
        settings={
            "tg_chat_id": int(chat_id_str)
        },
        is_active=True
    )

    session.add(new_account)
    await session.commit()
    await state.clear()

    logger.info(f"Админ {message.from_user.id} добавил аккаунт {platform}: {data['name']}")
    await message.answer(f"✅ Аккаунт <b>{data['name']}</b> ({platform}) успешно добавлен!", parse_mode="HTML", reply_markup=admin_keyboard)





# --- УДАЛЕНИЕ АККАУНТА ---

@router.callback_query(F.data == "del_account")
async def start_del_account(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AccountManagement.del_id)
    content = Text("Введите ID аккаунта (внутренний) для удаления из списка.")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
    await callback.answer()

@router.message(AccountManagement.del_id)
async def process_del_account_id(message: Message, state: FSMContext, session: AsyncSession):
    if not message.text or not message.text.isdigit():
        content = Text("❌ ID должен быть числом. Попробуйте еще раз.")
        await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
        return

    acc_id = int(message.text)
    
    # Поиск аккаунта
    stmt = select(Account).where(Account.id == acc_id)
    result = await session.execute(stmt)
    account_to_delete = result.scalar_one_or_none()

    if not account_to_delete:
        content = Text("⚠️ Аккаунт с ID ", Code(str(acc_id)), " не найден. Действие отменено.")
        await message.answer(**content.as_kwargs())
        await state.clear()
        return

    deleted_name = account_to_delete.name
    await session.delete(account_to_delete)
    await session.commit()
    
    await state.clear()
    logger.info(f"Админ {message.from_user.id} удалил аккаунт {acc_id}")

    content = Text("✅ Аккаунт ", Bold(deleted_name), " (ID: ", Code(str(acc_id)), ") удален.")
    await message.answer(**content.as_kwargs())


# --- ПОТАЙНАЯ КОМАНДА ДЛЯ ПОЛНОЙ ИСТОРИИ (UNIVERSAL) ---

@router.message(Command("dump_dialogue"))
async def secret_dump_handler(message: Message, session: AsyncSession):
    """
    Выгружает сырой лог диалога из БД в текстовый файл.
    Использование: /dump_dialogue [external_chat_id]
    """
    # Замените ID на свой, если нужно ограничить доступ
    # if message.from_user.id != 1975808643:
    #     return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: `/dump_dialogue [external_chat_id]`", parse_mode="Markdown")
        return

    chat_id = args[1]

    # Ищем диалог по external_chat_id (avito_chat_id или hh_response_id)
    stmt = select(Dialogue).where(Dialogue.external_chat_id == chat_id)
    result = await session.execute(stmt)
    dialogue = result.scalar_one_or_none()

    if not dialogue:
        await message.answer(f"❌ Диалог с ID `{chat_id}` не найден.")
        return

    # Определяем платформу для заголовка
    platform = dialogue.account.platform if dialogue.account else "unknown"
    platform_name = "AVITO" if platform == "avito" else "HH" if platform == "hh" else "UNKNOWN"

    # Формируем текстовый лог
    log_content = []
    log_content.append(f"=== ПОЛНЫЙ ДАМП ДИАЛОГА ({platform_name}) ===")
    log_content.append(f"External Chat ID: {dialogue.external_chat_id}")
    log_content.append(f"Platform: {platform}")
    log_content.append(f"Status: {dialogue.status}")
    log_content.append(f"Current State: {dialogue.current_state}")
    log_content.append(f"Created At: {dialogue.created_at}")

    stats = dialogue.usage_stats or {}
    log_content.append(f"Usage: {stats.get('tokens', 0)} tokens (${stats.get('total_cost', 0)})")
    log_content.append("="*35 + "\n")

    # 1. Обрабатываем основную историю из JSONB
    history = dialogue.history or []
    for msg in history:
        role = str(msg.get('role', 'unknown')).upper()
        # В авито-боте мы обычно храним время в timestamp_utc
        ts = msg.get('timestamp_utc', 'no_time')
        content = msg.get('content', '')

        # Инфо о состоянии, если оно сохранялось в историю
        state_info = f" [State: {msg.get('state')}]" if msg.get('state') else ""

        log_content.append(f"[{ts}] [{role}]{state_info}")
        log_content.append(f"TEXT: {content}")

        # Если есть извлеченные данные (результат скрининга)
        ext = msg.get('extracted_data')
        if ext:
            log_content.append(f"EXTRACTED DATA: {ext}")

        log_content.append("-" * 25)

    # 2. Метаданные диалога (результаты квалификации)
    meta = dialogue.metadata_json or {}
    if meta:
        log_content.append("\n" + "#"*10 + " METADATA " + "#"*10)
        for key, val in meta.items():
            log_content.append(f"{key}: {val}")

    final_text = "\n".join(log_content)

    # Создаем и отправляем файл
    file_buffer = io.BytesIO(final_text.encode('utf-8'))
    input_file = BufferedInputFile(file_buffer.getvalue(), filename=f"dump_dialogue_{chat_id}.txt")

    await message.answer_document(
        document=input_file,
        caption=f"📄 Полный дамп диалога `{chat_id}` ({platform_name})"
    )