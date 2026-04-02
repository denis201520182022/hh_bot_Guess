# tg_bot/handlers/common.py

import logging
from datetime import date, datetime, timedelta

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, cast, Date, select
import io
import pandas as pd
from datetime import date, datetime, timedelta
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.fsm.context import FSMContext
from sqlalchemy import select, cast, Date, func
from sqlalchemy.orm import selectinload
from app.db.models import Dialogue, AnalyticsEvent, Account, JobContext
from datetime import date, timedelta
from aiogram.utils.formatting import Text, Bold, Italic
from app.db.models import AnalyticsEvent

from app.db.models import TelegramUser
from app.tg_bot.keyboards import (
    user_keyboard, 
    admin_keyboard, 
    stats_main_menu_keyboard, 
    export_date_options_keyboard,
    back_to_stats_main_keyboard
)
import io
import pandas as pd
from datetime import date, datetime
from aiogram.types import Message, BufferedInputFile
from aiogram.fsm.context import FSMContext
from sqlalchemy import select, cast, Date, func, and_, case, Integer
from sqlalchemy.orm import selectinload
from app.db.models import Dialogue, AnalyticsEvent, Account, JobContext
logger = logging.getLogger(__name__)
router = Router()

# Состояния для FSM (экспорт Excel)
class ExportStates(StatesGroup):
    waiting_for_range = State() 



async def _build_7day_stats_content(session: AsyncSession) -> Text:
    content_parts = [
        Bold("📊 Статистика откликов за 7 дней:"), "\n", 
        Italic("(по дате прихода кандидата)"), "\n\n"
    ]
    
    # 1. Формируем список последних 7 дней
    today = date.today()
    days = [today - timedelta(days=i) for i in range(7)]
    has_any_data = False

    for day in days:
        # 2. ГЛУБОКИЙ ЗАПРОС: 
        # Берем все диалоги, созданные в конкретный день.
        # С помощью агрегации CASE/MAX проверяем наличие событий аналитики для каждого диалога.
        # Это гарантирует, что один отклик попадет только в ОДНУ (высшую по приоритету) категорию.
        
        stmt = (
            select(
                Dialogue.id,
                # Приоритет 1: Успех (записан на собес или подходит)
                func.max(case((AnalyticsEvent.event_type == 'qualified', 1), else_=0)).label('is_qual'),
                
                # Приоритет 2: Отказ (ботом или самим кандидатом)
                func.max(case((AnalyticsEvent.event_type.in_(['rejected_by_bot', 'rejected_by_candidate']), 1), else_=0)).label('is_rej'),
                
                # Приоритет 3: Молчун (завершил общение по таймауту)
                func.max(case((AnalyticsEvent.event_type == 'timed_out', 1), else_=0)).label('is_timeout')
            )
            .join(AnalyticsEvent, Dialogue.id == AnalyticsEvent.dialogue_id, isouter=True)
            .where(cast(Dialogue.created_at, Date) == day)
            .group_by(Dialogue.id)
        )
        
        result = await session.execute(stmt)
        day_rows = result.all()

        if day_rows:
            has_any_data = True
            
            total_leads = len(day_rows) # Общее кол-во откликов за день
            qual_count = 0              # Подошло
            rej_count = 0               # Отказы
            timeout_count = 0           # Молчуны
            in_work_count = 0           # В работе (нет финала и не уснул)

            for row in day_rows:
                # ЛОГИКА ПРИОРИТЕТОВ:
                if row.is_qual == 1:
                    qual_count += 1
                elif row.is_rej == 1:
                    rej_count += 1
                elif row.is_timeout == 1:
                    timeout_count += 1
                else:
                    # Если событий финала нет — значит кандидат еще в процессе общения
                    in_work_count += 1

            # Форматируем строку дня
            day_str = day.strftime('%d.%m (%a)')
            content_parts.extend([
                Bold(f"📅 {day_str}"), "\n",
                f"   Откликов: ", Bold(str(total_leads)), "\n",
                f"   - Подошло: {qual_count}\n",
                f"   - Отказов: {rej_count}\n",
                f"   - Молчуны: {timeout_count}\n",
                f"   - В работе: {in_work_count}\n",
                "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            ])

    if not has_any_data:
        return Text("📊 Данных за последние 7 дней не найдено.")

    return Text(*content_parts)

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ EXCEL ---

async def generate_and_send_excel(message: Message, start_date: date, end_date: date, session: AsyncSession, state: FSMContext):
    msg_wait = await message.answer("⏳ Формирую детальный аналитический отчет по событиям...")
    
    # 1. ГЛУБОКИЙ СБОР ДАННЫХ (Event Sourcing)
    # Собираем все диалоги за период и проверяем наличие событий в аналитике через CASE/MAX
    stmt = (
        select(
            Dialogue.id,
            Dialogue.created_at,
            Account.name.label('account_name'),
            JobContext.city.label('city'),
            JobContext.title.label('vacancy_title'),
            # Агрегируем флаги событий для каждого диалога
            func.max(case((AnalyticsEvent.event_type == 'first_contact', 1), else_=0)).label('has_contact'),
            func.max(case((AnalyticsEvent.event_type == 'qualified', 1), else_=0)).label('has_qual'),
            func.max(case((AnalyticsEvent.event_type == 'rejected_by_bot', 1), else_=0)).label('has_bot_rej'),
            func.max(case((AnalyticsEvent.event_type == 'rejected_by_candidate', 1), else_=0)).label('has_user_rej'),
            func.max(case((AnalyticsEvent.event_type == 'timed_out', 1), else_=0)).label('has_timeout')
        )
        .join(Account, Dialogue.account_id == Account.id)
        .join(JobContext, Dialogue.vacancy_id == JobContext.id)
        .join(AnalyticsEvent, Dialogue.id == AnalyticsEvent.dialogue_id, isouter=True)
        .where(
            and_(
                cast(Dialogue.created_at, Date) >= start_date,
                cast(Dialogue.created_at, Date) <= end_date
            )
        )
        .group_by(Dialogue.id, Account.name, JobContext.city, JobContext.title)
    )
    
    result = await session.execute(stmt)
    query_data = result.all()

    if not query_data:
        await msg_wait.edit_text("🤷 За этот период откликов не найдено.")
        await state.clear()
        return

    # 2. ПЕРВИЧНАЯ ГРУППИРОВКА (Сырые данные -> Статистика по ключам)
    report_map = {}
    for d in query_data:
        dt_str = d.created_at.strftime("%d.%m.%Y")
        acc = d.account_name or "Не указан"
        cit = d.city or "Не указан"
        vac = d.vacancy_title or "Не указана"
        key = (dt_str, acc, cit, vac)

        if key not in report_map:
            report_map[key] = {
                "отклики": 0, "вступили": 0, "собес": 0, 
                "отказали_мы": 0, "отказался_кд": 0, "молчуны": 0
            }
        
        m = report_map[key]
        m["отклики"] += 1
        
        # Считаем контакт (вступили в диалог)
        if d.has_contact: 
            m["вступили"] += 1
        
        # ЛОГИКА ПРИОРИТЕТОВ СТАТУСА (один диалог = одна категория в воронке)
        if d.has_qual:
            m["собес"] += 1
        elif d.has_user_rej:
            m["отказался_кд"] += 1
        elif d.has_bot_rej:
            m["отказали_мы"] += 1
        elif d.has_timeout and d.has_contact:
            # Молчун: начал диалог, но замолчал (и не имеет финала типа отказа/собеса)
            m["молчуны"] += 1

    # 3. ПОДГОТОВКА СТРОК ДЛЯ DATAFRAME
    rows = []
    for (dt, acc, cit, vac), m in report_map.items():
        rows.append({
            "Дата": dt, "Рекрутер": acc, "Город": cit, "Вакансия": vac,
            "Отклики": m["отклики"],
            "Не вступили": m["отклики"] - m["вступили"],
            "Начали диалог": m["вступили"],
            "Собес": m["собес"],
            "Отказался КД": m["отказался_кд"],
            "Отказали мы": m["отказали_мы"],
            "Молчуны": m["молчуны"],
            "Отказы всего": m["отказали_мы"] + m["отказался_кд"]
        })

    df_base = pd.DataFrame(rows)
    # Сортировка по дате для красоты
    df_base['dt_obj'] = pd.to_datetime(df_base['Дата'], format='%d.%m.%Y')
    df_base = df_base.sort_values(['dt_obj', 'Рекрутер']).drop(columns=['dt_obj'])

    # 4. ФУНКЦИЯ ДЛЯ СВОДНЫХ ТАБЛИЦ (Группировка + Конверсии)
    def create_summary(groupby_col):
        summary = df_base.groupby(groupby_col).agg({
            'Отклики': 'sum', 'Не вступили': 'sum', 'Начали диалог': 'sum', 
            'Собес': 'sum', 'Отказался КД': 'sum', 'Отказали мы': 'sum', 
            'Молчуны': 'sum', 'Отказы всего': 'sum'
        }).reset_index()

        # Расчет конверсий
        summary['Собес/отклик %'] = (summary['Собес'] / summary['Отклики']).fillna(0)
        summary['Молчуны/Диалог %'] = (summary['Молчуны'] / summary['Начали диалог']).fillna(0)
        summary['Отказы/Диалог %'] = (summary['Отказы всего'] / summary['Начали диалог']).fillna(0)

        # Итоговая строка
        total = summary.sum(numeric_only=True)
        total[groupby_col] = 'ИТОГО'
        
        # Пересчет % для ИТОГО
        t_resp = total['Отклики'] if total['Отклики'] > 0 else 1
        t_dial = total['Начали диалог'] if total['Начали диалог'] > 0 else 1
        total['Собес/отклик %'] = total['Собес'] / t_resp
        total['Молчуны/Диалог %'] = total['Молчуны'] / t_dial
        total['Отказы/Диалог %'] = total['Отказы всего'] / t_dial
        
        return pd.concat([summary, pd.DataFrame([total])], ignore_index=True)

    # Листы отчета
    df_date = create_summary('Дата')
    df_acc = create_summary('Рекрутер')
    df_city = create_summary('Город')
    df_vac = create_summary('Вакансия')

    # 5. СОХРАНЕНИЕ В EXCEL И СТИЛИЗАЦИЯ
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_date.to_excel(writer, index=False, sheet_name='Свод по датам')
        df_acc.to_excel(writer, index=False, sheet_name='Свод по рекрутерам')
        df_city.to_excel(writer, index=False, sheet_name='Свод по городам')
        df_vac.to_excel(writer, index=False, sheet_name='Свод по вакансиям')
        df_base.to_excel(writer, index=False, sheet_name='Общий отчет')

        workbook = writer.book
        # Стили
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9EAD3', 'border': 1, 'align': 'center'})
        total_fmt = workbook.add_format({'bold': True, 'bg_color': '#F4CCCC', 'border': 1, 'align': 'center'})
        perc_fmt = workbook.add_format({'num_format': '0%', 'border': 1, 'align': 'center'})
        num_fmt = workbook.add_format({'border': 1, 'align': 'center'})

        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            ws.freeze_panes(1, 0)
            
            # Определяем активный DF
            if sheet_name == 'Свод по датам': curr_df = df_date
            elif sheet_name == 'Свод по рекрутерам': curr_df = df_acc
            elif sheet_name == 'Свод по городам': curr_df = df_city
            elif sheet_name == 'Свод по вакансиям': curr_df = df_vac
            else: curr_df = df_base

            for i, col in enumerate(curr_df.columns):
                # Авто-ширина
                column_len = max(curr_df[col].astype(str).str.len().max(), len(str(col))) + 3
                
                if '%' in str(col):
                    ws.set_column(i, i, 18, perc_fmt)
                else:
                    ws.set_column(i, i, column_len, num_fmt)
                
                # Запись заголовка со стилем
                ws.write(0, i, col, header_fmt)

            # Формат строки ИТОГО (последняя строка)
            if sheet_name != 'Общий отчет':
                last_row_idx = len(curr_df) - 1
                for i in range(len(curr_df.columns)):
                    val = curr_df.iloc[last_row_idx, i]
                    # Если ячейка в итоговой строке процентная
                    is_perc = '%' in curr_df.columns[i]
                    cell_fmt = workbook.add_format({'bold': True, 'bg_color': '#F4CCCC', 'border': 1, 'num_format': '0%' if is_perc else '', 'align': 'center'})
                    ws.write(last_row_idx + 1, i, val, cell_fmt)

    output.seek(0)
    file_name = f"Report_{start_date}_to_{end_date}.xlsx"
    await message.answer_document(
        BufferedInputFile(output.read(), filename=file_name),
        caption=f"📈 Детальная аналитика откликов ({start_date} - {end_date})"
    )
    await msg_wait.delete()
    await state.clear()


@router.message(CommandStart())
async def handle_start(message: Message, session: AsyncSession):
    """
    Обработка команды /start. 
    Проверяет права доступа и выдает соответствующую клавиатуру.
    """
    if not message.from_user:
        return

    # Ищем пользователя в БД
    stmt = select(TelegramUser).where(TelegramUser.telegram_id == message.from_user.id)
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()

    if not user:
        await message.answer("❌ Нет доступа. Обратитесь к администратору.")
        return

    # Выбираем клавиатуру в зависимости от роли
    kb = admin_keyboard if user.role == 'admin' else user_keyboard
    
    await message.answer(
        f"👋 Привет, {message.from_user.first_name or 'Коллега'}!\n"
        f"Бот готов к работе.", 
        reply_markup=kb
    )




# --- МЕНЮ СТАТИСТИКИ ---

@router.message(F.text == "📊 Статистика")
async def stats_main_menu(message: Message):
    """Открывает меню выбора статистики"""
    await message.answer(
        "Выберите действие:", 
        reply_markup=stats_main_menu_keyboard
    )


@router.callback_query(F.data == "stats_back_to_main")
async def stats_back_to_main(callback: CallbackQuery):
    """Возврат в главное меню статистики (если будем делать вложенность)"""
    await callback.message.edit_text(
        "Выберите действие:", 
        reply_markup=stats_main_menu_keyboard
    )
    await callback.answer()




@router.callback_query(F.data == "view_stats_7days")
async def view_text_stats(callback: CallbackQuery, session: AsyncSession):
    """
    Выводит реальную текстовую статистику за 7 дней, 
    используя таблицу AnalyticsEvent.
    """
    # 1. Вызываем функцию подсчета (она вернет объект Text)
    content = await _build_7day_stats_content(session)
    
    # 2. Редактируем сообщение, распаковывая форматирование через as_kwargs()
    # parse_mode указывать не нужно, as_kwargs сам подставит нужный (HTML или MarkdownV2)
    await callback.message.edit_text(
        **content.as_kwargs(), 
        reply_markup=back_to_stats_main_keyboard
    )
    
    # 3. Отвечаем на колбэк, чтобы убрать "часики" в телеграме
    await callback.answer()



@router.callback_query(F.data == "export_excel_start")
async def export_start(callback: CallbackQuery, state: FSMContext):
    """Начало сценария выгрузки Excel"""
    await state.set_state(ExportStates.waiting_for_range)
    await callback.message.answer(
        "За какой период выгрузить данные?\n\n"
        "Выберите кнопку или пришлите диапазон вручную:\n"
        "<code>01.12.2025 - 15.12.2025</code>",
        reply_markup=export_date_options_keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(ExportStates.waiting_for_range, F.data.startswith("export_range_"))
async def export_range_quick(callback: CallbackQuery, state: FSMContext, session: AsyncSession):
    days_count = int(callback.data.split("_")[-1])
    end_date = date.today()
    start_date = end_date - timedelta(days=days_count-1)
    await generate_and_send_excel(callback.message, start_date, end_date, session, state)
    await callback.answer()

@router.message(ExportStates.waiting_for_range)
async def export_range_manual(message: Message, state: FSMContext, session: AsyncSession):
    try:
        parts = message.text.split("-")
        start_date = datetime.strptime(parts[0].strip(), "%d.%m.%Y").date()
        end_date = datetime.strptime(parts[1].strip(), "%d.%m.%Y").date()
        if (end_date - start_date).days > 60:
            await message.answer("❌ Ошибка: период не может превышать 60 дней.")
            return
        await generate_and_send_excel(message, start_date, end_date, session, state)
    except Exception:
        await message.answer("❌ Неверный формат. Пример: 01.12.2025 - 10.12.2025")


@router.callback_query(F.data == "cancel_fsm")
async def cancel_fsm(callback: CallbackQuery, state: FSMContext):
    """Отмена любого ввода"""
    await state.clear()
    await callback.message.delete()
    await callback.answer("❌ Отменено")