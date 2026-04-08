# app/output_chanels/telegram/tg_cards.py
import logging
import html
import datetime
from aiogram import Bot
from aiogram.types import BufferedInputFile
from app.db.models import Dialogue, Candidate, JobContext, Account, Director

logger = logging.getLogger("tg_cards")

def format_history_txt(dialogue: Dialogue, candidate: Candidate, vacancy: JobContext) -> str:
    """Формирует текстовый файл истории диалога"""
    lines = []
    lines.append(f"=== ИСТОРИЯ ДИАЛОГА ({dialogue.account.platform.upper()}) ===")
    lines.append(f"ID чата: {dialogue.external_chat_id}")
    lines.append(f"Кандидат: {candidate.full_name or 'Аноним'}")
    lines.append(f"Вакансия: {vacancy.title if vacancy else 'Не указана'}")
    lines.append(f"Дата создания отклика: {dialogue.created_at.strftime('%d.%m.%Y %H:%M')}")
    lines.append("-" * 50 + "\n")

    for entry in (dialogue.history or []):
        role = entry.get('role')
        content = entry.get('content', '')
        content_str = str(content)
        
        # ФИЛЬТР: Пропускаем пустые, системные команды [SYSTEM и мусор [Системное сообщение]
        if not content_str or content_str.startswith('[SYSTEM') or content_str.startswith('[Системное сообщение]'):
            continue
            
        ts = entry.get('timestamp_utc', '')
        if ts:
            try:
                dt = datetime.datetime.fromisoformat(ts.replace('Z', '+00:00'))
                # Конвертируем в МСК для файла (+3 часа)
                msk_dt = dt + datetime.timedelta(hours=3)
                ts_str = msk_dt.strftime('[%H:%M:%S] ')
            except: ts_str = ""
        else: ts_str = ""

        label = "👤 Кандидат" if role == 'user' else "🤖 Бот"
        lines.append(f"{ts_str}{label}: {content}\n")

    return "\n".join(lines)



async def send_tg_notification(bot: Bot, dialogue: Dialogue, candidate: Candidate, vacancy: JobContext, account: Account, director: Director = None, event_type: str = 'qualified'):
    """Логика формирования и отправки карточки в Telegram (HTML version)
    
    Args:
        event_type: тип события - 'qualified' (запись), 'cancelled' (отмена), 'rescheduled' (перезапись)
    """
    profile = candidate.profile_data or {}
    tg_settings = account.settings or {}
    
    # Приоритет: TG чат директора > TG чат аккаунта
    target_chat_id = None
    
    if director and director.tg_chat_id:
        target_chat_id = director.tg_chat_id
        logger.info(f"Отправляю карточку в чат директора '{director.name}' (ID: {target_chat_id})")
    else:
        # Fallback на старый tg_chat_id из Account.settings
        target_chat_id = tg_settings.get("tg_chat_id")
        if target_chat_id:
            logger.info(f"Отправляю карточку в чат аккаунта '{account.name}' (ID: {target_chat_id})")

    if not target_chat_id:
        logger.warning(f"Для диалога {dialogue.id} не найден TG чат (ни у директора, ни у аккаунта)")
        return False

    def esc(text):
        """Безопасное экранирование текста для HTML"""
        if text is None or text == "": 
            return "—"
        return html.escape(str(text))
    
    meta = dialogue.metadata_json or {}
    # 1. ОПРЕДЕЛЯЕМ ПЛАТФОРМУ И ССЫЛКУ
    platform_name = account.platform.upper() # 'AVITO' или 'HH'
    
    if account.platform == "avito":
        chat_link = f"https://www.avito.ru/profile/messenger/channel/{dialogue.external_chat_id}"
        link_label = "Открыть чат"
    elif account.platform == "hh":
        # Извлекаем hh_resume_id из нашего составного ключа (помнишь, мы делали resumeid_vacancyid?)
        # Либо берем из profile_data, куда мы его заботливо положили в service.py
        resume_id = candidate.profile_data.get("hh_resume_id") or dialogue.candidate.platform_user_id.split('_')[0]
        chat_link = f"https://hh.ru/resume/{resume_id}"
        link_label = "Открыть резюме на HH"
    else:
        chat_link = "#"
        link_label = "Чат"

    # === ПЕРЕВОД ЗНАЧЕНИЙ НА РУССКИЙ ===
    emp_type = profile.get('employment_type')
    emp_label = "Полная" if emp_type == "full" else "Частичная" if emp_type == "part" else "—"
    
    # Готовность к часам (для подработки)
    hours = profile.get('ready_20_40_hours')
    hours_label = "✅ Да" if hours == "yes" else "❌ Нет" if hours == "no" else "—"
    
    # Смена (для полного дня)
    shift = profile.get('shift_preference')
    shift_map = {"morning": "🌅 Утро", "evening": "🌆 Вечер", "any": "🔄 Любая"}
    shift_label = shift_map.get(shift, "—")
    
    # ТК РФ
    contract = profile.get('employment_contract_ready')
    contract_label = "✅ Да" if contract == "yes" else "❌ Нет" if contract == "no" else "—"
    
    # Военный билет
    military = profile.get('has_military_document')
    military_label = "✅ Да" if military == "yes" else "❌ Нет" if military == "no" else "—"

    # 2. ФОРМИРУЕМ ЗАГОЛОВОК В ЗАВИСИМОСТИ ОТ ТИПА СОБЫТИЯ
    if event_type == 'cancelled':
        header = f"❌ <b>Собеседование отменено ({platform_name})</b>"
    elif event_type == 'rescheduled':
        header = f"🔄 <b>Собеседование перезарегистрировано ({platform_name})</b>"
    else:  # qualified
        header = f"🚀 <b>Новый кандидат ({platform_name})</b>"

    # 3. ФОРМИРУЕМ УНИВЕРСАЛЬНЫЙ ТЕКСТ
    message_text = (
        f"{header}\n\n"
        f"📌 <b>Вакансия:</b> {esc(vacancy.title if vacancy else 'Не указана')}\n"
        f"👤 <b>ФИО:</b> {esc(candidate.full_name)}\n"
        f"📞 <b>Телефон:</b> <code>{esc(candidate.phone_number)}</code>\n\n"

        f"🎂 <b>Возраст:</b> {esc(profile.get('age'))}\n"
        f"🌍 <b>Гражданство:</b> {esc(profile.get('citizenship'))}\n"
        f"⏳ <b>Занятость:</b> {esc(emp_label)}\n"
    )

    # Доп. поля в зависимости от типа занятости
    if emp_type == "part":
        message_text += f"⏱ <b>Готов 20-40ч:</b> {esc(hours_label)}\n"
    elif emp_type == "full":
        message_text += f"🕒 <b>Смена:</b> {esc(shift_label)}\n"

    # Добавляем информацию о собеседовании
    if event_type == 'cancelled':
        message_text += (
            f"\n� <b>Отмененное собеседование:</b> {esc(meta.get('interview_date'))} в {esc(meta.get('interview_time'))}\n"
        )
        if meta.get('cancel_reason'):
            message_text += f"📝 <b>Причина отмены:</b> {esc(meta.get('cancel_reason'))}\n"
    elif event_type == 'rescheduled':
        old_date = meta.get('old_interview_date')
        old_time = meta.get('old_interview_time')
        new_date = meta.get('interview_date')
        new_time = meta.get('interview_time')
        message_text += (
            f"\n📅 <b>Было:</b> {esc(old_date)} в {esc(old_time)}\n"
            f"📅 <b>Стало:</b> {esc(new_date)} в {esc(new_time)}\n"
        )
    else:  # qualified
        message_text += (
            f"\n📅 <b>Собеседование:</b> {esc(meta.get('interview_date'))} в {esc(meta.get('interview_time'))}\n"
        )

    message_text += f"\n🔗 <a href='{chat_link}'>{link_label}</a>"

    history_text = format_history_txt(dialogue, candidate, vacancy)
    file_name = f"chat_{dialogue.external_chat_id}.txt"
    document = BufferedInputFile(history_text.encode('utf-8'), filename=file_name)

    try:
        await bot.send_document(
            chat_id=target_chat_id,
            document=document,
            caption=message_text,
            parse_mode="HTML"  # Указываем HTML вместо MarkdownV2
        )
        logger.info(
            "✅ Карточка отправлена в TG",
            extra={
                "target_chat": target_chat_id,
                "candidate": candidate.full_name,
                "event_type": event_type
            }
        )
        return True
    except Exception as e:
        logger.exception(f"❌ Ошибка отправки карточки в TG: {e}")