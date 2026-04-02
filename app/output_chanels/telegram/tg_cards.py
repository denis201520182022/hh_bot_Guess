# app/output_chanels/telegram/tg_cards.py
import logging
import html
import datetime
from aiogram import Bot
from aiogram.types import BufferedInputFile
from app.db.models import Dialogue, Candidate, JobContext, Account

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



async def send_tg_notification(bot: Bot, dialogue: Dialogue, candidate: Candidate, vacancy: JobContext, account: Account):
    """Логика формирования и отправки карточки в Telegram (HTML version)"""
    profile = candidate.profile_data or {}
    tg_settings = account.settings or {}
    target_chat_id = tg_settings.get("tg_chat_id")

    if not target_chat_id:
        logger.warning(f"Для аккаунта {account.name} не настроен tg_chat_id.")
        return

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

    # 2. ФОРМИРУЕМ УНИВЕРСАЛЬНЫЙ ТЕКСТ
    message_text = (
        f"🚀 <b>Новый кандидат ({platform_name})</b>\n\n" # Теперь пишет (HH) или (AVITO)
        f"📌 <b>Вакансия:</b> {esc(vacancy.title if vacancy else 'Не указана')}\n"
        f"📍 <b>Город:</b> {esc(profile.get('city', 'Не указан'))}\n\n"
        f"👤 <b>ФИО:</b> {esc(candidate.full_name)}\n"
        f"📞 <b>Телефон:</b> <code>{esc(candidate.phone_number)}</code>\n"
        f"📅 <b>Дата рождения:</b> {esc(profile.get('birth_date'))}\n"
        f"🎂 <b>Возраст:</b> {esc(profile.get('age'))}\n"
        f"🌍 <b>Гражданство:</b> {esc(profile.get('citizenship'))}\n"
        f"📜 <b>Патент:</b> {esc(profile.get('has_patent'))}\n\n"
        f"📅 <b>Собеседование:</b> {esc(meta.get('interview_date'))} в {esc(meta.get('interview_time'))}\n\n"
        f"🔗 <a href='{chat_link}'>{link_label}</a>"
    )

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
            "✅ Карточка кандидата отправлена в TG", 
            extra={
                "target_chat": target_chat_id,
                "candidate": candidate.full_name
            }
        )
    except Exception as e:
        logger.exception(f"❌ Ошибка отправки карточки в TG: {e}")