from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Boolean, Numeric, BigInteger, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy import Date, func
from sqlalchemy import UniqueConstraint
Base = declarative_base()

class Account(Base):
    """
    Универсальный аккаунт рекрутера/компании.
    Здесь хранятся доступы к Авито, HH или другим площадкам.
    """
    __tablename__ = 'accounts'
    
    id = Column(Integer, primary_key=True)
    platform = Column(String(20), nullable=False) # 'avito', 'hh', 'whatsapp'
    name = Column(String(100))
    
    # Все ключи, токены и настройки авторизации в одной корзине
    # Для Авито: {client_id, client_secret, access_token, refresh_token}
    # Для HH: {access_token, refresh_token, expires_at}
    auth_data = Column(JSONB, server_default='{}')
    
    # Настройки уведомлений (TG chat_id, темы и т.д.)
    settings = Column(JSONB, server_default='{}')
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    vacancies = relationship("JobContext", back_populates="account")
    dialogues = relationship("Dialogue", back_populates="account")


class JobContext(Base):
    __tablename__ = 'job_contexts'
    
    id = Column(Integer, primary_key=True)
    external_id = Column(String(50), unique=True, index=True)
    account_id = Column(Integer, ForeignKey('accounts.id'))
    
    title = Column(String(255))
    city = Column(String(100))
    is_active = Column(Boolean, default=True) # <-- НОВЫЙ СТОЛБЕЦ
    
    description_data = Column(JSONB, server_default='{}')


    # Остаток квот на открытие контактов именно для этой вакансии
    # Клиент вводит число в таблицу, бот обновляет это поле, а затем минусует при поиске
    search_remaining_quota = Column(Integer, default=0)
    
    # Все поисковые параметры из строк Google Таблицы (age_min, nationality, query и т.д.)
    search_filters = Column(JSONB, server_default='{}')
    
    account = relationship("Account", back_populates="vacancies")
    dialogues = relationship("Dialogue", back_populates="vacancy")

class Candidate(Base):
    """
    Профиль кандидата. Максимально гибкий.
    """
    __tablename__ = 'candidates'
    
    id = Column(Integer, primary_key=True)
    platform_user_id = Column(String(50), unique=True, index=True) # ID юзера на платформе
    
    full_name = Column(String(255))
    phone_number = Column(String(50), nullable=True)
    
    # Сюда пишем всё: возраст, гражданство, наличие прав, медкнижки и т.д.
    # Структура определяется конфигом YAML конкретного бота.
    profile_data = Column(JSONB, server_default='{}')
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    dialogues = relationship("Dialogue", back_populates="candidate")


class Dialogue(Base):
    """
    Диалог — сердце системы.
    """
    __tablename__ = 'dialogues'
    
    id = Column(Integer, primary_key=True)
    external_chat_id = Column(String(50), unique=True, index=True) # hh_response_id или avito_chat_id
    
    account_id = Column(Integer, ForeignKey('accounts.id'))
    candidate_id = Column(Integer, ForeignKey('candidates.id'))
    vacancy_id = Column(Integer, ForeignKey('job_contexts.id'))
    
    # Состояние для Engine (берется из YAML)
    current_state = Column(String(100)) 
    status = Column(String(50), default='new') # new, in_progress, qualified, rejected, closed
    
    # История сообщений для LLM
    history = Column(JSONB, server_default='[]')
    
    # Метаданные диалога (время собеса, результаты квалификации)
    metadata_json = Column(JSONB, server_default='{}')
    
    # Уровни напоминаний (для Scheduler)
    reminder_level = Column(Integer, default=0)
    last_message_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Финансовая статистика (токены)
    usage_stats = Column(JSONB, server_default='{"total_cost": 0, "tokens": 0}')
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    candidate = relationship("Candidate", back_populates="dialogues")
    vacancy = relationship("JobContext", back_populates="dialogues")
    account = relationship("Account", back_populates="dialogues")
    llm_logs = relationship("LlmLog", back_populates="dialogue")
    # Внутри класса Dialogue
    reminders = relationship("InterviewReminder", back_populates="dialogue", cascade="all, delete-orphan")
    followups = relationship("InterviewFollowup", back_populates="dialogue", cascade="all, delete-orphan")


class LlmLog(Base):
    """
    Логирование каждого запроса к ИИ (для аналитики и дебага)
    """
    __tablename__ = 'llm_logs'
    
    id = Column(Integer, primary_key=True)
    dialogue_id = Column(Integer, ForeignKey('dialogues.id'))
    prompt_type = Column(String(50)) # 'screening', 'audit', 'summary'
    
    model = Column(String(50))
    prompt_tokens = Column(Integer)
    completion_tokens = Column(Integer)
    cost = Column(Numeric(10, 6))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())


    dialogue = relationship("Dialogue", back_populates="llm_logs")



class TelegramUser(Base):
    """Пользователи-админы/рекрутеры, имеющие доступ к управлению ботом через ТГ"""
    __tablename__ = 'telegram_users'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    username = Column(String(100))
    role = Column(String(50), default='user') # 'admin', 'recruiter'
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class AppSettings(Base):
    """Единая таблица настроек и баланса (обычно одна строка с id=1)"""
    __tablename__ = 'app_settings'
    id = Column(Integer, primary_key=True)
    balance = Column(Numeric(12, 2), default=0.00)
    
    # Расценки (берем из конфига или храним здесь)
    costs = Column(JSONB, server_default='''{
        "dialogue": 19.00,
        "reminder": 5.00,
        "followup": 10.00
    }''')
    
    # Общая статистика трат
    stats = Column(JSONB, server_default='''{
        "total_spent": 0.00,
        "spent_on_dialogues": 0.00,
        "spent_on_reminders": 0.00,
        "spent_on_followups": 0.00
    }''')
    
    low_balance_threshold = Column(Numeric(10, 2), default=500.00)
    low_limit_notified = Column(Boolean, default=False)


class InterviewReminder(Base):
    """Напоминания ДО собеседования (за день, за 2 часа)"""
    __tablename__ = 'interview_reminders'
    id = Column(Integer, primary_key=True)
    dialogue_id = Column(Integer, ForeignKey('dialogues.id'), index=True)
    
    # 'evening_before', 'two_hours_before'
    reminder_type = Column(String(50)) 
    
    scheduled_at = Column(DateTime(timezone=True), index=True)
    status = Column(String(50), default='pending') # pending, sent, cancelled, error
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_at = Column(DateTime(timezone=True))
    dialogue = relationship("Dialogue", back_populates="reminders")

class InterviewFollowup(Base):
    """Дожимы ПОСЛЕ собеседования (Как всё прошло?)"""
    __tablename__ = 'interview_followups'
    id = Column(Integer, primary_key=True)
    dialogue_id = Column(Integer, ForeignKey('dialogues.id'), index=True)
    
    step = Column(Integer) # 1, 2, 3
    scheduled_at = Column(DateTime(timezone=True), index=True)
    status = Column(String(50), default='pending') # pending, sent, cancelled
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_at = Column(DateTime(timezone=True))
    dialogue = relationship("Dialogue", back_populates="followups")


class AnalyticsEvent(Base):
    """
    Таблица для мгновенной статистики. 
    """
    __tablename__ = 'analytics_events'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('accounts.id'), index=True)
    job_context_id = Column(Integer, ForeignKey('job_contexts.id'), index=True, nullable=True)
    dialogue_id = Column(Integer, ForeignKey('dialogues.id'), index=True)
    
    event_type = Column(String(50), index=True)
    
    # --- НОВЫЕ ПОЛЯ ---
    city = Column(String(100), index=True) # Для срезов по городам
    interview_date = Column(Date, index=True, nullable=True) # Для доходимости
    mode = Column(String(50), index=True, server_default='standard') # 'standard' (задел на будущее)
    # ------------------

    event_data = Column(JSONB, server_default='{}')
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SearchStatus(Base):
    """Глобальный рубильник поиска для конкретного аккаунта (Авито, HH и др.)"""
    __tablename__ = 'search_statuses'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('accounts.id'), unique=True)
    
    # Тот самый рубильник: True - ищем, False - всё стоит на паузе
    is_enabled = Column(Boolean, default=False)
    
    account = relationship("Account")

class SearchStat(Base):
    """Статистика затрат лимитов по дням и вакансиям"""
    __tablename__ = 'search_stats'
    __table_args__ = (
        UniqueConstraint('account_id', 'vacancy_id', 'date', name='_account_vacancy_date_uc'),
    )
    
    id = Column(Integer, primary_key=True)
    vacancy_id = Column(Integer, ForeignKey('job_contexts.id'))
    account_id = Column(Integer, ForeignKey('accounts.id'))
    date = Column(Date, server_default=func.current_date())
    spent_count = Column(Integer, default=0) # Сколько потрачено за этот день

    vacancy = relationship("JobContext")
