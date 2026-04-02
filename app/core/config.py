# app/core/config.py
import yaml
import os
from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- ВСПОМОГАТЕЛЬНЫЕ МОДЕЛИ (ОБЯЗАТЕЛЬНЫЕ ПОЛЯ) ---

class BotConfig(BaseModel):
    id: str

class SystemConfig(BaseModel):
    docker_mode: bool
    log_level: str

class OutboundSearchConfig(BaseModel):
    enabled: bool
    spreadsheet_url: str
    search_sheet_name: str

class AvitoPlatformSettings(BaseModel):
    enabled: bool
    poll_interval: int
    webhook_enabled: bool
    vacancy_description_source: str
    outbound_search: OutboundSearchConfig
    concurrency_limit: int
    dry_run_search: bool = False

class HHPlatformSettings(BaseModel):
    enabled: bool
    poll_interval: int
    vacancy_description_source: str
    outbound_search: OutboundSearchConfig
    concurrency_limit: int
    dry_run_search: bool = False

class PlatformsConfig(BaseModel):
    avito: AvitoPlatformSettings
    hh: HHPlatformSettings

class TelegramServicesConfig(BaseModel):
    enabled: bool
    interview_cards: bool
    reject_cards: bool
    molchun_cards: bool
    reschedule_cards: bool

class GoogleSheetsReportConfig(BaseModel):
    enabled: bool
    spreadsheet_url: str
    candidates_sheet_name: str

class CRMIntegrationConfig(BaseModel):
    enabled: bool

class ServicesConfig(BaseModel):
    telegram: TelegramServicesConfig
    google_sheets_report: GoogleSheetsReportConfig
    crm_integration: CRMIntegrationConfig

class TalantixConfig(BaseModel):
    # Пока пусто в конфиге
    pass

class SchedulingGoogleSheetsConfig(BaseModel):
    spreadsheet_url: str
    calendar_sheet_name: str

class SchedulingConfig(BaseModel):
    provider: str
    talantix: Optional[Dict[str, Any]] = None 
    google_sheets: Optional[SchedulingGoogleSheetsConfig] = None

class LLMConfig(BaseModel):
    main_model: str
    smart_model: str
    temperature: float
    max_tokens: int
    request_timeout: int
    global_concurrency: int

class KBConfig(BaseModel):
    prompt_doc_url: str
    cache_ttl: int

class SilenceReminderLevel(BaseModel):
    delay_minutes: int
    text: str
    stop_bot: bool = False

class QuietTimeConfig(BaseModel):
    enabled: bool
    start: str
    end: str
    default_timezone: str

class SilenceConfig(BaseModel):
    enabled: bool
    quiet_time: QuietTimeConfig
    levels: List[SilenceReminderLevel]

class InterviewReminderItem(BaseModel):
    id: str
    type: Literal["fixed_time", "relative"]
    days_before: Optional[int] = 0
    at_time: Optional[str] = None
    minutes_before: Optional[int] = None
    text: str

class InterviewConfig(BaseModel):
    enabled: bool
    items: List[InterviewReminderItem]

class RemindersConfig(BaseModel):
    silence: SilenceConfig
    interview: InterviewConfig

class MessagesConfig(BaseModel):
    initial_greeting: str
    qualification_failed_farewell: str

# --- ГЛАВНЫЙ КЛАСС НАСТРОЕК ---

class Settings(BaseModel):
    # Данные из config.yaml (Теперь всё без дефолтов)
    bot: BotConfig
    system: SystemConfig
    platforms: PlatformsConfig
    services: ServicesConfig
    scheduling: SchedulingConfig
    llm: LLMConfig
    knowledge_base: KBConfig
    reminders: RemindersConfig
    messages: MessagesConfig

    # --- ПАРАМЕТРЫ ИЗ .ENV (обязательные, если не заданы в окружении) ---
    DATABASE_URL: str = Field(default_factory=lambda: os.getenv("DATABASE_URL"))
    RABBITMQ_URL: str = Field(default_factory=lambda: os.getenv("RABBITMQ_URL"))
    REDIS_URL: str = Field(default_factory=lambda: os.getenv("REDIS_URL"))
    
    OPENAI_API_KEY: str = Field(default_factory=lambda: os.getenv("OPENAI_API_KEY"))
    TELEGRAM_BOT_TOKEN: str = Field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN"))
    
    AVITO_WEBHOOK_SECRET: Optional[str] = Field(default_factory=lambda: os.getenv("AVITO_WEBHOOK_SECRET"))
    WEBHOOK_BASE_URL: Optional[str] = Field(default_factory=lambda: os.getenv("WEBHOOK_BASE_URL"))
    
    TALANTIX_COOKIES: Optional[str] = Field(default_factory=lambda: os.getenv("TALANTIX_COOKIES"))
    
    # Настройки Прокси
    SQUID_PROXY_HOST: Optional[str] = Field(default_factory=lambda: os.getenv("SQUID_PROXY_HOST"))
    SQUID_PROXY_PORT: Optional[str] = Field(default_factory=lambda: os.getenv("SQUID_PROXY_PORT"))
    SQUID_PROXY_USER: Optional[str] = Field(default_factory=lambda: os.getenv("SQUID_PROXY_USER"))
    SQUID_PROXY_PASSWORD: Optional[str] = Field(default_factory=lambda: os.getenv("SQUID_PROXY_PASSWORD"))

    class Config:
        populate_by_name = True

    @classmethod
    def load(cls, path: str = "config.yaml"):
        if not Path(path).exists():
            raise FileNotFoundError(f"Файл конфигурации не найден: {path}")
            
        with open(path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
            
        # Pydantic выбросит ValidationError, если чего-то не хватает
        return cls(**config_data)

# Глобальный объект настроек
try:
    settings = Settings.load()
except Exception as e:
    import logging
    logging.error(f"❌ Критическая ошибка загрузки конфигурации: {e}")
    raise

