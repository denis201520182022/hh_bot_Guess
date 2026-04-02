# app/core/schemas.py
from pydantic import BaseModel
from typing import Optional, Dict, Any

class IncomingEventDTO(BaseModel):
    """Событие из очереди inbound"""
    platform: str
    external_chat_id: str
    text: Optional[str] = None
    user_id: str
    item_id: str
    raw_payload: Dict[str, Any]

class CandidateDTO(BaseModel):
    """Универсальный профиль кандидата"""
    full_name: Optional[str] = None
    phone: Optional[str] = None
    platform_user_id: str
    location: Optional[str] = None
    raw_payload: Optional[Dict[str, Any]] = None

class JobContextDTO(BaseModel):
    """Данные о вакансии"""
    external_id: str
    title: str
    description: str

class EngineTaskDTO(BaseModel):
    """Задача для воркера ИИ"""
    dialogue_id: int
    external_chat_id: str
    text: Optional[str] = None
    account_id: int
    platform: str
    event_type: str