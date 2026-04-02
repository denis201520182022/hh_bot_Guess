# app/db/base.py
from app.db.session import Base
from app.db.models import (
    Account, JobContext, Candidate, Dialogue, 
    LlmLog, TelegramUser, AppSettings, 
    InterviewReminder, InterviewFollowup, AnalyticsEvent
)