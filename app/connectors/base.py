# app/connectors/base.py
from abc import ABC, abstractmethod
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models import Account
from app.core.schemas import IncomingEventDTO, CandidateDTO, JobContextDTO

class BaseConnector(ABC):
    """
    Абстрактный базовый класс для всех коннекторов (Avito, HH и т.д.).
    Определяет единый интерфейс взаимодействия Engine с внешними платформами.
    """

    @abstractmethod
    async def start(self):
        """Запуск процессов коннектора (поллинг, вебхуки)"""
        pass

    @abstractmethod
    async def stop(self):
        """Остановка процессов коннектора"""
        pass

    @abstractmethod
    async def parse_event(self, payload: dict, account_id: int) -> IncomingEventDTO:
        """
        Переводит сырой JSON (из вебхука или поллера) в унифицированный IncomingEventDTO.
        """
        pass

    @abstractmethod
    async def get_candidate_details(self, account: Account, db: AsyncSession, **kwargs) -> CandidateDTO:
        """
        Запрашивает расширенную информацию о кандидате через API платформы.
        """
        pass

    @abstractmethod
    async def get_job_details(self, account: Account, db: AsyncSession, job_id: str) -> JobContextDTO:
        """
        Запрашивает информацию о вакансии (название, описание) через API платформы.
        """
        pass

    @abstractmethod
    async def send_message(self, account: Account, db: AsyncSession, chat_id: str, text: str, user_id: str = "me"):
        """
        Физическая отправка сообщения в чат платформы.
        """
        pass