# app/connectors/hh/hh_search.py
import logging

logger = logging.getLogger(__name__)

class HHSearchService:
    async def discover_and_propose(self):
        # Заглушка, чтобы scheduler не падал
        logger.debug("HH Search: логика поиска пока не реализована")
        pass

hh_search_service = HHSearchService()