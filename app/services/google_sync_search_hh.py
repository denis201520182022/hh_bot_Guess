# app/services/google_sync_search_hh.py
import logging

logger = logging.getLogger("GoogleSyncSearchHH")

class GoogleSyncSearchHHService:
    async def sync_all(self):
        """
        Заглушка для синхронизации параметров поиска HH из Google Таблиц.
        Сама логика поиска (outbound) пока не реализована.
        """
        logger.debug("Синхронизация поиска HH: функционал в разработке.")
        pass

# Создаем экземпляр, который ожидает scheduler
google_sync_search_hh_service = GoogleSyncSearchHHService()