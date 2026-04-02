import gspread
import logging
import re
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.db.models import JobContext, Account
from app.core.config import settings

logger = logging.getLogger("GoogleSyncSearch")

class GoogleSyncSearchAvitoService:
    def __init__(self):
        self.gc = gspread.service_account(filename=settings.GOOGLE_CREDENTIALS_JSON)

    def _parse_value(self, val, value_type="string"):
        if val is None: return None
        s_val = str(val).strip()
        if s_val == "" or s_val.lower() in ['null', 'none']: return None

        if value_type == "int":
            num_match = re.search(r'\d+', s_val)
            return int(num_match.group()) if num_match else None
        if value_type == "ids":
            # Ищем все числа, фильтруем пустые
            ids = [p for p in re.findall(r'\b\d+\b', s_val) if p]
            return ",".join(ids) if ids else None
        if value_type == "multi":
            # Разделяем по пробелам, переносам строк ИЛИ запятым
            # Фильтруем пустые элементы, чтобы не было двойных запятых
            parts = [p.strip() for p in re.split(r'[\s\n,]+', s_val) if p.strip()]
            return ",".join(parts) if parts else None
        return s_val

    async def sync_all(self):
        try:
            # Берем настройки из конфига платформы Avito
            search_cfg = settings.platforms.avito.outbound_search
            sh = self.gc.open_by_url(search_cfg.spreadsheet_url)
            ws = sh.worksheet(search_cfg.search_sheet_name)

            # Читаем всё один раз для анализа
            all_values = ws.get_all_values()
            if not all_values: return

            async with AsyncSessionLocal() as db:
                # 1. Получаем активные вакансии из БД
                stmt = select(JobContext).join(Account).where(
                    Account.platform == 'avito',
                    JobContext.is_active == True
                )
                db_vacancies = (await db.execute(stmt)).scalars().all()
                db_vac_map = {str(v.external_id): v for v in db_vacancies}

                # Координаты для пакетного обновления
                updates = []
                # Список столбцов для удаления (индексы gspread начинаются с 1)
                cols_to_delete = []

                header_row = all_values[1] # Строка 2 с ID вакансий
                num_cols_in_sheet = len(header_row)
                processed_ids_in_sheet = set()

                # 2. Обрабатываем существующие столбцы (начиная с C, это индекс 2)
                for col_idx in range(2, num_cols_in_sheet):
                    ext_id = header_row[col_idx].strip()
                    if not ext_id: continue

                    if ext_id in db_vac_map:
                        processed_ids_in_sheet.add(ext_id)
                        vacancy = db_vac_map[ext_id]

                        # --- ЧИТАЕМ ПАРАМЕТРЫ (ничего не пишем обратно в эти строки!) ---
                        raw_filters = {
                            "query":                   self._parse_value(all_values[5][col_idx]),
                            "location":                self._parse_value(all_values[6][col_idx], "ids"),
                            "metro":                   self._parse_value(all_values[7][col_idx], "ids"),
                            "district":                self._parse_value(all_values[8][col_idx], "ids"),
                            "specialization":          self._parse_value(all_values[9][col_idx], "ids"),
                            "schedule":                self._parse_value(all_values[10][col_idx], "multi"),
                            "business_trip_readiness": self._parse_value(all_values[11][col_idx], "multi"),
                            "relocation_readiness":    self._parse_value(all_values[12][col_idx], "multi"),
                            "gender":                  self._parse_value(all_values[13][col_idx], "multi"),
                            "age_min":                 self._parse_value(all_values[14][col_idx], "int"),
                            "age_max":                 self._parse_value(all_values[15][col_idx], "int"),
                            "education_level":         self._parse_value(all_values[16][col_idx], "multi"),
                            "experience_min":          self._parse_value(all_values[17][col_idx], "int"),
                            "experience_max":          self._parse_value(all_values[18][col_idx], "int"),
                            "salary_min":              self._parse_value(all_values[19][col_idx], "int"),
                            "salary_max":              self._parse_value(all_values[20][col_idx], "int"),
                            "nationality":             self._parse_value(all_values[21][col_idx], "ids"),
                            "driver_licence":          self._parse_value(all_values[22][col_idx]),
                            "driver_licence_category": self._parse_value(all_values[23][col_idx], "multi"),
                            "driving_experience":      self._parse_value(all_values[24][col_idx], "multi"),
                            "own_transport":           self._parse_value(all_values[25][col_idx], "multi"),
                            "medical_book":            self._parse_value(all_values[26][col_idx]),
                        }
                        vacancy.search_filters = {k: v for k, v in raw_filters.items() if v is not None}

                        # --- ОБРАБОТКА КВОТ ---
                        quota_to_add = self._parse_value(all_values[3][col_idx], "int")
                        if quota_to_add:
                            # Если в БД сейчас None, заменяем на 0 перед прибавлением
                            current_quota = vacancy.search_remaining_quota or 0
                            vacancy.search_remaining_quota = current_quota + quota_to_add
                            logger.info(f"Вакансия {ext_id}: добавлено {quota_to_add} квот.")

                        # Готовим обновление ячеек (только строки 3, 4, 5)
                        col_letter = re.sub(r'\d+', '', gspread.utils.rowcol_to_a1(1, col_idx + 1))
                        # Строка 3: Название (вдруг сменилось)
                        updates.append({'range': f'{col_letter}3', 'values': [[vacancy.title]]})
                        # Строка 4: Сброс ввода квот в 0/пусто
                        updates.append({'range': f'{col_letter}4', 'values': [['']]})
                        # Строка 5: Новый остаток квот
                        updates.append({'range': f'{col_letter}5', 'values': [[str(vacancy.search_remaining_quota)]]})

                    else:
                        # Вакансия есть в таблице, но нет в БД -> Удаляем столбец
                        # Индекс в gspread начинается с 1, поэтому col_idx + 1
                        cols_to_delete.append(col_idx + 1)

                # 3. ДОБАВЛЕНИЕ НОВЫХ ВАКАНСИЙ (в конец)
                current_last_col = num_cols_in_sheet
                for ext_id, vac in db_vac_map.items():
                    if ext_id not in processed_ids_in_sheet:
                        current_last_col += 1
                        col_letter = re.sub(r'\d+', '', gspread.utils.rowcol_to_a1(1, current_last_col))

                        # Пишем базу для новой вакансии (ID, Название, Пустая квота, Остаток)
                        new_vac_data = [
                            [vac.external_id], # Строка 2
                            [vac.title],       # Строка 3
                            [''],              # Строка 4
                            [str(vac.search_remaining_quota or 0)]
                        ]
                        updates.append({'range': f'{col_letter}2:{col_letter}5', 'values': new_vac_data})
                        logger.info(f"Добавлена новая вакансия в колонку {col_letter}")

                # 4. ВЫПОЛНЯЕМ ОБНОВЛЕНИЯ ЯЧЕЕК
                if updates:
                    ws.batch_update(updates)

                # 5. УДАЛЯЕМ СТОЛБЦЫ (справа налево, чтобы индексы не поехали)
                for col_idx in sorted(cols_to_delete, reverse=True):
                    ws.delete_columns(col_idx)
                    logger.info(f"Удален столбец вакансии индекс {col_idx}, так как она неактивна в БД")

                await db.commit()
                logger.info("✅ Синхронизация завершена (без потери форматирования)")

        except Exception as e:
            logger.error(f"❌ Ошибка: {e}", exc_info=True)

google_sync_search_avito_service = GoogleSyncSearchAvitoService()