# === 12. ВАЛИДАЦИЯ ДАТЫ И ВРЕМЕНИ (АУДИТ + РЕГЛАМЕНТ + СЛОТЫ) ===
            DATE_CRITICAL_STATES = ['init_scheduling_spb', 'scheduling_spb_day', 'scheduling_spb_time', 'interview_scheduled_spb']
            
            

            # Список ключевых слов (как в HH)
            TIME_KEYWORDS = [
                "сегодня", "завтра", "послезавтра", "понедельник", "вторник", "сред", "четверг", 
                "пятниц", "суббот", "воскресен", "январ", "феврал", "март", "апрел", "май", "июн", 
                "июл", "август", "сентябр", "октябр", "ноябр", "декабр", "число", "время", "числа", "числ", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
            ]

            if new_state in DATE_CRITICAL_STATES:
                interview_date = extracted_data.get("interview_date")
                interview_time = extracted_data.get("interview_time")
                # Проверяем наличие маркеров времени
                bot_text_low = (bot_response_text or "").lower()
                user_text_low = combined_masked_message.lower()
                has_time_keywords = any(kw in bot_text_low or kw in user_text_low for kw in TIME_KEYWORDS)

                # Входим, если есть дата в JSON или обсуждение времени в тексте
                if interview_date or has_time_keywords:
                    ctx_logger.info("Есть дата или маркеры")

                    # --- [НОВОЕ] ПРОВЕРКА: ВЫПОЛНИЛ ЛИ БОТ ПРЕДЫДУЩУЮ КОМАНДУ? ---
                    is_obeyed = False
                    last_msg = dialogue.history[-1] if dialogue.history else {}
                    last_content = str(last_msg.get("content", ""))

                    if last_msg.get("role") == "user" and "[SYSTEM COMMAND]" in last_content:
                        import re
                        # Ищем в прошлой команде указание установить дату или null
                        target_date_match = re.search(r"interview_date' в JSON на '(\d{4}-\d{2}-\d{2})'", last_content)
                        target_null = "interview_date' в JSON на null" in last_content

                        if target_date_match and interview_date == target_date_match.group(1):
                            is_obeyed = True
                            ctx_logger.info(f"✅ Бот выполнил инструкцию по дате: {interview_date}")
                        elif target_null and (interview_date is None or interview_date == ""):
                            is_obeyed = True
                            ctx_logger.info("✅ Бот выполнил инструкцию: установил interview_date в null")

                    if not is_obeyed:
                        # --- 12.1 ЕДИНЫЙ ЦЕНТР ВАЛИДАЦИИ ДАТЫ И ДОСТУПНОСТИ СЛОТОВ ---
                        # Мы объединяем Аудитора (понимание намерений) и Гугл Таблицу (реальность)

                        # 1. Узнаем, какую дату реально ХОЧЕТ пользователь (через Умного Аудитора)
                        full_hist = (dialogue.history or [])
                        # Убираем системные команды для Аудитора, чтобы он не запутался в прошлых исправлениях
                        clean_hist_for_audit = [m for m in full_hist if not str(m.get("content", "")).startswith("[SYSTEM")]
                        
                        # Оптимизация: запускаем Аудитора только если дата новая или есть маркеры времени
                        stored_meta = dialogue.metadata_json or {}
                        stored_date = stored_meta.get("interview_date")
                        
                        run_audit = True
                        verified_date = "none"
                        audit_reason = "Ожидание аудита"

                        if stored_date == interview_date and not has_time_keywords:
                            ctx_logger.debug("Дата совпадает с сохраненной и нет новых триггеров. Пропуск аудита.")
                            run_audit = False
                            verified_date = interview_date
                            audit_reason = "Совпадение с метаданными"
                        
                        if run_audit:
                            ctx_logger.info(f"🔍 Запуск аудита даты: {interview_date}")
                            all_slots_map = await sheets_service.get_all_slots_map(sheet_name)
                            calendar_ctx = self._generate_calendar_context_2(all_slots_map)
                            
                            verified_date, audit_reason = await self._verify_date_audit(
                                db, dialogue, interview_date, clean_hist_for_audit, calendar_ctx, ctx_logger.extra
                            )
                            ctx_logger.info(f"Auditor result: {verified_date} | Reason: {audit_reason}")

                        if verified_date and verified_date != "none":
                            # 2. Проверяем РЕАЛЬНУЮ доступность этой даты в Google Sheets
                            try:
                                now_msk = datetime.datetime.now(MOSCOW_TZ)
                                today_str = now_msk.strftime('%Y-%m-%d')
                                
                                available_slots = await sheets_service.get_available_slots(verified_date, sheet_name)
                                
                                # Фильтр "Сегодня": только слоты, на которые человек успеет доехать (+1 час запаса)
                                if verified_date == today_str:
                                    available_slots = [s for s in available_slots if int(s.split(':')[0]) > now_msk.hour]

                                # Определяем день недели для текста системных команд
                                try:
                                    v_date_obj = datetime.datetime.strptime(verified_date, '%Y-%m-%d')
                                    weekdays_ru = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
                                    v_weekday = weekdays_ru[v_date_obj.weekday()]
                                except:
                                    v_weekday = "указанный день"

                                # 3. ПРИНЯТИЕ РЕШЕНИЯ (Анализ расхождений)
                                is_day_full = not available_slots
                                is_hallucination = (verified_date != interview_date)
                                
                                # Проверяем, не выполнил ли бот задачу самостоятельно с первого раза
                                is_already_correct = False
                                if is_day_full and (interview_date is None or interview_date == ""):
                                    is_already_correct = True
                                elif not is_day_full and interview_date == verified_date:
                                    is_already_correct = True
                                
                                if is_already_correct:
                                    ctx_logger.info(f"✅ Бот самостоятельно верно определил дату и доступность ({verified_date})")
                                    interview_date = verified_date # Синхронизируем для дальнейшей логики
                                else:
                                    hint_content = None
                                    if is_day_full:
                                        # Сценарий А: Мест нет. Требуем отказать и занулить JSON.
                                        hint_content = (
                                            f"[SYSTEM COMMAND] Внимание!!! На {verified_date} ({v_weekday}) нет свободных мест в графике. "
                                            f"Ты ОБЯЗАНА сообщить об этом кандидату и предложить выбрать любой другой свободный день из календаря. "
                                            f"ОБЯЗАТЕЛЬНО установи 'interview_date' в JSON на null."
                                        )
                                    elif is_hallucination:
                                        # Сценарий Б: Ошибка извлечения (дата в JSON не та), но места есть.
                                        slots_str = ", ".join(available_slots)
                                        hint_content = (
                                            f"[SYSTEM COMMAND] В прошлом шаге ты ошиблась в извлечении даты. "
                                            f"На самом деле пользователь выбрал {v_weekday} ({verified_date}). "
                                            f"На этот день есть свободные места: {slots_str}. "
                                            f"Сгенерируй ответ заново: подтверди дату ({v_weekday}, {verified_date}), "
                                            f"расскажи про доступное время и ОБЯЗАТЕЛЬНО установи 'interview_date' в JSON на '{verified_date}'."
                                        )
                                        
                                        # Алерт о галлюцинации в ТГ
                                        await mq.publish("tg_alerts", {
                                            "type": "hallucination",
                                            "dialogue_id": dialogue.id,
                                            "external_chat_id": dialogue.external_chat_id,
                                            "user_said": combined_masked_message,
                                            "llm_suggested": interview_date,
                                            "corrected_val": verified_date,
                                            "reasoning": audit_reason,
                                            "history_text": self._get_history_as_text(dialogue)
                                        })

                                    # 4. ИСПОЛНЕНИЕ (Ретрай)
                                    if hint_content:
                                        # Проверка на дубликаты (чтобы не спамить одной и той же командой подряд)
                                        last_msg_content = str(full_hist[-1].get("content", "")) if full_hist else ""
                                        
                                        if hint_content != last_msg_content:
                                            ctx_logger.info(f"Adding system command for {verified_date}. Retry triggered.")
                                            sys_msg = {
                                                "role": "user",
                                                "content": hint_content,
                                                "message_id": f"sys_date_fix_{time.time()}",
                                                "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
                                            }
                                            dialogue.history = full_hist + [sys_msg]
                                            dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
                                            await db.commit()
                                        else:
                                            ctx_logger.warning(f"System command already in history, but bot ignored it. Re-triggering retry for {verified_date}.")

                                        # В ЛЮБОМ СЛУЧАЕ прерываем текущую отправку и отправляем на ретрай
                                        await mq.publish("engine_tasks", {
                                            "dialogue_id": dialogue.id, 
                                            "trigger": "date_slots_refine",
                                            "initial_msg_count": hh_msg_count_start
                                        })
                                        return
                            except Exception as e:
                                ctx_logger.error(f"Ошибка в этапе Hint (Google Sheets): {e}")
                                await mq.publish("tg_alerts", {
                                    "type": "system",
                                    "text": f"🚨 **СБОЙ GOOGLE SHEETS:** Не удалось получить слоты для диалога `{dialogue.id}`. Проверьте таблицу!",
                                    "alert_type": "admin_only"
                                })
                                raise e

            # =====================================================================
            # [START] ШАГ 3: ЖЕСТКАЯ ВАЛИДАЦИЯ ВРЕМЕНИ (TIME ENFORCEMENT)
            # =====================================================================
            if new_state in DATE_CRITICAL_STATES and interview_date and interview_time:
                try:
                    # 1. Получаем свежий список слотов для этой даты
                    available_slots = await sheets_service.get_available_slots(interview_date, sheet_name)
                    
                    # 2. Фильтр "Сегодня"
                    now_msk = datetime.datetime.now(MOSCOW_TZ)
                    if interview_date == now_msk.strftime('%Y-%m-%d'):
                        available_slots = [s for s in available_slots if int(s.split(':')[0]) > now_msk.hour]

                    # 3. СРАВНЕНИЕ: Проверяем, входит ли время от LLM в список разрешенных
                    clean_time = interview_time.strip()
                    
                    if clean_time not in available_slots:
                        ctx_logger.warning(f"🚨 МОДЕЛЬ ВЫБРАЛА ЗАНЯТОЕ ВРЕМЯ! Выбрано: {clean_time}, Свободно: {available_slots}")

                        error_msg = f"На дату {interview_date} сейчас доступно только время: {', '.join(available_slots)}. Слот {clean_time} недоступен или уже занят."
                        
                        time_corr_cmd = {
                            'message_id': f'sys_time_corr_{time.time()}',
                            'role': 'user',
                            'content': (
                                f"[SYSTEM COMMAND] {error_msg} Извенись и предложи выбрать из свободных сейчас слотов: "
                                f"{', '.join(available_slots) if available_slots else 'другой день'}. "
                                f"ОБЯЗАТЕЛЬНО обнови поле 'interview_time' в JSON на null."
                            ),
                            'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()
                        }

                        dialogue.history = (dialogue.history or []) + [time_corr_cmd]
                        await db.commit()
                        
                       
                        await mq.publish("engine_tasks", {
                            "dialogue_id": dialogue.id, 
                            "trigger": "time_enforce_retry",
                            "initial_msg_count": hh_msg_count_start
                        })
                        return 

                except Exception as e:
                    ctx_logger.error(f"Ошибка в этапе жесткой валидации времени: {e}")
                    await mq.publish("tg_alerts", {
                        "type": "system",
                        "text": f"🚨 **СБОЙ GOOGLE SHEETS:** Не удалось получить слоты для диалога `{dialogue.id}`. Проверьте таблицу!",
                        "alert_type": "admin_only"
                    })
                    raise e