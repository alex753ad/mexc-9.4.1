# ROADMAP: Phase 3 — Production Integration

## Статус проекта

### Завершено (Phase 1-2)
| Компонент | Файл | Статус |
|-----------|------|--------|
| Кластерный рендж (VWAP) | robot_screener.py | ✅ |
| Squeeze feasibility | robot_screener.py | ✅ |
| Volume profile | robot_screener.py | ✅ |
| Recovery time tracking | history.py v5 | ✅ |
| Entry zone calculator | robot_screener.py v3 | ✅ |
| Long-term drift detection | robot_screener.py v3 | ✅ |
| Enhanced self-trade (alternating) | algo_detector.py v3.1 | ✅ |
| Wall disappearance/recovery | history.py v5 | ✅ |
| 6-block scoring (100 pts) | robot_screener.py v3 | ✅ |

### НЕ завершено (критические пробелы)
| Проблема | Почему критично |
|----------|-----------------|
| app.py не интегрирован | Скринер существует как отдельный модуль, но НЕ подключён в UI. Пользователь не может его использовать. |
| Нет persistent storage | Все данные теряются при перезапуске Streamlit. Невозможно накопить 10+ сканов для надёжного анализа. |
| config разбросан по файлам | Константы скринера в 3 местах (config.py, robot_screener.py, algo_detector.py). Нельзя настраивать через UI. |
| Нет multi-timeframe | Рендж определяется только по текущему стакану. Ложные ренджи от временных консолидаций не фильтруются. |
| ws_monitor отключён | WebSocket мониторинг существует (ws_monitor.py), но полностью отделён от Streamlit. Скринер работает только на REST снимках каждые 60с. |

---

## Phase 3: Подробный план

### 3.1 — Полная интеграция app.py (HIGH PRIORITY)

**Задача**: Переписать app.py с встроенной вкладкой скринера. Не патч, а полноценная 5-я страница.

**Что делаем**:
- Добавляем `from robot_screener import RobotScreener, render_robot_screener_tab`
- Добавляем 5-ю вкладку "🎯 Скринер" в TAB_LABELS
- Добавляем `elif page == 4:` с вызовом `render_robot_screener_tab()`
- Обновляем sidebar: новые параметры скринера (пороги squeeze, recovery, drift)
- Добавляем в auto-scan: после каждого скана автоматически обновлять скринер
- Добавляем кнопку "Открыть в стакане" из скринера → page 1

**Файлы**: `app.py` (полная замена)

---

### 3.2 — Persistent Storage (SQLite) (HIGH PRIORITY)

**Задача**: Сохранять данные между перезапусками Streamlit.

**Что сохраняем**:
- `scan_results` — результаты каждого скана (symbol, walls, score, timestamp)
- `algo_signals` — результаты алго-скана
- `range_history` — история границ ренджа (для drift)
- `recovery_events` — события исчезновения/восстановления стенок
- `screener_results` — результаты скринера с оценками

**Архитектура**:
- Файл: `storage.py` — класс `ScannerDB` с SQLite backend
- Таблицы: `scans`, `algo_signals`, `range_points`, `recovery_events`, `screener_scores`
- БД создаётся в `~/.mexc_scanner/scanner.db`
- При старте Streamlit: загрузка последних N записей в DensityTracker
- После каждого скана: автосохранение

**Преимущества**:
- 10+ сканов за минуты вместо часов ожидания
- Drift detection на данных за дни/недели
- Recovery statistics накапливаются между сессиями

**Файлы**: `storage.py` (новый), изменения в `app.py`

---

### 3.3 — Консолидация конфигурации (MEDIUM)

**Задача**: Все настраиваемые параметры в одном месте + UI контроль.

**Что делаем**:
- Перенести все константы из robot_screener.py (RANGE_*, ROBOT_*, TIMING_*, SQUEEZE_*, RECOVERY_*) в config.py
- Добавить в sidebar expander "⚙️ Параметры скринера" с ключевыми ползунками
- Runtime-изменяемые параметры: min/max range width, squeeze threshold, min scans for score

**Файлы**: `config.py` (обновление), `app.py` (sidebar)

---

### 3.4 — Multi-Timeframe Range Validation (MEDIUM)

**Задача**: Проверять рендж на нескольких таймфреймах.

**Логика**:
- После обнаружения ренджа по стакану, загружаем свечи 1h (24 шт) и 4h (6 шт)
- Считаем: high-low range на 1h и 4h
- Если 1h range > 2× ширины ренджа по стакану → ложный рендж (тренд)
- Если 4h показывает чёткий тренд (последовательно растёт/падает) → предупреждение
- Добавляем новый флаг: `FLAG_TREND_CONFLICT`

**Метрика**: `trend_alignment_score` (0-1): насколько свечной рендж совпадает со стаканным.

**Файлы**: новая функция в `robot_screener.py`, использует `mexc_client.get_klines()`

---

### 3.5 — Screener Auto-Refresh Pipeline (MEDIUM)

**Задача**: Скринер обновляется автоматически без ручного запуска.

**Что делаем**:
- После каждого auto-scan (кнопка уже есть) — автоматически прогонять скринер
- Сохранять в `st.session_state.screener_results`
- Показывать badge в навигации: "🎯 Скринер (3 🔥)" — количество рабочих монет
- При изменении grade (S→B или B→S) — показывать notification

**Файлы**: `app.py`

---

### Порядок реализации

```
3.2 Storage  ──→  3.1 App Integration  ──→  3.3 Config
                        │
                        ├──→  3.4 Multi-TF
                        │
                        └──→  3.5 Auto-Refresh
```

Storage первым, потому что app integration зависит от него (загрузка истории при старте).

---

## Deliverables Phase 3

| # | Файл | Описание |
|---|------|----------|
| 1 | `storage.py` | SQLite backend для persistent storage |
| 2 | `config.py` | Консолидированная конфигурация + параметры скринера |
| 3 | `robot_screener.py` | v4: multi-timeframe validation |
| 4 | `app.py` | Полная интеграция: 5 вкладок, auto-refresh, sidebar |
