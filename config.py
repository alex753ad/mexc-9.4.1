"""
Конфигурация MEXC Density Scanner v7.0 + Algo Detector v3.1 + Robot Screener v4 (Phase 3)

Все настраиваемые параметры в одном файле.
"""

# ═══════════════════════════════════════════════
# MEXC API
# ═══════════════════════════════════════════════
MEXC_BASE_URL = "https://api.mexc.com"
MEXC_WS_URL = "wss://wbs.mexc.com/ws"

# ═══════════════════════════════════════════════
# ФИЛЬТРЫ ОТБОРА МОНЕТ
# ═══════════════════════════════════════════════
MAX_DAILY_VOLUME_USDT = 500_000
MIN_DAILY_VOLUME_USDT = 100
QUOTE_ASSET = "USDT"

VOLUME_TIMEFRAMES = {
    "5m":  288,
    "15m": 96,
    "1h":  24,
    "4h":  6,
    "24h": 1,
}

# ═══════════════════════════════════════════════
# ДЕТЕКЦИЯ ПЛОТНОСТЕЙ
# ═══════════════════════════════════════════════
ORDER_BOOK_DEPTH = 100
MIN_WALL_SIZE_USDT = 50
WALL_MULTIPLIER = 5
MIN_SPREAD_PCT = 0.5
MAX_WALL_DISTANCE_PCT = 15.0

# ═══════════════════════════════════════════════
# СКАНИРОВАНИЕ
# ═══════════════════════════════════════════════
MAX_CONCURRENT_REQUESTS = 5
BATCH_DELAY = 1.0
TOP_RESULTS = 50

# ═══════════════════════════════════════════════
# ДЕТЕКЦИЯ ПЕРЕСТАВЛЯШЕЙ
# ═══════════════════════════════════════════════
MOVER_SIZE_TOLERANCE = 0.20
MOVER_TIME_WINDOW = 120
MOVER_MIN_PRICE_SHIFT = 0.1
MAX_SNAPSHOTS_PER_PAIR = 60

# ═══════════════════════════════════════════════
# ГРАФИКИ — таймфреймы свечей
# ═══════════════════════════════════════════════
CHART_INTERVALS = {
    "1m":  {"api": "1m",  "limit": 120, "label": "1 мин"},
    "5m":  {"api": "5m",  "limit": 100, "label": "5 мин"},
    "1h":  {"api": "60m", "limit": 100, "label": "1 час"},
    "4h":  {"api": "4h",  "limit": 100, "label": "4 часа"},
    "1d":  {"api": "1d",  "limit": 100, "label": "1 день"},
}

# ═══════════════════════════════════════════════
# АЛГО-ДЕТЕКТОР (Ёршики)
# ═══════════════════════════════════════════════

# Хардкод-блэклист
ALGO_BLACKLIST = [
    "KWENTAUSDT", "WILDUSDT", "WNKUSDT", "WNXMUSDT",
    "BORINGUSDT", "ANMLUSDT", "AOSUSDT", "APNUSDT",
    "DFYNUSDT", "WHITEUSDT", "RDACUSDT", "REPPOUSDT",
]

# Буфер сделок
ALGO_TRADES_BUFFER_SIZE = 500
ALGO_TRADES_MAX_AGE_SEC = 600
ALGO_TRADES_REST_LIMIT = 1000

# Кластеризация объёмов
ALGO_SIZE_TOLERANCE = 0.15
ALGO_MIN_CLUSTER_PCT = 15
ALGO_MIN_TRADES_CLUSTER = 5
ALGO_MIN_TRADES = 15

# Тайминги
ALGO_TIMING_CV_THRESHOLD = 0.35

# Спред
ALGO_MIN_SPREAD_PCT = 2.0
ALGO_MAX_SPREAD_PCT = 12.0
ALGO_IDEAL_SPREAD_MIN = 4.0
ALGO_IDEAL_SPREAD_MAX = 8.0

# Скоринг
ALGO_SCORE_FIRE = 65
ALGO_SCORE_ROBOT = 40
ALGO_SCORE_WEAK = 20

# ═══════════════════════════════════════════════
# ПРОВЕРКА ЧЕСТНОСТИ v2.1
# ═══════════════════════════════════════════════
HONESTY_MIN_TRADES = 10
HONESTY_SIDE_DOMINANT_PCT = 75
HONESTY_PRICE_LEVELS_RATIO = 0.15
HONESTY_SELFTRADE_WINDOW_MS = 1500
HONESTY_SELFTRADE_WARN_PCT = 10
HONESTY_SELFTRADE_SCAM_PCT = 25
HONESTY_SCORE_HONEST = 55
HONESTY_SCORE_SUSPECT = 30

# v3.1: Alternating pattern
HONESTY_ALTERNATING_WINDOW_MS = 3000  # 3 сек окно для чередования
HONESTY_ALTERNATING_SIZE_TOL = 0.22   # ±22% допуск на сайз

# ═══════════════════════════════════════════════
# ДЕТЕКТОР ЛЕСЕНОК
# ═══════════════════════════════════════════════
LADDER_MIN_STEPS = 3
LADDER_STEP_TOLERANCE = 0.15
LADDER_SIZE_TOLERANCE = 0.25
LADDER_MAX_DISTANCE_PCT = 10.0
LADDER_MIN_SIZE_USDT = 5

# ═══════════════════════════════════════════════
# ПОВЕДЕНИЕ СТЕНОК (Wall Behavior)
# ═══════════════════════════════════════════════
WALL_BEHAVIOR_MIN_EVENTS = 3
WALL_BEHAVIOR_RETREAT_THRESHOLD = 0.65
WALL_BEHAVIOR_ADVANCE_THRESHOLD = 0.65

# ═══════════════════════════════════════════════
# СКРИНЕР «РАБОЧИЙ ЁРШ» v4 (Phase 3)
# ═══════════════════════════════════════════════

# --- Рендж ---
SCREENER_RANGE_MIN_WIDTH_PCT = 2.0       # Мин ширина ренджа
SCREENER_RANGE_MAX_WIDTH_PCT = 20.0      # Макс ширина ренджа
SCREENER_RANGE_IDEAL_MIN_PCT = 4.0       # Идеальный мин
SCREENER_RANGE_IDEAL_MAX_PCT = 12.0      # Идеальный макс
SCREENER_RANGE_STABILITY_MIN_SCANS = 3   # Мин сканов для оценки стабильности
SCREENER_RANGE_DRIFT_THRESHOLD_PCT = 0.5 # Дрейф порог (% за скан)
SCREENER_RANGE_CLUSTER_TOP_N = 3         # Топ-N стенок для VWAP
SCREENER_RANGE_LONG_DRIFT_MIN_POINTS = 10  # Мин точек для long-term drift

# --- Робот ---
SCREENER_ROBOT_MIN_SCANS_PRESENT = 3     # Мин присутствие стенки (сканов)
SCREENER_ROBOT_RETREAT_MIN_PCT = 60.0    # Мин retreat% для "рабочего"

# --- Тайминг ---
SCREENER_TIMING_NANO_CV_MAX = 0.25       # CV < порог → наносекундный
                                         # B-22 fix: повышено с 0.15 до 0.25 —
                                         # учитываем дрейф часов сервера биржи (±500мс),
                                         # который размывает CV даже у чётких роботов.
SCREENER_TIMING_OK_AVG_MIN_SEC = 1.5     # Мин средний интервал (сек) для "ловимого"

# B-22 fix: допуск на дрейф часов биржи при кластеризации second_locked.
# MEXC-серверы имеют типичный дрейф ±500мс → ±1 секунда при round().
# Без допуска робот, торгующий каждую секунду, размазывается по двум
# соседним секундам и не детектируется как second_locked.
ALGO_TIMING_CLOCK_DRIFT_SEC = 1          # допуск ±N секунд при second_locked

# --- Self-trade ---
SCREENER_SELFTRADE_EXCLUDE_PCT = 20.0    # > порог → исключаем

# --- Squeeze feasibility ---
SCREENER_SQUEEZE_EASY_RATIO = 2.0        # wall/hourly_vol < порог → легко
SCREENER_SQUEEZE_HARD_RATIO = 10.0       # wall/hourly_vol > порог → невозможно

# --- Volume profile ---
SCREENER_VOLUME_SYMMETRY_THRESHOLD = 0.6 # bid/ask ratio порог симметричности

# --- Recovery time ---
SCREENER_RECOVERY_EXCELLENT_SEC = 120    # < 2 мин — отличное восстановление
SCREENER_RECOVERY_GOOD_SEC = 300         # < 5 мин — хорошее
SCREENER_RECOVERY_POOR_SEC = 600         # > 10 мин — плохое

# --- Entry zone ---
SCREENER_ENTRY_BUFFER_PCT = 0.5          # Буфер от стенки (% цены)

# --- Multi-timeframe validation ---
SCREENER_MTF_ENABLED = True              # Включить multi-TF проверку
SCREENER_MTF_TREND_RATIO = 2.0           # 1h range > N× стаканный рендж → тренд
SCREENER_MTF_TREND_CANDLES_1H = 24       # Свечей 1h для проверки
SCREENER_MTF_TREND_CANDLES_4H = 6        # Свечей 4h для проверки

# --- Скоринг ---
SCREENER_MIN_SCANS_FOR_FULL_SCORE = 5    # Мин сканов для полного скора
SCREENER_SCORE_WEIGHTS = {
    "range": 20,      # Блок А: Рендж
    "robot": 25,      # Блок Б: Робот
    "honesty": 15,    # Блок В: Честность
    "timing": 15,     # Блок Г: Тайминг
    "squeeze": 15,    # Блок Д: Squeeze
    "recovery": 10,   # Блок Е: Recovery
}

# --- Persistent Storage ---
STORAGE_ENABLED = True                   # Включить SQLite сохранение
STORAGE_MAX_AGE_DAYS = 7                 # Хранить данные N дней
STORAGE_CLEANUP_INTERVAL_SEC = 3600      # Чистка каждые N секунд

# ═══════════════════════════════════════════════
# STREAMLIT
# ═══════════════════════════════════════════════
AUTO_REFRESH_INTERVAL = 60
