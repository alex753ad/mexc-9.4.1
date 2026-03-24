"""
Range Bounce Scanner v2.0 — Модуль поиска монет в ценовом коридоре

Ищет монеты с ДВУСТОРОННИМ рендж-паттерном (торгуемые от обоих уровней):
  - Повышенный объём у нижней границы (Big Size)
  - Возраст диапазона ≥ 60 минут (Aged 1h+)
  - 3–9 отскоков от нижней И верхней границы (Rejected ×N)

Фильтры (v2.0):
  [F1] Dead book — исключает PXUSDT-тип: мёртвый стакан (book_quality.py)
  [F2] Top bounces — исключает MANA-тип: цена никогда не доходит до верха
  [F3] Range utilization — исключает монеты, застрявшие в одном углу рендж
  [F4] Recent activity — исключает монеты с активностью 30 мин из 24 часов:
       проверяет ПОСЛЕДНИЕ N свечей отдельно, а не среднее по всему периоду

Интеграция:
    from range_bounce_scanner import RangeBounceScanner, render_range_bounce_tab
    render_range_bounce_tab(client, st.session_state.trades_buffer)
"""

import time
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# book_quality используется для фильтра мёртвых стаканов (F1)
try:
    from book_quality import check_book_quality
    _BOOK_QUALITY_AVAILABLE = True
except ImportError:
    _BOOK_QUALITY_AVAILABLE = False

# ═══════════════════════════════════════════════
# Конфигурация по умолчанию
# ═══════════════════════════════════════════════

RBS_DEFAULTS = {
    "timeframe":           "5m",          # Таймфрейм анализа
    "candles_lookback":    48,             # Кол-во свечей для анализа
    "min_range_pct":       2.0,            # Мин. ширина ренджа, %
    "max_range_pct":       15.0,           # Макс. ширина ренджа, %
    "touch_tolerance_pct": 5.0,           # Допуск касания (% от ширины)
    "min_trades_per_min":  5,             # Мин. сделок/мин (среднее за lookback)
    "min_volume_per_hour": 5000,          # Мин. объём/час (USDT), альтернатива
    "vol_multiplier":      10.0,          # Множитель объёма для Big Size
    "vol_avg_period_min":  5,             # Период для среднего объёма (мин)
    "min_range_age_min":   60,            # Мин. возраст ренджа (мин)
    "min_bounces":         3,             # Мин. отскоков для тега
    "max_bounces":         9,             # Макс. отскоков для тега
    "bounce_confirm_pct":  10.0,          # Мин. отскок (% от ширины канала)
    "top_results":         50,            # Кол-во результатов в выдаче

    # ── F1: Dead book filter ──────────────────────────────────────────
    "dead_book_filter":    True,          # Включить проверку стакана

    # ── F2: Top bounces (двусторонние отскоки) ────────────────────────
    "min_top_bounces":     1,             # Мин. отскоков от ВЕРХНЕЙ границы
                                          # 0 = выключено (старое поведение)

    # ── F3: Range utilization ─────────────────────────────────────────
    "min_utilization":     0.10,          # Мин. равномерность использования рендж
                                          # 0.0 = выключено; 0.10 = мягко; 0.20 = строго
                                          # Sqrt(lower_visits% × upper_visits%) < порог → исключить

    # ── F4: Recent activity (защита от "30мин активно, потом мертво") ─
    "recent_candles":      3,             # Проверяем N последних свечей отдельно
    "recent_min_trades":   3,             # Мин. сделок СУММАРНО за N последних свечей
                                          # 0 = выключено
    "recent_vol_ratio":    0.05,          # Мин. доля объёма последних N свечей
                                          # от общего объёма lookback-периода
                                          # Если <5% — монета сейчас мертва
}

TIMEFRAME_TO_API = {
    "1m":  "1m",
    "3m":  "3m",
    "5m":  "5m",
    "15m": "15m",
    "30m": "30m",
    "1h":  "60m",
    "4h":  "4h",
}

TIMEFRAME_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240,
}


# ═══════════════════════════════════════════════
# Структуры данных
# ═══════════════════════════════════════════════

@dataclass
class BounceEvent:
    """Одно касание нижней границы с отскоком"""
    candle_idx: int
    low_price: float
    high_after: float
    bounce_pct: float    # подъём от low до max после, % ширины канала
    time_ms: int = 0


@dataclass
class RangeResult:
    """Результат анализа одной монеты"""
    symbol: str
    current_price: float
    range_low: float
    range_high: float
    range_pct: float              # ширина ренджа в %
    position_in_range: float      # позиция цены (0% = у дна, 100% = у верха)
    range_age_min: float          # возраст ренджа в минутах
    bounces: int                  # кол-во подтверждённых отскоков от низа
    bounce_events: list = field(default_factory=list)

    # Бонусные теги
    has_big_size: bool = False     # объём у нижней границы > avg * multiplier
    has_aged: bool = False         # возраст ≥ min_range_age_min
    has_rejected: bool = False     # 3 ≤ bounces ≤ 9

    # Дополнительно
    avg_vol_usdt: float = 0.0      # средний объём у нижней границы
    big_size_usdt: float = 0.0     # объём крупной сделки у нижней границы
    big_size_ratio: float = 0.0    # во сколько раз > среднего
    trades_per_min: float = 0.0    # сделок в минуту (среднее за lookback)
    hourly_vol_usdt: float = 0.0   # объём за час
    score: int = 0                 # итоговый скор (0–3 бонуса)

    # ── v2.0: новые поля ──────────────────────────────────────────────
    top_bounces: int = 0               # [F2] отскоков от ВЕРХНЕЙ границы
    top_bounce_events: list = field(default_factory=list)
    range_utilization: float = 0.0    # [F3] 0-1, равномерность хождения по рендж
    lower_visits_pct: float = 0.0     # % свечей, касавшихся нижней зоны
    upper_visits_pct: float = 0.0     # % свечей, касавшихся верхней зоны
    recent_trades: int = 0             # [F4] сделок за последние N свечей
    recent_vol_usdt: float = 0.0      # [F4] объём за последние N свечей
    recent_vol_ratio: float = 0.0     # [F4] доля последних N свечей в общем объёме
    is_recently_active: bool = True    # [F4] False = монета сейчас мертва

    @property
    def tags(self) -> list[str]:
        t = []
        if self.has_big_size:
            t.append(f"[Big Size ×{self.big_size_ratio:.0f}]")
        if self.has_aged:
            age_h = self.range_age_min / 60
            t.append(f"[Aged {age_h:.1f}h]" if age_h >= 1 else f"[Aged {self.range_age_min:.0f}m]")
        if self.has_rejected:
            t.append(f"[Rejected ×{self.bounces}]")
        if self.top_bounces > 0:
            t.append(f"[Top ×{self.top_bounces}]")
        return t

    @property
    def tags_str(self) -> str:
        return " ".join(self.tags) if self.tags else "—"

    @property
    def mexc_url(self) -> str:
        return f"https://www.mexc.com/exchange/{self.symbol.replace('USDT', '_USDT')}"


# ═══════════════════════════════════════════════
# Основной класс сканера
# ═══════════════════════════════════════════════

class RangeBounceScanner:
    """Сканер монет в ценовом коридоре с признаками накопления"""

    def __init__(self, client, trades_buffer=None):
        self.client = client
        self.trades_buffer = trades_buffer
        self._cache: dict[str, tuple[float, RangeResult]] = {}  # symbol → (timestamp, result)
        self._cache_ttl = 60  # секунд

    def _parse_klines(self, raw) -> pd.DataFrame:
        if not raw or not isinstance(raw, list):
            return pd.DataFrame()
        rows = []
        for k in raw:
            if not isinstance(k, (list, tuple)) or len(k) < 6:
                continue
            try:
                rows.append({
                    "open_time": float(k[0]),
                    "open":  float(k[1]),
                    "high":  float(k[2]),
                    "low":   float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "quote_vol": float(k[7]) if len(k) > 7 else float(k[5]) * float(k[4]),
                    "trades": int(k[8]) if len(k) > 8 else 0,
                })
            except (ValueError, TypeError, IndexError):
                continue
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["time"] = pd.to_datetime(df["open_time"], unit="ms")
        return df

    def _detect_range(self, df: pd.DataFrame, cfg: dict) -> Optional[dict]:
        """
        Определяет ценовой канал на свечах.
        Возвращает dict с low/high/age или None если не рендж.
        """
        if df.empty or len(df) < 5:
            return None

        range_low  = df["low"].min()
        range_high = df["high"].max()

        if range_low <= 0:
            return None

        range_pct = (range_high - range_low) / range_low * 100

        min_r = cfg["min_range_pct"]
        max_r = cfg["max_range_pct"]

        if not (min_r <= range_pct <= max_r):
            return None

        # Возраст ренджа — ищем с какой свечи установился текущий диапазон
        tf_min = TIMEFRAME_MINUTES.get(cfg["timeframe"], 5)
        range_age_candles = _find_range_age(df, range_low, range_high, cfg)
        range_age_min = range_age_candles * tf_min

        return {
            "low":      range_low,
            "high":     range_high,
            "range_pct": range_pct,
            "age_min":  range_age_min,
            "age_candles": range_age_candles,
        }

    def analyze_symbol(self, symbol: str, cfg: dict) -> Optional[RangeResult]:
        """
        Анализирует одну монету. Возвращает RangeResult или None.

        Порядок фильтров (от дешёвого к дорогому):
          1. klines + базовый рендж
          2. [F4] Recent activity — монета сейчас мертва?
          3. [F4] Средняя активность (trades_per_min / hourly_vol)
          4. [F1] Dead book — стакан живой?
          5. [F2] Top bounces — цена доходит до верха?
          6. [F3] Range utilization — цена ходит по всему рендж?
          7. Бонусные теги + скор
        """
        # Кеш
        cached = self._cache.get(symbol)
        if cached and time.time() - cached[0] < self._cache_ttl:
            return cached[1]

        tf_api    = TIMEFRAME_TO_API.get(cfg["timeframe"], "5m")
        n_candles = cfg["candles_lookback"]

        raw = self.client.get_klines(symbol, tf_api, n_candles)
        df  = self._parse_klines(raw)

        if df.empty or len(df) < 10:
            return None

        # ── Базовый рендж ──
        rng = self._detect_range(df, cfg)
        if rng is None:
            return None

        range_low   = rng["low"]
        range_high  = rng["high"]
        range_pct   = rng["range_pct"]
        range_width = range_high - range_low

        current_price = float(df["close"].iloc[-1])
        pos_in_range  = (
            (current_price - range_low) / range_width * 100
            if range_width > 0 else 50
        )

        tf_min = TIMEFRAME_MINUTES.get(cfg["timeframe"], 5)

        # ────────────────────────────────────────────────────────────────
        # [F4] RECENT ACTIVITY — проверяем ПОСЛЕДНИЕ N свечей отдельно.
        #
        # Проблема: монета может торговаться 30 минут из 24 часов.
        # Средний trades_per_min за 4 часа будет выглядеть нормально,
        # но прямо сейчас она мертва.
        #
        # Решение: смотрим последние recent_candles свечей независимо.
        # Два условия (оба должны выполняться при recent_min_trades > 0):
        #   A) Суммарно сделок >= recent_min_trades (не 0-стрип последних свечей)
        #   B) Доля объёма последних N свечей в общем >= recent_vol_ratio
        #      Если всего объём за последние 3 свечи составляет <5% от всего
        #      lookback-объёма — монета сейчас стоит.
        # ────────────────────────────────────────────────────────────────
        recent_n        = max(1, cfg.get("recent_candles", 3))
        recent_min_tr   = cfg.get("recent_min_trades", 3)
        recent_vol_thr  = cfg.get("recent_vol_ratio", 0.05)

        recent_df       = df.tail(recent_n)
        recent_trades   = int(recent_df["trades"].sum())
        recent_vol      = float(recent_df["quote_vol"].sum())

        total_vol_all   = float(df["quote_vol"].sum())
        recent_vol_ratio = (
            recent_vol / total_vol_all if total_vol_all > 0 else 0.0
        )

        is_recently_active = True
        if recent_min_tr > 0:
            # A) абсолютный минимум сделок в последних N свечах
            if recent_trades < recent_min_tr:
                is_recently_active = False
            # B) доля последнего объёма: ниже порога → монета сейчас спит
            # Нормальная монета даёт примерно recent_n/n_candles доли.
            # Если recent_vol_ratio < recent_vol_thr — аномально тихо сейчас.
            if recent_vol_thr > 0 and recent_vol_ratio < recent_vol_thr:
                is_recently_active = False

        if not is_recently_active:
            return None

        # ── Средняя активность за lookback (старая проверка, оставляем) ──
        last_candles_5m = max(1, 5 // tf_min)
        avg_df          = df.tail(last_candles_5m)
        total_trades_5m = int(avg_df["trades"].sum())
        elapsed_min     = last_candles_5m * tf_min
        trades_per_min  = total_trades_5m / elapsed_min if elapsed_min > 0 else 0

        last_candles_1h = max(1, 60 // tf_min)
        hourly_vol      = float(df.tail(last_candles_1h)["quote_vol"].sum())

        min_tpm  = cfg["min_trades_per_min"]
        min_hvol = cfg["min_volume_per_hour"]
        active   = (trades_per_min >= min_tpm) or (hourly_vol >= min_hvol)
        if not active:
            return None

        # ────────────────────────────────────────────────────────────────
        # [F1] DEAD BOOK FILTER
        # Исключает монеты типа PXUSDT: мёртвый стакан, ASK/BID перекос >10x,
        # стенки далеко от mid, < 3 уровней рядом с рынком.
        # Запрашиваем стакан только если klines и рендж уже прошли проверку
        # (экономим API-вызовы).
        # ────────────────────────────────────────────────────────────────
        if cfg.get("dead_book_filter", True) and _BOOK_QUALITY_AVAILABLE:
            ob_raw = self.client.get_order_book(symbol, limit=50)
            if ob_raw:
                bids = [
                    (float(p), float(q))
                    for p, q in ob_raw.get("bids", [])
                    if float(p) > 0 and float(q) > 0
                ]
                asks = [
                    (float(p), float(q))
                    for p, q in ob_raw.get("asks", [])
                    if float(p) > 0 and float(q) > 0
                ]
                mid = (range_low + range_high) / 2
                bq  = check_book_quality(bids, asks, mid)
                if bq.is_dead:
                    return None  # FLAG_DEAD_BOOK

        # ── Параметры зон касания ──
        tolerance     = range_width * cfg["touch_tolerance_pct"] / 100
        lower_touch   = range_low  + tolerance
        upper_touch   = range_high - tolerance
        bounce_confirm = range_width * cfg["bounce_confirm_pct"] / 100

        # ── Отскоки от нижней границы ──
        bounce_events, bounces = _count_bounces(
            df, range_low, lower_touch, bounce_confirm, cfg
        )

        # ────────────────────────────────────────────────────────────────
        # [F2] TOP BOUNCES — двусторонние отскоки.
        # Исключает монеты типа MANA: цена зависает у одной стенки и никогда
        # не доходит до другой.  Нам нужна монета, которую можно купить внизу
        # и продать вверху — значит цена ДОЛЖНА ходить к обоим уровням.
        # ────────────────────────────────────────────────────────────────
        top_bounce_events, top_bounces = _count_top_bounces(
            df, range_high, upper_touch, bounce_confirm, cfg
        )

        min_top = cfg.get("min_top_bounces", 1)
        if min_top > 0 and top_bounces < min_top:
            return None  # цена никогда не доходила до верхней границы

        # ────────────────────────────────────────────────────────────────
        # [F3] RANGE UTILIZATION
        # Измеряем равномерность хождения цены по рендж.
        # Монета застрявшая в нижних 20% рендж — не торгуема для стратегии
        # "купи у дна, продай у верха".
        # Метрика: sqrt(lower_visits% × upper_visits%)
        # Геометрическое среднее сильно штрафует перекос: 40%×1% даёт 0.063,
        # тогда как арифметическое дало бы 20.5%.
        # ────────────────────────────────────────────────────────────────
        util = _calc_range_utilization(df, range_low, range_high)
        min_util = cfg.get("min_utilization", 0.10)
        if min_util > 0 and util["utilization"] < min_util:
            return None  # цена застряла в одном углу рендж

        # ── Big Size — объём у нижней границы ──
        has_big_size, avg_vol, big_vol, big_ratio = _check_big_size(
            df, range_low, lower_touch, cfg
        )

        # ── Возраст ──
        has_aged = rng["age_min"] >= cfg["min_range_age_min"]

        # ── Rejected ──
        min_b = cfg["min_bounces"]
        max_b = cfg["max_bounces"]
        has_rejected = min_b <= bounces <= max_b

        # Скор = сумма бонусов
        score = int(has_big_size) + int(has_aged) + int(has_rejected)

        result = RangeResult(
            symbol=symbol,
            current_price=current_price,
            range_low=range_low,
            range_high=range_high,
            range_pct=round(range_pct, 2),
            position_in_range=round(pos_in_range, 1),
            range_age_min=round(rng["age_min"], 0),
            bounces=bounces,
            bounce_events=bounce_events,
            has_big_size=has_big_size,
            has_aged=has_aged,
            has_rejected=has_rejected,
            avg_vol_usdt=round(avg_vol, 2),
            big_size_usdt=round(big_vol, 2),
            big_size_ratio=round(big_ratio, 1),
            trades_per_min=round(trades_per_min, 2),
            hourly_vol_usdt=round(hourly_vol, 0),
            score=score,
            # v2.0
            top_bounces=top_bounces,
            top_bounce_events=top_bounce_events,
            range_utilization=round(util["utilization"], 3),
            lower_visits_pct=round(util["lower_visits_pct"], 1),
            upper_visits_pct=round(util["upper_visits_pct"], 1),
            recent_trades=recent_trades,
            recent_vol_usdt=round(recent_vol, 0),
            recent_vol_ratio=round(recent_vol_ratio, 3),
            is_recently_active=is_recently_active,
        )

        self._cache[symbol] = (time.time(), result)
        return result

    def scan(self, symbols: list[str], cfg: dict,
             progress_cb=None) -> list[RangeResult]:
        """Сканирует список символов. progress_cb(done, total)."""
        results = []
        total = len(symbols)
        for i, sym in enumerate(symbols):
            try:
                r = self.analyze_symbol(sym, cfg)
                if r is not None:
                    results.append(r)
            except Exception:
                pass
            if progress_cb:
                progress_cb(i + 1, total)

        # Сортировка: сначала по скору (убыв.), потом по позиции в рендже (ближе к дну)
        results.sort(key=lambda r: (-r.score, r.position_in_range))
        return results[:cfg.get("top_results", 50)]

    def get_chart(self, symbol: str, cfg: dict, result: RangeResult) -> Optional[go.Figure]:
        """Строит свечной график с разметкой ренджа и отскоков."""
        tf_api = TIMEFRAME_TO_API.get(cfg["timeframe"], "5m")
        raw = self.client.get_klines(symbol, tf_api, cfg["candles_lookback"])
        df = self._parse_klines(raw)
        if df.empty:
            return None
        return _build_range_chart(df, result, cfg)


# ═══════════════════════════════════════════════
# Вспомогательные функции
# ═══════════════════════════════════════════════

def _find_range_age(df: pd.DataFrame, range_low: float, range_high: float, cfg: dict) -> int:
    """
    Возвращает кол-во свечей назад с момента установления текущего канала.
    Идём с конца назад и ищем первую свечу, которая пробила бы диапазон.
    """
    n = len(df)
    for i in range(n - 1, -1, -1):
        row = df.iloc[i]
        if row["low"] < range_low * 0.995 or row["high"] > range_high * 1.005:
            return n - i - 1  # количество свечей ПОСЛЕ этой
    return n  # весь период в рендже


def _count_bounces(df: pd.DataFrame, range_low: float, lower_touch: float,
                   bounce_confirm: float, cfg: dict) -> tuple[list, int]:
    """
    Подсчитывает касания нижней границы с отскоком.
    Касание: low <= lower_touch
    Отскок: следующие N свечей поднимаются выше low + bounce_confirm
    """
    events = []
    n = len(df)
    i = 0
    while i < n:
        row = df.iloc[i]
        if row["low"] <= lower_touch:
            # Ищем подтверждение отскока (следующие до 10 свечей)
            confirm_price = row["low"] + bounce_confirm
            bounced = False
            best_high = row["low"]
            for j in range(i + 1, min(i + 11, n)):
                fwd = df.iloc[j]
                best_high = max(best_high, fwd["high"])
                if fwd["high"] >= confirm_price:
                    bounced = True
                    break

            if bounced:
                bounce_pct = (best_high - row["low"]) / max(bounce_confirm, 1e-10) * 100
                ts = int(row["open_time"]) if "open_time" in row else 0
                events.append(BounceEvent(
                    candle_idx=i,
                    low_price=row["low"],
                    high_after=best_high,
                    bounce_pct=round(bounce_pct, 1),
                    time_ms=ts,
                ))
                i += 3  # пропускаем несколько свечей после отскока
                continue
        i += 1

    return events, len(events)


def _count_top_bounces(df: pd.DataFrame, range_high: float, upper_touch: float,
                       bounce_confirm: float, cfg: dict) -> tuple[list, int]:
    """
    [F2] Считает касания ВЕРХНЕЙ границы с откатом вниз.

    Касание: high >= upper_touch (верхняя граница минус допуск).
    Откат: следующие N свечей опускаются ниже high - bounce_confirm.

    Симметричный аналог _count_bounces — нужен чтобы убедиться, что цена
    реально добирается до верхней стенки и отскакивает, а не просто
    зависает в нижней половине рендж (MANA-тип).
    """
    events = []
    n = len(df)
    i = 0
    while i < n:
        row = df.iloc[i]
        if row["high"] >= upper_touch:
            confirm_price = row["high"] - bounce_confirm
            bounced       = False
            best_low      = row["high"]
            for j in range(i + 1, min(i + 11, n)):
                fwd      = df.iloc[j]
                best_low = min(best_low, fwd["low"])
                if fwd["low"] <= confirm_price:
                    bounced = True
                    break
            if bounced:
                bounce_pct = (row["high"] - best_low) / max(bounce_confirm, 1e-10) * 100
                ts = int(row["open_time"]) if "open_time" in row else 0
                events.append(BounceEvent(
                    candle_idx=i,
                    low_price=row["high"],    # переиспользуем поле: здесь это high касания
                    high_after=best_low,      # переиспользуем: здесь это low отката
                    bounce_pct=round(bounce_pct, 1),
                    time_ms=ts,
                ))
                i += 3
                continue
        i += 1
    return events, len(events)


def _calc_range_utilization(
    df: pd.DataFrame, range_low: float, range_high: float
) -> dict:
    """
    [F3] Измеряет равномерность хождения цены по рендж.

    Делит рендж на три зоны: нижние 30%, средние 40%, верхние 30%.
    Считает долю свечей, касавшихся каждой из крайних зон.

    Метрика utilization = sqrt(lower_visits% × upper_visits%) / 100
      - Геометрическое среднее сильно штрафует перекос.
      - Пример PAXIUSDT (хорошая): lower=45%, upper=38% → util=0.41
      - Пример MANA-тип (плохая): lower=50%, upper=3%  → util=0.12
      - Пример застрявшей монеты:  lower=5%,  upper=2%  → util=0.03

    Returns:
        lower_visits_pct: % свечей, касавшихся нижней зоны (low <= порог)
        upper_visits_pct: % свечей, касавшихся верхней зоны (high >= порог)
        utilization: 0.0–1.0
    """
    if df.empty or range_high <= range_low:
        return {"lower_visits_pct": 0.0, "upper_visits_pct": 0.0, "utilization": 0.0}

    rw          = range_high - range_low
    lower_zone  = range_low  + rw * 0.30   # нижние 30%
    upper_zone  = range_high - rw * 0.30   # верхние 30%

    n            = len(df)
    lower_visits = int((df["low"]  <= lower_zone).sum())
    upper_visits = int((df["high"] >= upper_zone).sum())

    lower_pct = lower_visits / n * 100
    upper_pct = upper_visits / n * 100

    # Геометрическое среднее — штрафует перекос
    utilization = math.sqrt((lower_pct / 100) * (upper_pct / 100))

    return {
        "lower_visits_pct": round(lower_pct, 1),
        "upper_visits_pct": round(upper_pct, 1),
        "utilization":      round(utilization, 3),
    }


def _check_big_size(df: pd.DataFrame, range_low: float, lower_touch: float,
                    cfg: dict) -> tuple[bool, float, float, float]:
    """
    Проверяет наличие крупных объёмов у нижней границы.
    Возвращает: (has_big_size, avg_vol_usdt, max_vol_usdt, ratio)
    """
    multiplier = cfg["vol_multiplier"]
    avg_period_min = cfg["vol_avg_period_min"]
    tf_min = TIMEFRAME_MINUTES.get(cfg["timeframe"], 5)
    avg_candles = max(1, avg_period_min // tf_min)

    # Средний объём за последние N свечей
    recent = df.tail(avg_candles)
    vols = [v for v in recent["quote_vol"].tolist() if v > 0]
    if not vols:
        return False, 0.0, 0.0, 0.0
    avg_vol = statistics.mean(vols)

    # Ищем свечи у нижней границы с аномальным объёмом
    near_low_df = df[df["low"] <= lower_touch]
    if near_low_df.empty:
        return False, avg_vol, 0.0, 0.0

    max_vol_near = float(near_low_df["quote_vol"].max())
    ratio = max_vol_near / avg_vol if avg_vol > 0 else 0.0

    has_big = ratio >= multiplier
    return has_big, avg_vol, max_vol_near, ratio


# ═══════════════════════════════════════════════
# Построение графика
# ═══════════════════════════════════════════════

def _build_range_chart(df: pd.DataFrame, result: RangeResult, cfg: dict) -> go.Figure:
    """Свечной график с разметкой ренджа, отскоков и уровней."""
    tolerance = (result.range_high - result.range_low) * cfg["touch_tolerance_pct"] / 100

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.75, 0.25],
                        vertical_spacing=0.03)

    # Свечи
    colors_up   = ["#00d1a0" if c >= o else "#ff4466"
                   for c, o in zip(df["close"], df["open"])]
    colors_down = colors_up

    fig.add_trace(go.Candlestick(
        x=df["time"],
        open=df["open"], high=df["high"],
        low=df["low"],   close=df["close"],
        increasing_line_color="#00d1a0",
        decreasing_line_color="#ff4466",
        name="Цена",
    ), row=1, col=1)

    # Границы ренджа
    fig.add_hline(y=result.range_high, line_color="#ff8800",
                  line_dash="dash", line_width=1.5,
                  annotation_text="▲ Верх", annotation_position="left",
                  row=1, col=1)
    fig.add_hline(y=result.range_low,  line_color="#00bfff",
                  line_dash="dash", line_width=1.5,
                  annotation_text="▼ Низ",  annotation_position="left",
                  row=1, col=1)
    # Зона допуска (нижняя)
    fig.add_hrect(
        y0=result.range_low, y1=result.range_low + tolerance,
        fillcolor="rgba(0,191,255,0.08)", line_width=0,
        row=1, col=1
    )

    # Отскоки
    for ev in result.bounce_events:
        if ev.candle_idx < len(df):
            t = df["time"].iloc[ev.candle_idx]
            fig.add_trace(go.Scatter(
                x=[t], y=[ev.low_price * 0.998],
                mode="markers",
                marker=dict(symbol="triangle-up", size=12, color="#ffdd00"),
                showlegend=False, name="Отскок",
            ), row=1, col=1)

    # Объём
    vol_colors = ["#00d1a0" if c >= o else "#ff4466"
                  for c, o in zip(df["close"], df["open"])]
    fig.add_trace(go.Bar(
        x=df["time"], y=df["quote_vol"],
        marker_color=vol_colors,
        name="Объём", opacity=0.7,
    ), row=2, col=1)

    # Оформление
    tags_str = " ".join(result.tags) if result.tags else "нет бонусов"
    fig.update_layout(
        title=dict(
            text=f"{result.symbol} — Рендж {result.range_pct:.1f}% | {tags_str}",
            font=dict(color="#e0e0e0", size=14),
        ),
        xaxis_rangeslider_visible=False,
        paper_bgcolor="#0f1117",
        plot_bgcolor="#0f1117",
        font=dict(color="#aaaaaa", size=11),
        height=500,
        margin=dict(l=50, r=20, t=50, b=20),
        showlegend=False,
    )
    fig.update_xaxes(gridcolor="#1e2230", showgrid=True)
    fig.update_yaxes(gridcolor="#1e2230", showgrid=True)
    return fig


# ═══════════════════════════════════════════════
# Streamlit UI
# ═══════════════════════════════════════════════

def render_range_bounce_tab(client, trades_buffer=None):
    """
    Рендерит вкладку Range Bounce Scanner в Streamlit.
    Вызывать из app.py:
        from range_bounce_scanner import render_range_bounce_tab
        render_range_bounce_tab(client, st.session_state.trades_buffer)
    """
    st.markdown("### 📊 Range Bounce Scanner v2.0")
    st.caption(
        "Ищет монеты с **двусторонним** рендж-паттерном (цена ходит от дна к верху и обратно). "
        "Три тега: **[Big Size]** — крупный объём у дна, "
        "**[Aged]** — рендж существует долго, "
        "**[Rejected ×N]** — многократные отскоки."
    )

    # ── Настройки ──
    with st.expander("⚙️ Параметры сканирования", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Рендж**")
            tf = st.selectbox("Таймфрейм", ["1m","3m","5m","15m","30m","1h","4h"],
                              index=2, key="rbs_tf")
            n_candles = st.slider("Свечей для анализа", 20, 200, 48, key="rbs_candles")
            min_range = st.slider("Мин. ширина ренджа, %", 1.0, 10.0, 2.0, 0.5, key="rbs_min_r")
            max_range = st.slider("Макс. ширина ренджа, %", 5.0, 50.0, 15.0, 1.0, key="rbs_max_r")
            tolerance = st.slider("Допуск нечёткости (% от ширины)", 1.0, 20.0, 5.0, 1.0, key="rbs_tol")

        with c2:
            st.markdown("**Активность**")
            min_tpm  = st.slider("Мин. сделок/мин (среднее)", 1, 50, 5, key="rbs_tpm")
            min_hvol = st.number_input("Мин. объём/час ($)", 100, 100_000, 5000, 500, key="rbs_hvol")
            st.markdown("**Big Size**")
            vol_mult   = st.slider("Множитель объёма", 2.0, 30.0, 10.0, 1.0, key="rbs_vmult")
            vol_period = st.slider("Период для среднего (мин)", 5, 60, 5, 5, key="rbs_vperiod")

        with c3:
            st.markdown("**Бонусы**")
            min_age      = st.slider("Мин. возраст ренджа (мин)", 10, 240, 60, 10, key="rbs_age")
            min_bounces  = st.slider("Мин. отскоков (низ)", 1, 5, 3, key="rbs_minb")
            max_bounces  = st.slider("Макс. отскоков (низ)", 5, 20, 9, key="rbs_maxb")
            bounce_confirm = st.slider("Мин. отскок (% от ширины)", 5.0, 50.0, 10.0, 5.0, key="rbs_bc")
            top_n        = st.slider("Топ-N результатов", 10, 200, 50, 10, key="rbs_topn")

        st.markdown("---")
        st.markdown("**🔍 Фильтры качества (v2.0)**")
        fc1, fc2, fc3, fc4 = st.columns(4)

        with fc1:
            st.markdown("**[F1] Dead Book**")
            dead_book_on = st.toggle(
                "Фильтр мёртвого стакана", value=True, key="rbs_deadbook",
                help="Исключает монеты с ASK/BID перекосом >10x, "
                     "стенками далеко от mid, <3 уровней рядом с рынком (PXUSDT-тип)"
            )

        with fc2:
            st.markdown("**[F2] Двустор. отскоки**")
            min_top_bounces = st.slider(
                "Мин. отскоков сверху", 0, 5, 1, key="rbs_topb",
                help="0 = выключено. ≥1 = требуем что цена отскакивала "
                     "и от верхней границы (исключает MANA-тип)"
            )

        with fc3:
            st.markdown("**[F3] Utilization**")
            min_util = st.slider(
                "Мин. utilization", 0.0, 0.5, 0.10, 0.01, key="rbs_util",
                help="√(lower% × upper%) < порога → монета застряла в углу рендж. "
                     "0.0 = выключено, 0.10 = мягко, 0.20 = строго"
            )

        with fc4:
            st.markdown("**[F4] Текущая активность**")
            recent_candles   = st.slider(
                "Последних свечей", 1, 10, 3, key="rbs_rc",
                help="Проверяем N последних свечей ОТДЕЛЬНО. "
                     "Защита от монет, активных 30 мин из 24 часов."
            )
            recent_min_trades = st.slider(
                "Мин. сделок за N свечей", 0, 30, 3, key="rbs_rmt",
                help="0 = выключено. Если последние N свечей дали "
                     "меньше X сделок — монета сейчас мертва."
            )
            recent_vol_ratio = st.slider(
                "Мин. доля последнего объёма", 0.0, 0.20, 0.05, 0.01,
                key="rbs_rvr",
                help="Мин. доля объёма последних N свечей от общего "
                     "lookback-объёма. <5% → монета сейчас спит."
            )

    cfg = {
        "timeframe":           tf,
        "candles_lookback":    n_candles,
        "min_range_pct":       min_range,
        "max_range_pct":       max_range,
        "touch_tolerance_pct": tolerance,
        "min_trades_per_min":  min_tpm,
        "min_volume_per_hour": min_hvol,
        "vol_multiplier":      vol_mult,
        "vol_avg_period_min":  vol_period,
        "min_range_age_min":   min_age,
        "min_bounces":         min_bounces,
        "max_bounces":         max_bounces,
        "bounce_confirm_pct":  bounce_confirm,
        "top_results":         top_n,
        # v2.0
        "dead_book_filter":    dead_book_on,
        "min_top_bounces":     min_top_bounces,
        "min_utilization":     min_util,
        "recent_candles":      recent_candles,
        "recent_min_trades":   recent_min_trades,
        "recent_vol_ratio":    recent_vol_ratio,
    }

    # ── Запуск скана ──
    run_col, info_col = st.columns([1, 3])
    with run_col:
        run_scan = st.button("🔍 Запустить скан", type="primary", width="stretch",
                             key="rbs_run")
    with info_col:
        last_scan = st.session_state.get("rbs_last_scan_time")
        if last_scan:
            elapsed = int(time.time() - last_scan)
            st.caption(f"Последний скан: {elapsed} сек. назад | "
                       f"Найдено: {len(st.session_state.get('rbs_results', []))} монет")

    if run_scan:
        # Получаем список монет (от основного скана или запрашиваем сами)
        symbols = _get_symbols_for_scan(client)
        if not symbols:
            st.error("Не удалось получить список монет. Сначала запусти основной скан.")
        else:
            scanner = RangeBounceScanner(client, trades_buffer)
            progress_bar = st.progress(0, text="Сканирование...")

            def _progress(done, total):
                pct = done / total
                progress_bar.progress(pct, text=f"Сканирование {done}/{total}...")

            with st.spinner(f"Анализирую {len(symbols)} монет..."):
                results = scanner.scan(symbols, cfg, progress_cb=_progress)

            progress_bar.empty()
            st.session_state["rbs_results"] = results
            st.session_state["rbs_last_scan_time"] = time.time()
            st.session_state["rbs_scanner"] = scanner
            st.session_state["rbs_cfg"] = cfg
            st.rerun()

    # ── Результаты ──
    results: list[RangeResult] = st.session_state.get("rbs_results", [])
    if not results:
        if not st.session_state.get("rbs_last_scan_time"):
            st.info("👆 Нажми «Запустить скан» для поиска монет в рендже.")
        else:
            st.warning("😔 Монеты с рендж-паттерном не найдены. Попробуй изменить параметры.")
        return

    # ── Метрики ──
    total_found   = len(results)
    score3 = sum(1 for r in results if r.score == 3)
    score2 = sum(1 for r in results if r.score == 2)
    score1 = sum(1 for r in results if r.score == 1)
    score0 = sum(1 for r in results if r.score == 0)

    mc = st.columns(5)
    mc[0].metric("Всего монет", total_found)
    mc[1].metric("🎯 3 бонуса", score3)
    mc[2].metric("⭐ 2 бонуса", score2)
    mc[3].metric("✅ 1 бонус",  score1)
    mc[4].metric("📊 0 бонусов", score0)

    # ── Уведомление при 3+ бонусах ──
    top_3 = [r for r in results if r.score >= 3]
    if top_3:
        syms_str = ", ".join(r.symbol for r in top_3[:5])
        st.success(f"🔔 Монеты с 3+ бонусами: **{syms_str}**")

    # ── Фильтр по скору ──
    filter_col1, filter_col2 = st.columns([2, 3])
    with filter_col1:
        min_score_filter = st.selectbox(
            "Мин. бонусов", [0, 1, 2, 3], index=0, key="rbs_filter_score"
        )
    with filter_col2:
        show_tags = st.multiselect(
            "Фильтр по тегу", ["Big Size", "Aged", "Rejected"],
            default=[], key="rbs_filter_tags"
        )

    filtered = [r for r in results if r.score >= min_score_filter]
    if "Big Size" in show_tags:
        filtered = [r for r in filtered if r.has_big_size]
    if "Aged" in show_tags:
        filtered = [r for r in filtered if r.has_aged]
    if "Rejected" in show_tags:
        filtered = [r for r in filtered if r.has_rejected]

    if not filtered:
        st.info("Нет монет с выбранными фильтрами.")
        return

    # ── Таблица результатов ──
    def _fmt_price(p):
        if p >= 1000: return f"{p:,.0f}"
        if p >= 1:    return f"{p:.4f}"
        if p >= 0.01: return f"{p:.6f}"
        return f"{p:.8f}"

    def _fmt_usd(v):
        if v <= 0:          return "—"
        if v >= 1_000_000:  return f"${v/1_000_000:.1f}M"
        if v >= 1000:       return f"${v/1000:.1f}K"
        return f"${v:,.0f}"

    rows = []
    for r in filtered:
        age_str = (f"{r.range_age_min/60:.1f}ч"
                   if r.range_age_min >= 60 else f"{r.range_age_min:.0f}м")
        # Индикатор текущей активности
        activity_icon = "🟢" if r.is_recently_active else "🔴"
        rows.append({
            "Скор":      r.score,
            "Пара":      r.symbol,
            "Цена":      _fmt_price(r.current_price),
            "Рендж %":   f"{r.range_pct:.1f}%",
            "Позиция":   f"{r.position_in_range:.0f}%",
            "↓Отск":     r.bounces,
            "↑Отск":     r.top_bounces,
            "Util":      f"{r.range_utilization:.2f}",
            "Сейчас":    f"{activity_icon} {r.recent_trades}сд",
            "Возраст":   age_str,
            "Vol/час":   _fmt_usd(r.hourly_vol_usdt),
            "Теги":      r.tags_str,
        })

    df_table = pd.DataFrame(rows)

    # Цветовое выделение по скору
    def _row_style(row):
        score = row["Скор"]
        if score == 3:   return ["background-color: rgba(255,200,0,0.15)"] * len(row)
        if score == 2:   return ["background-color: rgba(0,200,150,0.10)"] * len(row)
        return [""] * len(row)

    styled = df_table.style.apply(_row_style, axis=1)
    st.dataframe(styled, hide_index=True, use_container_width=True,
                 height=min(len(filtered) * 35 + 45, 600))

    # Скачать CSV
    st.download_button(
        "📥 Скачать CSV",
        data=df_table.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"range_bounce_{datetime.now().strftime('%H%M')}.csv",
        mime="text/csv",
        key="rbs_csv",
    )

    # ── Детальный просмотр ──
    st.markdown("---")
    st.markdown("#### 🔍 Детальный анализ")

    sym_options = [r.symbol for r in filtered]
    sel_col1, sel_col2 = st.columns([3, 1])
    with sel_col1:
        selected_sym = st.selectbox("Выбери монету для анализа",
                                    sym_options, key="rbs_detail_sym")
    with sel_col2:
        show_chart = st.button("📈 Показать график", key="rbs_show_chart")

    sel_result = next((r for r in filtered if r.symbol == selected_sym), None)

    if sel_result:
        # Карточка
        dc1, dc2, dc3, dc4 = st.columns(4)
        dc1.metric("Текущая цена",   _fmt_price(sel_result.current_price))
        dc2.metric("Ширина ренджа",  f"{sel_result.range_pct:.1f}%")
        dc3.metric("Позиция в рендже", f"{sel_result.position_in_range:.0f}%",
                   help="0% = у нижней границы, 100% = у верхней")
        dc4.metric("Отскоков ↓/↑",  f"{sel_result.bounces} / {sel_result.top_bounces}")

        dc5, dc6, dc7, dc8 = st.columns(4)
        age_str = (f"{sel_result.range_age_min/60:.1f}ч"
                   if sel_result.range_age_min >= 60 else f"{sel_result.range_age_min:.0f}м")
        dc5.metric("Возраст ренджа", age_str)
        dc6.metric("Big Size ×",     f"{sel_result.big_size_ratio:.1f}x" if sel_result.has_big_size else "—")
        dc7.metric("Utilization",    f"{sel_result.range_utilization:.2f}",
                   help=f"Низ: {sel_result.lower_visits_pct:.0f}% свечей | "
                        f"Верх: {sel_result.upper_visits_pct:.0f}% свечей")
        dc8.metric("Объём/час",      _fmt_usd(sel_result.hourly_vol_usdt))

        # Блок текущей активности (F4)
        act_col1, act_col2, act_col3 = st.columns(3)
        recent_n_label = st.session_state.get("rbs_rc", 3)
        act_icon = "🟢 Активна" if sel_result.is_recently_active else "🔴 Сейчас мертва"
        act_col1.metric(f"Статус сейчас", act_icon)
        act_col2.metric(f"Сделок за {recent_n_label} свечи",  sel_result.recent_trades)
        act_col3.metric("Доля текущ. объёма",
                        f"{sel_result.recent_vol_ratio*100:.1f}%",
                        help="Доля объёма последних N свечей от всего lookback. "
                             "<5% = монета сейчас спит")

        # Теги
        if sel_result.tags:
            tags_html = " ".join(
                f'<span style="background:#1e3a5f;color:#4db8ff;'
                f'padding:3px 10px;border-radius:4px;font-size:13px;">{t}</span>'
                for t in sel_result.tags
            )
            st.markdown(tags_html, unsafe_allow_html=True)

        # Граница входа
        entry_buffer_pct = 0.005
        entry_zone = sel_result.range_low * (1 + entry_buffer_pct)
        target_zone = sel_result.range_low + (sel_result.range_high - sel_result.range_low) * 0.5
        st.info(
            f"📍 **Вход:** ≤ {_fmt_price(entry_zone)} | "
            f"🎯 **Цель:** {_fmt_price(target_zone)} | "
            f"🛑 **Стоп:** {_fmt_price(sel_result.range_low * 0.99)}"
        )

        # История отскоков
        if sel_result.bounce_events:
            st.markdown("**История отскоков:**")
            bounce_rows = []
            for i, ev in enumerate(sel_result.bounce_events, 1):
                ts = datetime.fromtimestamp(ev.time_ms / 1000).strftime("%H:%M") if ev.time_ms else "—"
                bounce_rows.append({
                    "#": i,
                    "Время": ts,
                    "Low цена": _fmt_price(ev.low_price),
                    "Макс после": _fmt_price(ev.high_after),
                    "Рост (% от ширины)": f"{ev.bounce_pct:.0f}%",
                })
            st.dataframe(pd.DataFrame(bounce_rows), hide_index=True, use_container_width=True)

        # Ссылка на MEXC
        st.markdown(f"[🔗 Открыть {selected_sym} на MEXC]({sel_result.mexc_url})")

        # График
        if show_chart or st.session_state.get("rbs_chart_sym") == selected_sym:
            st.session_state["rbs_chart_sym"] = selected_sym
            scanner: RangeBounceScanner = st.session_state.get("rbs_scanner")
            saved_cfg = st.session_state.get("rbs_cfg", cfg)
            if scanner:
                with st.spinner("Строю график..."):
                    fig = scanner.get_chart(selected_sym, saved_cfg, sel_result)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Не удалось построить график.")


# ═══════════════════════════════════════════════
# Вспомогательные
# ═══════════════════════════════════════════════

def _get_symbols_for_scan(client) -> list[str]:
    """Получает список символов для скана."""
    # Пробуем из session_state (если основной скан уже запускался)
    if "scan_results" in st.session_state:
        scan_res = st.session_state["scan_results"]
        if scan_res and isinstance(scan_res, list):
            syms = [r.get("symbol") or r.get("Пара") for r in scan_res if isinstance(r, dict)]
            syms = [s for s in syms if s and s.endswith("USDT")]
            if syms:
                return syms

    # Фоллбэк: тикеры напрямую
    try:
        tickers = client.get_all_tickers_24h()
        if not tickers or not isinstance(tickers, list):
            return []
        filtered = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            vol = float(t.get("quoteVolume", 0) or 0)
            if 100 <= vol <= 10_000_000:
                filtered.append((sym, vol))
        filtered.sort(key=lambda x: x[1], reverse=True)
        return [s for s, _ in filtered[:300]]
    except Exception:
        return []
