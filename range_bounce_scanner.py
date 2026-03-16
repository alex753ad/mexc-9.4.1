"""
Range Bounce Scanner v2.0

Ищет монеты в рендже с признаками накопления у нижней границы.
Ключевые улучшения v2.0:
  - Мин/макс объём в час в настройках
  - Сортировка: скор → позиция в рендже (ближе к дну = выше)
  - Сделки/мин из свечей (не пусто)
  - Блок сайзов стакана у нижней границы
  - Верхняя граница стакана не учитывается (BFTUSDT-логика)
  - Рендж до 20%, зона нижней части 30% от ширины по умолчанию
"""

import time
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ═══════════════════════════════════════════════
# Таймфреймы
# ═══════════════════════════════════════════════

TIMEFRAME_TO_API = {
    "1m": "1m", "3m": "3m", "5m": "5m",
    "15m": "15m", "30m": "30m", "1h": "60m", "4h": "4h",
}
TIMEFRAME_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240,
}


# ═══════════════════════════════════════════════
# Структуры данных
# ═══════════════════════════════════════════════

@dataclass
class BounceEvent:
    candle_idx: int
    low_price: float
    high_after: float
    bounce_pct: float   # % подъёма от ширины канала
    time_ms: int = 0


@dataclass
class BookLevel:
    """Уровень стакана у нижней границы"""
    price: float
    usdt: float
    dist_pct: float   # расстояние от текущей цены в %


@dataclass
class RangeResult:
    symbol: str
    current_price: float
    range_low: float
    range_high: float
    range_pct: float
    position_in_range: float   # 0% = у дна, 100% = у верха
    range_age_min: float
    bounces: int
    bounce_events: list = field(default_factory=list)

    # Бонусные теги
    has_big_size: bool = False
    has_aged: bool = False
    has_rejected: bool = False

    # Метрики
    avg_candle_vol_usdt: float = 0.0
    big_size_usdt: float = 0.0
    big_size_ratio: float = 0.0
    trades_per_min: float = 0.0
    hourly_vol_usdt: float = 0.0
    score: int = 0

    # Стакан у нижней границы
    book_bid_levels: list = field(default_factory=list)
    book_bid_total_usdt: float = 0.0
    book_top_bid_usdt: float = 0.0
    book_top_bid_price: float = 0.0

    @property
    def tags(self) -> list[str]:
        t = []
        if self.has_big_size:
            t.append(f"[Big\u00d7{self.big_size_ratio:.0f}]")
        if self.has_aged:
            age_h = self.range_age_min / 60
            t.append(f"[Aged {age_h:.1f}h]" if age_h >= 1 else f"[Aged {self.range_age_min:.0f}m]")
        if self.has_rejected:
            t.append(f"[Rej\u00d7{self.bounces}]")
        return t

    @property
    def tags_str(self) -> str:
        return " ".join(self.tags) if self.tags else "\u2014"

    @property
    def mexc_url(self) -> str:
        return f"https://www.mexc.com/exchange/{self.symbol.replace('USDT', '_USDT')}"


# ═══════════════════════════════════════════════
# Основной класс
# ═══════════════════════════════════════════════

class RangeBounceScanner:

    def __init__(self, client, trades_buffer=None):
        self.client = client
        self.trades_buffer = trades_buffer
        self._cache: dict = {}
        self._cache_ttl = 60

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
                    "open":      float(k[1]),
                    "high":      float(k[2]),
                    "low":       float(k[3]),
                    "close":     float(k[4]),
                    "volume":    float(k[5]),
                    "quote_vol": float(k[7]) if len(k) > 7 else float(k[5]) * float(k[4]),
                    "trades":    int(k[8])    if len(k) > 8 else 0,
                })
            except (ValueError, TypeError, IndexError):
                continue
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["time"] = pd.to_datetime(df["open_time"], unit="ms")
        return df

    def _parse_book(self, book_raw) -> tuple:
        bids, asks = [], []
        if not book_raw:
            return bids, asks
        for item in book_raw.get("bids", []):
            try:
                pf, qf = float(item[0]), float(item[1])
                if pf > 0 and qf > 0:
                    bids.append((pf, pf * qf))
            except (ValueError, TypeError, IndexError):
                continue
        for item in book_raw.get("asks", []):
            try:
                pf, qf = float(item[0]), float(item[1])
                if pf > 0 and qf > 0:
                    asks.append((pf, pf * qf))
            except (ValueError, TypeError, IndexError):
                continue
        return bids, asks

    def analyze_symbol(self, symbol: str, cfg: dict) -> Optional[RangeResult]:
        cached = self._cache.get(symbol)
        if cached and time.time() - cached[0] < self._cache_ttl:
            return cached[1]

        tf_api    = TIMEFRAME_TO_API.get(cfg["timeframe"], "5m")
        tf_min    = TIMEFRAME_MINUTES.get(cfg["timeframe"], 5)
        n_candles = cfg["candles_lookback"]

        raw = self.client.get_klines(symbol, tf_api, n_candles)
        df  = self._parse_klines(raw)
        if df.empty or len(df) < 10:
            return None

        # 1. Рендж
        range_low  = float(df["low"].min())
        range_high = float(df["high"].max())
        if range_low <= 0:
            return None
        range_pct   = (range_high - range_low) / range_low * 100
        range_width = range_high - range_low

        if not (cfg["min_range_pct"] <= range_pct <= cfg["max_range_pct"]):
            return None

        current_price = float(df["close"].iloc[-1])
        pos_in_range  = (current_price - range_low) / range_width * 100 if range_width > 0 else 50

        # 2. Возраст
        age_candles = _find_range_age(df, range_low, range_high)
        age_min     = age_candles * tf_min

        # 3. Объём/час
        candles_1h = max(1, 60 // tf_min)
        hourly_vol = float(df.tail(candles_1h)["quote_vol"].sum())

        if not (cfg["min_hourly_vol"] <= hourly_vol <= cfg["max_hourly_vol"]):
            return None

        # 4. Сделки/мин из свечей
        candles_5m     = max(1, 5 // tf_min)
        trades_5m      = int(df.tail(candles_5m)["trades"].sum())
        elapsed_5m     = candles_5m * tf_min
        trades_per_min = trades_5m / elapsed_5m if elapsed_5m > 0 else 0
        # Фолбэк если биржа не вернула trades — оцениваем по объёму
        if trades_per_min == 0 and hourly_vol > 0:
            trades_per_min = max(0.1, round((hourly_vol / 60) / 15, 1))

        # 5. Big Size у нижней границы
        tolerance   = range_width * cfg["touch_tolerance_pct"] / 100
        lower_touch = range_low + tolerance

        avg_period_c = max(1, cfg["vol_avg_period_min"] // tf_min)
        vols_recent  = [v for v in df.tail(avg_period_c)["quote_vol"].tolist() if v > 0]
        avg_vol      = statistics.mean(vols_recent) if vols_recent else 0

        near_low_df  = df[df["low"] <= lower_touch]
        big_vol      = float(near_low_df["quote_vol"].max()) if not near_low_df.empty else 0
        big_ratio    = big_vol / avg_vol if avg_vol > 0 else 0
        has_big_size = big_ratio >= cfg["vol_multiplier"]

        # 6. Отскоки
        bounce_confirm = range_width * cfg["bounce_confirm_pct"] / 100
        bounce_events, bounces = _count_bounces(df, lower_touch, bounce_confirm)

        has_aged     = age_min >= cfg["min_range_age_min"]
        has_rejected = cfg["min_bounces"] <= bounces <= cfg["max_bounces"]
        score        = int(has_big_size) + int(has_aged) + int(has_rejected)

        # 7. Стакан — BID у нижней зоны
        lower_zone_top      = range_low + range_width * cfg["book_lower_zone_pct"] / 100
        book_bid_levels     = []
        book_bid_total      = 0.0
        book_top_bid_usdt   = 0.0
        book_top_bid_price  = 0.0

        try:
            book_raw = self.client.get_order_book(symbol, 100)
            if book_raw:
                bids, _ = self._parse_book(book_raw)
                for price, usdt in bids:
                    if price <= lower_zone_top and usdt >= cfg["book_min_level_usdt"]:
                        dist = (current_price - price) / current_price * 100 if current_price > 0 else 0
                        book_bid_levels.append(BookLevel(
                            price=price, usdt=usdt, dist_pct=round(dist, 2)
                        ))
                        book_bid_total += usdt
                        if usdt > book_top_bid_usdt:
                            book_top_bid_usdt  = usdt
                            book_top_bid_price = price
                book_bid_levels.sort(key=lambda x: -x.usdt)
        except Exception:
            pass

        result = RangeResult(
            symbol=symbol,
            current_price=current_price,
            range_low=range_low,
            range_high=range_high,
            range_pct=round(range_pct, 2),
            position_in_range=round(pos_in_range, 1),
            range_age_min=round(age_min, 0),
            bounces=bounces,
            bounce_events=bounce_events,
            has_big_size=has_big_size,
            has_aged=has_aged,
            has_rejected=has_rejected,
            avg_candle_vol_usdt=round(avg_vol, 1),
            big_size_usdt=round(big_vol, 1),
            big_size_ratio=round(big_ratio, 1),
            trades_per_min=round(trades_per_min, 2),
            hourly_vol_usdt=round(hourly_vol, 0),
            score=score,
            book_bid_levels=book_bid_levels[:20],
            book_bid_total_usdt=round(book_bid_total, 0),
            book_top_bid_usdt=round(book_top_bid_usdt, 0),
            book_top_bid_price=book_top_bid_price,
        )

        self._cache[symbol] = (time.time(), result)
        return result

    def scan(self, symbols: list, cfg: dict, progress_cb=None) -> list:
        results = []
        total   = len(symbols)
        for i, sym in enumerate(symbols):
            try:
                r = self.analyze_symbol(sym, cfg)
                if r is not None:
                    results.append(r)
            except Exception:
                pass
            if progress_cb:
                progress_cb(i + 1, total)
        # Сортировка: скор desc, позиция в рендже asc (ближе к дну = выше)
        results.sort(key=lambda r: (-r.score, r.position_in_range))
        return results[:cfg.get("top_results", 50)]

    def get_chart(self, symbol: str, cfg: dict, result: RangeResult):
        tf_api = TIMEFRAME_TO_API.get(cfg["timeframe"], "5m")
        raw    = self.client.get_klines(symbol, tf_api, cfg["candles_lookback"])
        df     = self._parse_klines(raw)
        if df.empty:
            return None
        return _build_range_chart(df, result, cfg)


# ═══════════════════════════════════════════════
# Вспомогательные функции
# ═══════════════════════════════════════════════

def _find_range_age(df: pd.DataFrame, range_low: float, range_high: float) -> int:
    n = len(df)
    for i in range(n - 1, -1, -1):
        row = df.iloc[i]
        if float(row["low"]) < range_low * 0.995 or float(row["high"]) > range_high * 1.005:
            return n - i - 1
    return n


def _count_bounces(df: pd.DataFrame, lower_touch: float, bounce_confirm: float) -> tuple:
    events = []
    n = len(df)
    i = 0
    while i < n:
        row = df.iloc[i]
        if float(row["low"]) <= lower_touch:
            confirm_price = float(row["low"]) + bounce_confirm
            best_high     = float(row["low"])
            bounced       = False
            for j in range(i + 1, min(i + 11, n)):
                best_high = max(best_high, float(df.iloc[j]["high"]))
                if best_high >= confirm_price:
                    bounced = True
                    break
            if bounced:
                ts = int(row["open_time"]) if "open_time" in row.index else 0
                bc = (best_high - float(row["low"])) / max(bounce_confirm, 1e-10) * 100
                events.append(BounceEvent(
                    candle_idx=i,
                    low_price=float(row["low"]),
                    high_after=round(best_high, 8),
                    bounce_pct=round(bc, 1),
                    time_ms=ts,
                ))
                i += 3
                continue
        i += 1
    return events, len(events)


def _build_range_chart(df: pd.DataFrame, result: RangeResult, cfg: dict):
    tolerance      = (result.range_high - result.range_low) * cfg["touch_tolerance_pct"] / 100
    lower_zone_top = result.range_low + (result.range_high - result.range_low) * cfg["book_lower_zone_pct"] / 100

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.75, 0.25], vertical_spacing=0.03,
    )

    fig.add_trace(go.Candlestick(
        x=df["time"],
        open=df["open"], high=df["high"], low=df["low"], close=df["close"],
        increasing_line_color="#00d1a0", decreasing_line_color="#ff4466",
        name="Цена",
    ), row=1, col=1)

    # Только нижняя граница
    fig.add_hline(
        y=result.range_low, line_color="#00bfff", line_dash="dash", line_width=2,
        annotation_text=f"Поддержка {result.range_low:.6f}",
        annotation_position="left", row=1, col=1,
    )
    # Зона допуска
    fig.add_hrect(
        y0=result.range_low, y1=result.range_low + tolerance,
        fillcolor="rgba(0,191,255,0.08)", line_width=0, row=1, col=1,
    )
    # Нижняя накопительная зона
    fig.add_hrect(
        y0=result.range_low, y1=lower_zone_top,
        fillcolor="rgba(0,255,150,0.04)", line_width=0, row=1, col=1,
    )

    # Уровни стакана у дна (толщина линии пропорциональна объёму)
    for lvl in result.book_bid_levels[:10]:
        lw = max(1.0, min(4.0, lvl.usdt / max(result.book_top_bid_usdt, 1) * 4))
        fig.add_hline(
            y=lvl.price, line_color="rgba(0,200,255,0.55)",
            line_dash="dot", line_width=lw, row=1, col=1,
        )

    # Отскоки
    for ev in result.bounce_events:
        if ev.candle_idx < len(df):
            t = df["time"].iloc[ev.candle_idx]
            fig.add_trace(go.Scatter(
                x=[t], y=[ev.low_price * 0.997],
                mode="markers",
                marker=dict(symbol="triangle-up", size=13, color="#ffdd00"),
                showlegend=False,
            ), row=1, col=1)

    vol_colors = ["#00d1a0" if c >= o else "#ff4466"
                  for c, o in zip(df["close"], df["open"])]
    fig.add_trace(go.Bar(
        x=df["time"], y=df["quote_vol"],
        marker_color=vol_colors, opacity=0.7, name="Объём",
    ), row=2, col=1)

    age_str  = f"{result.range_age_min/60:.1f}ч" if result.range_age_min >= 60 else f"{result.range_age_min:.0f}м"
    tags_str = " ".join(result.tags) if result.tags else "нет бонусов"

    fig.update_layout(
        title=dict(
            text=f"{result.symbol}  ·  Рендж {result.range_pct:.1f}%  ·  Позиция {result.position_in_range:.0f}%  ·  {age_str}  ·  {tags_str}",
            font=dict(color="#e0e0e0", size=13),
        ),
        xaxis_rangeslider_visible=False,
        paper_bgcolor="#0f1117", plot_bgcolor="#0f1117",
        font=dict(color="#aaaaaa", size=11),
        height=520, margin=dict(l=50, r=20, t=50, b=20),
        showlegend=False,
    )
    fig.update_xaxes(gridcolor="#1e2230")
    fig.update_yaxes(gridcolor="#1e2230")
    return fig


# ═══════════════════════════════════════════════
# Streamlit UI
# ═══════════════════════════════════════════════

def render_range_bounce_tab(client, trades_buffer=None):
    st.markdown("### 📊 Range Bounce Scanner v2.0")
    st.caption(
        "Монеты в ценовом коридоре с накоплением у нижней границы. "
        "**[Big\u00d7N]** — крупный объём у дна \u00b7 "
        "**[Aged]** — рендж живёт давно \u00b7 "
        "**[Rej\u00d7N]** — отскоки от поддержки."
    )

    with st.expander("\u2699\ufe0f Параметры сканирования", expanded=False):
        c1, c2, c3 = st.columns(3)

        with c1:
            st.markdown("**Рендж**")
            tf        = st.selectbox("Таймфрейм", ["1m","3m","5m","15m","30m","1h","4h"],
                                     index=2, key="rbs_tf")
            n_candles = st.slider("Свечей для анализа", 20, 200, 60, key="rbs_candles")
            min_range = st.slider("Мин. ширина ренджа, %", 1.0, 10.0, 2.0, 0.5, key="rbs_min_r")
            max_range = st.slider("Макс. ширина ренджа, %", 5.0, 50.0, 20.0, 1.0, key="rbs_max_r")
            tolerance = st.slider("Допуск нечёткости (% от ширины)", 1.0, 20.0, 8.0, 1.0, key="rbs_tol")

        with c2:
            st.markdown("**Объём / час**")
            min_hvol  = st.number_input("Мин. объём/час ($)", 0, 500_000, 200, 100, key="rbs_min_hvol")
            max_hvol  = st.number_input("Макс. объём/час ($)", 100, 2_000_000, 50_000, 1000, key="rbs_max_hvol")
            st.markdown("**Big Size (свечи)**")
            vol_mult   = st.slider("Множитель объёма \u00d7", 2.0, 30.0, 8.0, 1.0, key="rbs_vmult")
            vol_period = st.slider("Период среднего (мин)", 5, 60, 15, 5, key="rbs_vperiod")

        with c3:
            st.markdown("**Бонусы**")
            min_age     = st.slider("Мин. возраст ренджа (мин)", 10, 240, 60, 10, key="rbs_age")
            min_bounces = st.slider("Мин. отскоков", 1, 5, 2, key="rbs_minb")
            max_bounces = st.slider("Макс. отскоков", 5, 20, 9, key="rbs_maxb")
            bounce_conf = st.slider("Мин. отскок (% от ширины)", 5.0, 50.0, 10.0, 5.0, key="rbs_bc")
            st.markdown("**Стакан — нижняя зона**")
            lower_zone  = st.slider("Размер нижней зоны (% от ширины)", 10, 50, 30, 5, key="rbs_lz")
            min_bid_lvl = st.slider("Мин. BID уровень ($)", 5, 500, 20, 5, key="rbs_minbid")
            top_n       = st.slider("Топ-N результатов", 10, 200, 50, 10, key="rbs_topn")

    cfg = {
        "timeframe":           tf,
        "candles_lookback":    n_candles,
        "min_range_pct":       min_range,
        "max_range_pct":       max_range,
        "touch_tolerance_pct": tolerance,
        "min_hourly_vol":      float(min_hvol),
        "max_hourly_vol":      float(max_hvol),
        "vol_multiplier":      vol_mult,
        "vol_avg_period_min":  vol_period,
        "min_range_age_min":   min_age,
        "min_bounces":         min_bounces,
        "max_bounces":         max_bounces,
        "bounce_confirm_pct":  bounce_conf,
        "book_lower_zone_pct": lower_zone,
        "book_min_level_usdt": float(min_bid_lvl),
        "top_results":         top_n,
    }

    # Запуск
    run_col, info_col = st.columns([1, 3])
    with run_col:
        run_scan = st.button("\U0001f50d Запустить скан", type="primary",
                             use_container_width=True, key="rbs_run")
    with info_col:
        last_scan = st.session_state.get("rbs_last_scan_time")
        if last_scan:
            elapsed = int(time.time() - last_scan)
            found   = len(st.session_state.get("rbs_results", []))
            st.caption(f"Последний скан: {elapsed} сек. назад \u00b7 Найдено: {found} монет")

    if run_scan:
        symbols = _get_symbols_for_scan(client)
        if not symbols:
            st.error("Не удалось получить список монет.")
        else:
            scanner      = RangeBounceScanner(client, trades_buffer)
            progress_bar = st.progress(0, text="Сканирование...")

            def _progress(done, total):
                progress_bar.progress(done / total,
                                      text=f"Сканирование {done}/{total}...")

            with st.spinner(f"Анализирую {len(symbols)} монет..."):
                results = scanner.scan(symbols, cfg, progress_cb=_progress)

            progress_bar.empty()
            st.session_state["rbs_results"]        = results
            st.session_state["rbs_last_scan_time"] = time.time()
            st.session_state["rbs_scanner"]        = scanner
            st.session_state["rbs_cfg"]            = cfg
            st.rerun()

    # Результаты
    results = st.session_state.get("rbs_results", [])
    if not results:
        if not st.session_state.get("rbs_last_scan_time"):
            st.info("\U0001f446 Нажми «Запустить скан» для поиска монет в рендже.")
        else:
            st.warning("\U0001f614 Монеты не найдены. Расширь диапазон объёма или ренджа.")
        return

    # Метрики верхней строки
    score3 = sum(1 for r in results if r.score == 3)
    score2 = sum(1 for r in results if r.score == 2)
    score1 = sum(1 for r in results if r.score == 1)
    mc = st.columns(5)
    mc[0].metric("Всего",      len(results))
    mc[1].metric("\U0001f3af 3 бонуса", score3)
    mc[2].metric("\u2b50 2 бонуса",    score2)
    mc[3].metric("\u2705 1 бонус",     score1)
    mc[4].metric("\U0001f4ca 0 бонусов", len(results) - score3 - score2 - score1)

    top3 = [r for r in results if r.score == 3]
    if top3:
        st.success("\U0001f514 3 бонуса: **" + ", ".join(r.symbol for r in top3) + "**")

    # Фильтры
    fc1, fc2 = st.columns([2, 3])
    with fc1:
        min_score_filter = st.selectbox("Мин. бонусов", [0,1,2,3], index=0, key="rbs_fscore")
    with fc2:
        tag_filter = st.multiselect("Тег", ["Big Size","Aged","Rejected"],
                                    default=[], key="rbs_ftag")

    filtered = [r for r in results if r.score >= min_score_filter]
    if "Big Size" in tag_filter: filtered = [r for r in filtered if r.has_big_size]
    if "Aged"     in tag_filter: filtered = [r for r in filtered if r.has_aged]
    if "Rejected" in tag_filter: filtered = [r for r in filtered if r.has_rejected]

    if not filtered:
        st.info("Нет монет с выбранными фильтрами.")
        return

    # Форматирование
    def _fp(p):
        if p >= 1000: return f"{p:,.0f}"
        if p >= 1:    return f"{p:.4f}"
        if p >= 0.01: return f"{p:.6f}"
        return f"{p:.8f}"

    def _fu(v):
        if v <= 0:         return "\u2014"
        if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
        if v >= 1000:      return f"${v/1000:.1f}K"
        return f"${v:,.0f}"

    # Таблица — сортировка уже сделана в scan()
    rows = []
    for r in filtered:
        age_str = (f"{r.range_age_min/60:.1f}ч"
                   if r.range_age_min >= 60 else f"{r.range_age_min:.0f}м")
        tpm_str = f"{r.trades_per_min:.1f}" if r.trades_per_min > 0 else "\u2014"
        rows.append({
            "Скор":        r.score,
            "Пара":        r.symbol,
            "Цена":        _fp(r.current_price),
            "Рендж%":      f"{r.range_pct:.1f}%",
            "Позиция":     f"{r.position_in_range:.0f}%",
            "Отскоки":     r.bounces,
            "Возраст":     age_str,
            "Сд/мин":      tpm_str,
            "Vol/ч":       _fu(r.hourly_vol_usdt),
            "BID у дна":   _fu(r.book_bid_total_usdt),
            "Топ BID":     _fu(r.book_top_bid_usdt),
            "Теги":        r.tags_str,
        })

    df_table = pd.DataFrame(rows)

    def _row_style(row):
        s = row["Скор"]
        if s == 3: return ["background-color: rgba(255,200,0,0.15)"] * len(row)
        if s == 2: return ["background-color: rgba(0,200,150,0.10)"] * len(row)
        return [""] * len(row)

    st.dataframe(
        df_table.style.apply(_row_style, axis=1),
        hide_index=True, use_container_width=True,
        height=min(len(filtered) * 35 + 45, 600),
    )

    st.download_button(
        "\U0001f4e5 CSV",
        data=df_table.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"range_{datetime.now().strftime('%H%M')}.csv",
        mime="text/csv", key="rbs_csv",
    )

    # Детальный просмотр
    st.markdown("---")
    st.markdown("#### \U0001f50d Детальный анализ")

    sc1, sc2 = st.columns([3, 1])
    with sc1:
        selected_sym = st.selectbox("Монета", [r.symbol for r in filtered],
                                    key="rbs_detail_sym")
    with sc2:
        show_chart = st.button("\U0001f4c8 График", key="rbs_show_chart",
                               use_container_width=True)

    sel = next((r for r in filtered if r.symbol == selected_sym), None)
    if not sel:
        return

    # Метрики карточки
    d1, d2, d3, d4, d5, d6 = st.columns(6)
    d1.metric("Цена",    _fp(sel.current_price))
    d2.metric("Рендж",   f"{sel.range_pct:.1f}%")
    d3.metric("Позиция", f"{sel.position_in_range:.0f}%",
              help="0% = у дна, 100% = у верха")
    age_label = f"{sel.range_age_min/60:.1f}ч" if sel.range_age_min >= 60 else f"{sel.range_age_min:.0f}м"
    d4.metric("Возраст", age_label)
    d5.metric("Сд/мин",  f"{sel.trades_per_min:.1f}" if sel.trades_per_min > 0 else "\u2014")
    d6.metric("Vol/час", _fu(sel.hourly_vol_usdt))

    # Теги
    if sel.tags:
        tags_html = " ".join(
            f'<span style="background:#1e3a5f;color:#4db8ff;'
            f'padding:3px 10px;border-radius:4px;font-size:13px;">{t}</span>'
            for t in sel.tags
        )
        st.markdown(tags_html + "<br>", unsafe_allow_html=True)

    # Уровень входа
    entry  = sel.range_low * 1.005
    target = sel.range_low + (sel.range_high - sel.range_low) * 0.5
    stop   = sel.range_low * 0.99
    upside = (target / sel.current_price - 1) * 100 if sel.current_price > 0 else 0
    st.info(
        f"\U0001f4cd **Вход:** \u2264 {_fp(entry)}  |  "
        f"\U0001f3af **Цель:** {_fp(target)} (+{upside:.1f}%)  |  "
        f"\U0001f6d1 **Стоп:** {_fp(stop)}"
    )

    # ── Блок сайзов стакана у нижней границы ──
    lower_zone_top = sel.range_low + (sel.range_high - sel.range_low) * cfg["book_lower_zone_pct"] / 100
    st.markdown(
        f"**\U0001f4e6 BID-стакан у нижней зоны**  "
        f"(цены \u2264 {_fp(lower_zone_top)}, итого **{_fu(sel.book_bid_total_usdt)}**)"
    )

    if sel.book_bid_levels:
        bid_rows = []
        top_usdt = sel.book_top_bid_usdt or 1
        for lvl in sel.book_bid_levels[:15]:
            bar_w = int(min(lvl.usdt / top_usdt * 20, 20))
            bar   = "\u2588" * bar_w + "\u2591" * (20 - bar_w)
            bid_rows.append({
                "Цена":       _fp(lvl.price),
                "Объём $":    _fu(lvl.usdt),
                "% от цены":  f"{lvl.dist_pct:.1f}%",
                "Визуализация": bar,
            })
        st.dataframe(
            pd.DataFrame(bid_rows),
            hide_index=True, use_container_width=True,
            height=min(len(bid_rows) * 35 + 40, 420),
        )
    else:
        st.caption("Данные стакана у нижней зоны не получены.")

    # История отскоков
    if sel.bounce_events:
        st.markdown("**История отскоков от поддержки:**")
        brows = []
        for i, ev in enumerate(sel.bounce_events, 1):
            ts = datetime.fromtimestamp(ev.time_ms / 1000).strftime("%H:%M") if ev.time_ms else "\u2014"
            brows.append({
                "#": i, "Время": ts,
                "Low": _fp(ev.low_price),
                "Макс после": _fp(ev.high_after),
                "Рост (% ширины)": f"{ev.bounce_pct:.0f}%",
            })
        st.dataframe(pd.DataFrame(brows), hide_index=True, use_container_width=True)

    st.markdown(f"[\U0001f517 Открыть {selected_sym} на MEXC]({sel.mexc_url})")

    # График
    if show_chart or st.session_state.get("rbs_chart_sym") == selected_sym:
        st.session_state["rbs_chart_sym"] = selected_sym
        scanner_obj = st.session_state.get("rbs_scanner")
        saved_cfg   = st.session_state.get("rbs_cfg", cfg)
        if scanner_obj:
            with st.spinner("Строю график..."):
                fig = scanner_obj.get_chart(selected_sym, saved_cfg, sel)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Не удалось построить график.")


# ═══════════════════════════════════════════════
# Вспомогательная: список символов
# ═══════════════════════════════════════════════

def _get_symbols_for_scan(client) -> list:
    scan_res = st.session_state.get("scan_results", [])
    if scan_res:
        syms = []
        for r in scan_res:
            if hasattr(r, "symbol"):
                syms.append(r.symbol)
            elif isinstance(r, dict):
                s = r.get("symbol") or r.get("\u041f\u0430\u0440\u0430", "")
                if s:
                    syms.append(s)
        syms = [s for s in syms if s.endswith("USDT")]
        if syms:
            return syms

    try:
        tickers = client.get_all_tickers_24h()
        if not tickers:
            return []
        filtered = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            vol = float(t.get("quoteVolume", 0) or 0)
            if 200 <= vol <= 5_000_000:
                filtered.append((sym, vol))
        filtered.sort(key=lambda x: x[1], reverse=True)
        return [s for s, _ in filtered[:300]]
    except Exception:
        return []
