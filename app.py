# -*- coding: utf-8 -*-
"""MEXC Density Scanner v7.1 — Phase 3.1: All Bugs Fixed + Hunter + Dead Book"""
import io, time, zipfile, math
from datetime import datetime
from collections import Counter
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from mexc_client import MexcClientSync
from analyzer import analyze_order_book, LadderPattern
from history import DensityTracker
from trades_buffer import TradesBuffer, Trade, parse_rest_trades, parse_agg_trades
try:
    from algo_detector import (
        analyze_algo, find_size_clusters, analyze_timing, check_honesty,
        analyze_wall_behavior,
        AlgoSignal, SizeCluster, TimingPattern, HonestyCheck, WallBehavior
    )
except ImportError:
    from algo_detector import (
        analyze_algo, find_size_clusters, analyze_timing, check_honesty,
        AlgoSignal, SizeCluster, TimingPattern, HonestyCheck
    )
    WallBehavior = None
    def analyze_wall_behavior(*a, **kw): return None

# Phase 3: Robot Screener
try:
    from robot_screener import render_robot_screener_tab, RobotScreener
    _HAS_SCREENER = True
except ImportError:
    render_robot_screener_tab = None
    _HAS_SCREENER = False

# Phase 3: Book Quality
try:
    from book_quality import check_book_quality
except ImportError:
    check_book_quality = None

# Phase 3: Hunter Detector
try:
    from hunter_detector import detect_hunter_pattern
except ImportError:
    detect_hunter_pattern = None

st.set_page_config(page_title="MEXC Scanner", layout="wide",
                   initial_sidebar_state="expanded")

# ======= helpers =======
def sf(v, d=0.0):
    if v is None or v == "": return d
    try: return float(v)
    except: return d
def si(v, d=0):
    try: return int(sf(v, d))
    except: return d
def parse_book(raw):
    out = []
    if not raw or not isinstance(raw, list): return out
    for e in raw:
        if not isinstance(e, (list, tuple)) or len(e) < 2: continue
        p, q = sf(e[0]), sf(e[1])
        if p > 0 and q > 0: out.append((p, q))
    return out
def extract_tc(td):
    if isinstance(td, list): td = td[0] if td else {}
    if not isinstance(td, dict): return 0
    # MEXC может вернуть count как строку или число в разных полях
    for k in ("count", "tradeCount", "trades", "txcnt", "c", "tradeNum", "numberOfTrades"):
        v = td.get(k)
        if v is not None:
            r = si(v)
            if r > 0: return r
    # Фолбэк: если нет поля count, пробуем вычислить из volume/avgPrice
    vol = sf(td.get("quoteVolume", 0))
    avg_price = sf(td.get("lastPrice", 0))
    if vol > 0 and avg_price > 0:
        # Грубая оценка: объём / средний размер сделки ($10)
        estimated = int(vol / max(avg_price * 100, 10))
        if estimated > 0:
            return estimated
    return 0
def parse_klines(raw):
    if not raw or not isinstance(raw, list): return pd.DataFrame()
    rows = []
    for k in raw:
        if not isinstance(k, (list, tuple)) or len(k) < 6: continue
        rows.append({"open_time": sf(k[0]), "open": sf(k[1]), "high": sf(k[2]),
            "low": sf(k[3]), "close": sf(k[4]), "volume": sf(k[5]),
            "close_time": sf(k[6]) if len(k) > 6 else 0,
            "quote_volume": sf(k[7]) if len(k) > 7 else 0,
            "trades": si(k[8]) if len(k) > 8 else 0})
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df
def fmt_price(price):
    if price <= 0: return "0"
    if price >= 1000: return f"{price:,.0f}"
    if price >= 1: return f"{price:.2f}"
    if price >= 0.01: return f"{price:.4f}"
    if price >= 0.0001: return f"{price:.6f}"
    return f"{price:.8f}"
def fmt_usd(v):
    if v <= 0: return "---"
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:,.0f}"
def mexc_link(s):
    return f"https://www.mexc.com/exchange/{s.replace('USDT', '_USDT')}"
def make_csv(df):
    return df.to_csv(index=False).encode("utf-8-sig")
def kline_stats(df, n=None):
    if df is None or df.empty:
        return {"volume": 0.0, "trades": 0}
    sub = df.tail(n) if n else df
    return {
        "volume": float(sub["quote_volume"].sum()) if "quote_volume" in sub else 0.0,
        "trades": int(sub["trades"].sum()) if "trades" in sub else 0
    }
def analyze_robots(trades_raw):
    if not trades_raw or not isinstance(trades_raw, list) or len(trades_raw) < 5: return None
    times = sorted([sf(t.get("time", t.get("T", 0))) for t in trades_raw if sf(t.get("time", t.get("T", 0))) > 0], reverse=True)
    if len(times) < 5: return None
    deltas = [abs(times[i] - times[i + 1]) / 1000 for i in range(len(times) - 1)]
    deltas = [d for d in deltas if 0 <= d < 600]
    if not deltas: return None
    volumes = [sf(t.get("price", t.get("p", 0))) * sf(t.get("qty", t.get("q", 0))) for t in trades_raw
               if sf(t.get("price", t.get("p", 0))) > 0 and sf(t.get("qty", t.get("q", 0))) > 0]
    avg_d = sum(deltas) / len(deltas)
    mc = Counter([round(d) for d in deltas])
    mode_val, mode_cnt = mc.most_common(1)[0]
    return {"avg": avg_d, "min": min(deltas), "max": max(deltas),
        "mode": mode_val, "mode_count": mode_cnt,
        "mode_pct": round(mode_cnt / len(deltas) * 100, 1),
        "is_robot": avg_d < 30 and max(deltas) < 120,
        "avg_vol": sum(volumes) / len(volumes) if volumes else 0}


def count_trades_5m(client, symbol):
    try:
        kl = client.get_klines(symbol, "5m", 1)
        if kl and isinstance(kl, list) and len(kl) > 0:
            last = kl[-1]
            if isinstance(last, (list, tuple)) and len(last) > 8:
                cnt = si(last[8])
                if cnt > 0: return cnt
        trades = client.get_recent_trades(symbol, 500)
        if trades and isinstance(trades, list) and len(trades) > 0:
            now_ms = time.time() * 1000
            cutoff = now_ms - 5 * 60 * 1000
            cnt = sum(1 for t in trades if sf(t.get("time", 0)) >= cutoff)
            if cnt > 0: return cnt
        return 0
    except: return 0


def count_trades_5m_from_recent(trades_raw):
    if not trades_raw or not isinstance(trades_raw, list): return 0
    now_ms = time.time() * 1000; cutoff = now_ms - 5*60*1000
    cnt = 0
    for t in trades_raw:
        ts = sf(t.get("time", t.get("T", 0)))
        if ts >= cutoff: cnt += 1
    return cnt


# ═══════════════════════════════════════════════════
# CHARTS
# ═══════════════════════════════════════════════════
BID_COLOR = "rgba(0,250,154,0.95)"
ASK_COLOR = "rgba(255,0,0,0.95)"
BID_CANDLE = "#00FA9A"
ASK_CANDLE = "#FF0000"
PRICE_LINE = "#00d2ff"
ALGO_COLOR = "#ffab00"
LADDER_COLOR = "#7c4dff"


def _plotly_price_tickformat(price):
    if price >= 1000: return ",.0f"
    if price >= 1: return ".2f"
    if price >= 0.01: return ".4f"
    if price >= 0.0001: return ".6f"
    if price >= 0.00000001: return ".8f"
    return ".10f"


# ═══════════════════════════════════════════════════
# B-24 fix: Кеширование Plotly-графиков через @st.cache_data
#
# Проблема: каждый rerun Streamlit перестраивал все графики заново (~200-300мс каждый).
# Решение: функции с примитивными аргументами кешируются напрямую (ttl=30с).
# Функции с объектами (DataFrame, Trade, SizeCluster) получают wrapper
# который считает hash входных данных и передаёт его кешированной версии.
# ═══════════════════════════════════════════════════

def _df_hash(df) -> str:
    """Быстрый хеш DataFrame для использования как ключ кеша."""
    if df is None or df.empty:
        return "empty"
    try:
        return f"{len(df)}_{df.iloc[0,0]}_{df.iloc[-1,0]}_{float(df['close'].iloc[-1]):.8f}"
    except Exception:
        return f"{len(df)}"

def _trades_hash(trades) -> str:
    """Хеш списка Trade."""
    if not trades:
        return "empty"
    return f"{len(trades)}_{trades[0].time_ms}_{trades[-1].time_ms}_{sum(t.usdt for t in trades[:5]):.4f}"

def _cluster_hash(cluster) -> str:
    """Хеш SizeCluster."""
    if not cluster:
        return "empty"
    return f"{cluster.count}_{cluster.center_usdt:.4f}_{len(cluster.trades)}"

def _book_hash(bids, asks) -> str:
    """Хеш стакана."""
    b_top = tuple((round(p,8), round(q,8)) for p,q in bids[:5]) if bids else ()
    a_top = tuple((round(p,8), round(q,8)) for p,q in asks[:5]) if asks else ()
    return str(hash((b_top, a_top)))


@st.cache_data(ttl=30, show_spinner=False)
def _cached_candlestick(df_json: str, symbol: str, interval: str, cur_price: float):
    """Кешированная версия — принимает JSON строку вместо DataFrame."""
    try:
        df = pd.read_json(df_json)
        if "time" not in df.columns and "open_time" in df.columns:
            df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    except Exception:
        return None
    return _build_candlestick_impl(df, symbol, interval, cur_price)

def build_candlestick_dual(df, symbol, interval, cur_price=None):
    """B-24 fix: публичная обёртка — сериализует df и делегирует кешированной функции."""
    if df is None or df.empty or len(df) < 2: return None
    try:
        price = float(cur_price) if cur_price and cur_price > 0 else float(df["close"].iloc[-1])
        df_json = df.to_json()
        return _cached_candlestick(df_json, str(symbol), str(interval), round(price, 8))
    except Exception:
        return None

def _build_candlestick_impl(df, symbol, interval, cur_price=None):
    if df is None or df.empty or len(df) < 2: return None
    try:
        ref_price = cur_price if cur_price and cur_price > 0 else float(df["close"].iloc[-1])
        if ref_price <= 0: return None
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03,
            row_heights=[0.75, 0.25], specs=[[{"secondary_y": True}], [{"secondary_y": False}]])
        fig.add_trace(go.Candlestick(x=df["time"], open=df["open"], high=df["high"],
            low=df["low"], close=df["close"], increasing_line_color=BID_CANDLE,
            decreasing_line_color=ASK_CANDLE, increasing_line_width=2, decreasing_line_width=2, name="Price"), row=1, col=1, secondary_y=False)
        pct_vals = [(float(c) - ref_price) / ref_price * 100 for c in df["close"]]
        fig.add_trace(go.Scatter(x=df["time"], y=pct_vals, mode="lines", line=dict(width=0),
            showlegend=False, hoverinfo="skip"), row=1, col=1, secondary_y=True)
        colors = [BID_CANDLE if c >= o else ASK_CANDLE for c, o in zip(df["close"], df["open"])]
        vol_data = df["quote_volume"] if "quote_volume" in df.columns and df["quote_volume"].sum() > 0 else df["volume"]
        fig.add_trace(go.Bar(x=df["time"], y=vol_data, marker_color=colors, opacity=0.5),
            row=2, col=1)
        if cur_price and cur_price > 0:
            fig.add_hline(y=float(cur_price), line_dash="dot", line_color=PRICE_LINE,
                line_width=1.5, annotation_text=f"  {fmt_price(cur_price)}",
                annotation_font_color=PRICE_LINE, annotation_font_size=11, row=1, col=1)
        fig.add_hline(y=0, line_dash="solid", line_color="rgba(255,255,0,0.5)",
            line_width=1.5, row=1, col=1, secondary_y=True)
        pmin, pmax = float(df["low"].min()), float(df["high"].max())
        pct_abs = max(abs((pmin - ref_price) / ref_price * 100),
                      abs((pmax - ref_price) / ref_price * 100), 0.5)
        tfmt = _plotly_price_tickformat(ref_price)
        fig.update_layout(title=f"{symbol}  •  {interval}", template="plotly_dark", height=480,
            xaxis_rangeslider_visible=False, showlegend=False, margin=dict(l=70, r=70, t=45, b=20))
        fig.update_yaxes(title_text="Цена", side="left", tickformat=tfmt, exponentformat="none",
            row=1, col=1, secondary_y=False)
        fig.update_yaxes(title_text="Откл. %", side="right", range=[-pct_abs*1.15, pct_abs*1.15],
            ticksuffix="%", zeroline=True, zerolinecolor="rgba(255,255,0,0.5)", zerolinewidth=2,
            row=1, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Объём $", row=2, col=1)
        return fig
    except: return None


@st.cache_data(ttl=20, show_spinner=False)
def _cached_orderbook_chart(bids_t: tuple, asks_t: tuple, cur_price: float, depth: int):
    bids = list(bids_t); asks = list(asks_t)
    return _build_orderbook_impl(bids, asks, cur_price, depth)

def build_orderbook_chart(bids, asks, cur_price, depth=50):
    """B-24 fix: конвертируем в tuple для хеширования."""
    if not bids and not asks: return None
    return _cached_orderbook_chart(
        tuple(bids[:depth]), tuple(asks[:depth]),
        round(float(cur_price), 8) if cur_price else 0.0, depth)

def _build_orderbook_impl(bids, asks, cur_price, depth=50):
    try:
        b, a = bids[:depth], asks[:depth]
        if not b and not a: return None
        ref = cur_price if cur_price and cur_price > 0 else (b[0][0] if b else a[0][0])
        if ref <= 0: return None
        # Фильтр: только уровни в пределах ±30% от mid (дальние ломают масштаб)
        max_dist = 0.30
        b_f = [(p,q) for p,q in b if abs(float(p) - ref)/ref <= max_dist]
        a_f = [(p,q) for p,q in a if abs(float(p) - ref)/ref <= max_dist]
        if not b_f and not a_f:
            b_f, a_f = b[:15], a[:15]
        tfmt = _plotly_price_tickformat(ref)
        fig = go.Figure()
        if b_f:
            fig.add_trace(go.Bar(y=[float(p) for p,q in b_f],
                x=[float(p*q) for p,q in b_f],
                orientation="h", name="BID", marker_color=BID_COLOR))
        if a_f:
            fig.add_trace(go.Bar(y=[float(p) for p,q in a_f],
                x=[float(p*q) for p,q in a_f],
                orientation="h", name="ASK", marker_color=ASK_COLOR))
        if cur_price and float(cur_price) > 0:
            fig.add_hline(y=float(cur_price), line_dash="dot", line_color=PRICE_LINE, line_width=3,
                annotation_text=f"  {fmt_price(float(cur_price))}", annotation_font_color=PRICE_LINE)
        all_p = [float(p) for p,q in b_f] + [float(p) for p,q in a_f]
        if all_p:
            y_lo = min(all_p) * 0.998; y_hi = max(all_p) * 1.002
        else:
            y_lo, y_hi = ref * 0.7, ref * 1.3
        fig.update_layout(xaxis_title="$ USDT", template="plotly_dark",
            height=max(500, len(all_p)*14), barmode="relative", showlegend=True,
            margin=dict(l=90, r=20, t=40, b=30))
        fig.update_yaxes(tickformat=tfmt, exponentformat="none", range=[y_lo, y_hi])
        return fig
    except: return None


@st.cache_data(ttl=20, show_spinner=False)
def _cached_heatmap(bids_t: tuple, asks_t: tuple, cur_price: float, depth: int):
    return _build_heatmap_impl(list(bids_t), list(asks_t), cur_price, depth)

def build_heatmap(bids, asks, cur_price, depth=30):
    """B-24 fix: конвертируем в tuple для хеширования."""
    if not bids and not asks: return None
    return _cached_heatmap(
        tuple(bids[:depth]), tuple(asks[:depth]),
        round(float(cur_price), 8) if cur_price else 0.0, depth)

def _build_heatmap_impl(bids, asks, cur_price, depth=30):
    try:
        ref = cur_price if cur_price and cur_price > 0 else (bids[0][0] if bids else asks[0][0])
        if ref <= 0: return None
        max_dist = 0.30
        levels = []
        for p,q in bids[:depth]:
            if abs(float(p) - ref)/ref <= max_dist:
                levels.append(("BID", float(p), float(p*q)))
        for p,q in asks[:depth]:
            if abs(float(p) - ref)/ref <= max_dist:
                levels.append(("ASK", float(p), float(p*q)))
        if not levels: return None
        levels.sort(key=lambda x: x[1])
        mx = max(v for _,_,v in levels)
        if mx <= 0: mx = 1.0
        tfmt = _plotly_price_tickformat(ref)
        prices, vols, colors, hovers = [], [], [], []
        for side, price, vol in levels:
            intensity = min(float(vol)/float(mx), 1.0)
            prices.append(price); vols.append(vol)
            if side == "BID":
                g = int(130 + 125*intensity); c = f"rgba(0,{g},60,0.95)"
            else:
                r = int(130 + 125*intensity); c = f"rgba({r},20,40,0.95)"
            colors.append(c); hovers.append(f"{side}: ${vol:,.0f} @ {fmt_price(price)}")
        fig = go.Figure(go.Bar(y=prices, x=vols, orientation="h", marker_color=colors,
            hovertext=hovers, hoverinfo="text", showlegend=False))
        if cur_price and float(cur_price) > 0:
            fig.add_hline(y=float(cur_price), line_dash="dot", line_color=PRICE_LINE, line_width=3,
                annotation_text=f"  {fmt_price(float(cur_price))}", annotation_font_color=PRICE_LINE)
        y_lo = min(prices) * 0.998; y_hi = max(prices) * 1.002
        fig.update_layout(template="plotly_dark", height=500, xaxis_title="$ USDT",
            margin=dict(l=90, r=20, t=40, b=30))
        fig.update_yaxes(tickformat=tfmt, exponentformat="none", range=[y_lo, y_hi])
        return fig
    except: return None



@st.cache_data(ttl=15, show_spinner=False)
def _cached_trades_scatter(trades_hash: str, trades_key: tuple,
                            cluster_center: float, cluster_tol: float):
    """Кеш по хешу сделок. trades_key содержит (time_ms, usdt, is_buy) первых/последних."""
    return None  # заглушка — реальные данные передаются через closure ниже

def build_trades_scatter(trades: list[Trade], signal: AlgoSignal = None):
    """B-24 fix: кеш через session_state с ключом по хешу данных."""
    if not trades or len(trades) < 3: return None
    # Ключ: хеш сделок + параметры сигнала
    dc = signal.dominant_cluster if signal and signal.dominant_cluster else None
    cache_key = (
        _trades_hash(trades),
        round(dc.center_usdt, 4) if dc else 0.0,
    )
    cached = st.session_state.get("_fig_cache_scatter")
    if cached and cached[0] == cache_key:
        return cached[1]
    fig = _build_trades_scatter_impl(trades, signal)
    st.session_state["_fig_cache_scatter"] = (cache_key, fig)
    return fig

def _build_trades_scatter_impl(trades: list[Trade], signal: AlgoSignal = None):
    if not trades or len(trades) < 3: return None
    try:
        buys = [t for t in trades if t.is_buy]
        sells = [t for t in trades if not t.is_buy]
        fig = go.Figure()
        if buys:
            fig.add_trace(go.Scatter(
                x=[pd.to_datetime(t.time_ms, unit="ms") for t in buys],
                y=[t.usdt for t in buys], mode="markers", name="BUY",
                marker=dict(color=BID_CANDLE, size=6, opacity=0.7,
                            line=dict(width=0.5, color="white")),
                hovertemplate="BUY $%{y:.2f}<br>%{x}<extra></extra>"))
        if sells:
            fig.add_trace(go.Scatter(
                x=[pd.to_datetime(t.time_ms, unit="ms") for t in sells],
                y=[t.usdt for t in sells], mode="markers", name="SELL",
                marker=dict(color=ASK_CANDLE, size=6, opacity=0.7,
                            line=dict(width=0.5, color="white")),
                hovertemplate="SELL $%{y:.2f}<br>%{x}<extra></extra>"))
        if signal and signal.dominant_cluster:
            dc = signal.dominant_cluster
            fig.add_hline(y=dc.center_usdt, line_dash="dash", line_color=ALGO_COLOR, line_width=2,
                annotation_text=f"  Кластер ${dc.center_usdt:.1f}", annotation_font_color=ALGO_COLOR)
            import config as _cfg
            tol = _cfg.ALGO_SIZE_TOLERANCE
            fig.add_hrect(y0=dc.center_usdt*(1-tol), y1=dc.center_usdt*(1+tol),
                fillcolor=ALGO_COLOR, opacity=0.08, line_width=0)
        fig.update_layout(title="Принты (сделки) — объём USDT", template="plotly_dark",
            height=380, xaxis_title="Время", yaxis_title="$ USDT", showlegend=True,
            margin=dict(l=60, r=20, t=45, b=30))
        return fig
    except: return None


def build_timing_histogram(cluster: SizeCluster):
    """B-24 fix: кеш через session_state по хешу кластера."""
    if not cluster or len(cluster.trades) < 5: return None
    cache_key = ("hist", _cluster_hash(cluster))
    cached = st.session_state.get("_fig_cache_hist")
    if cached and cached[0] == cache_key:
        return cached[1]
    fig = _build_timing_histogram_impl(cluster)
    st.session_state["_fig_cache_hist"] = (cache_key, fig)
    return fig

def _build_timing_histogram_impl(cluster: SizeCluster):
    if not cluster or len(cluster.trades) < 5: return None
    try:
        sorted_t = sorted(cluster.trades, key=lambda t: t.time_ms)
        deltas = [(sorted_t[i+1].time_ms - sorted_t[i].time_ms) / 1000.0
                  for i in range(len(sorted_t) - 1)
                  if 0 < (sorted_t[i+1].time_ms - sorted_t[i].time_ms) / 1000.0 < 300]
        if len(deltas) < 3: return None
        fig = go.Figure(go.Histogram(x=deltas, nbinsx=min(30, max(10, len(deltas)//3)),
            marker_color=ALGO_COLOR, opacity=0.85))
        fig.update_layout(title="Интервалы между принтами", template="plotly_dark",
            height=300, xaxis_title="сек", yaxis_title="Кол-во", margin=dict(l=50,r=20,t=45,b=30))
        return fig
    except: return None


def build_seconds_heatmap(cluster: SizeCluster):
    """B-24 fix: кеш через session_state по хешу кластера."""
    if not cluster or len(cluster.trades) < 10: return None
    cache_key = ("sechmap", _cluster_hash(cluster))
    cached = st.session_state.get("_fig_cache_sechmap")
    if cached and cached[0] == cache_key:
        return cached[1]
    fig = _build_seconds_heatmap_impl(cluster)
    st.session_state["_fig_cache_sechmap"] = (cache_key, fig)
    return fig

def _build_seconds_heatmap_impl(cluster: SizeCluster):
    if not cluster or len(cluster.trades) < 10: return None
    try:
        counts = [0]*60
        for t in cluster.trades: counts[t.second_of_minute] += 1
        mx = max(counts) if counts else 1
        colors = [ALGO_COLOR if c/mx > 0.5 else "rgba(255,171,0,0.5)" if c/mx > 0.2
                  else "rgba(100,100,100,0.3)" for c in counts]
        fig = go.Figure(go.Bar(x=list(range(60)), y=counts, marker_color=colors))
        fig.update_layout(title="Секунды минуты", template="plotly_dark",
            height=250, xaxis_title="Секунда", yaxis_title="Кол-во",
            margin=dict(l=50,r=20,t=45,b=30))
        return fig
    except: return None


def build_ladder_chart(bids, asks, ladders, mid_price):
    """B-24 fix: кеш через session_state по хешу стакана и лесенок."""
    if not ladders: return None
    try:
        ladder_key = tuple((l.side, round(l.start_price,8), l.step_count) for l in ladders[:5])
        cache_key = ("ladder", _book_hash(bids[:10], asks[:10]), ladder_key, round(float(mid_price),8))
        cached = st.session_state.get("_fig_cache_ladder")
        if cached and cached[0] == cache_key:
            return cached[1]
    except Exception:
        cache_key = None
    fig = _build_ladder_chart_impl(bids, asks, ladders, mid_price)
    if cache_key is not None:
        st.session_state["_fig_cache_ladder"] = (cache_key, fig)
    return fig

def _build_ladder_chart_impl(bids, asks, ladders, mid_price):
    if not ladders: return None
    try:
        fig = go.Figure()
        for p,q in bids[:30]:
            fig.add_trace(go.Bar(y=[float(p)], x=[float(p*q)], orientation="h",
                marker_color="rgba(0,180,80,0.2)", showlegend=False, hoverinfo="skip"))
        for p,q in asks[:30]:
            fig.add_trace(go.Bar(y=[float(p)], x=[float(p*q)], orientation="h",
                marker_color="rgba(200,30,30,0.2)", showlegend=False, hoverinfo="skip"))
        for ladder in ladders:
            color = LADDER_COLOR if ladder.side == "BID" else "#ff6e40"
            levels = bids if ladder.side == "BID" else asks
            for p,q in levels:
                if (ladder.start_price <= p <= ladder.end_price or
                    ladder.end_price <= p <= ladder.start_price):
                    fig.add_trace(go.Bar(y=[float(p)], x=[float(p*q)], orientation="h",
                        marker_color=color, opacity=0.85, showlegend=False,
                        hovertemplate=f"ЛЕСЕНКА {ladder.side}<br>${float(p*q):,.0f}<extra></extra>"))
        if mid_price > 0:
            fig.add_hline(y=float(mid_price), line_dash="dot", line_color=PRICE_LINE, line_width=3,
                annotation_text=f"  mid {fmt_price(mid_price)}", annotation_font_color=PRICE_LINE)
        tfmt = _plotly_price_tickformat(mid_price)
        fig.update_layout(title="Лесенки в стакане", template="plotly_dark", height=400,
            xaxis_title="$ USDT", margin=dict(l=90,r=20,t=45,b=30))
        fig.update_yaxes(tickformat=tfmt, exponentformat="none")
        return fig
    except: return None


# ======= State =======
import config as cfg_module

# ── Phase 4: WebSocket мониторинг ──────────────────────────────
_HAS_WS = False
try:
    import threading
    import asyncio
    from ws_monitor import MexcWsMonitor
    from storage import ScannerDB
    _HAS_WS = True
except ImportError as _ws_err:
    pass



# ══════════════════════════════════════════════════════════════
# Phase 4: WS Manager — background thread + SQLite bridge
# ══════════════════════════════════════════════════════════════

class WsManager:
    """
    Запускает MexcWsMonitor в отдельном потоке.
    Пишет события в ScannerDB (WAL mode → безопасно для многопоточности).
    Streamlit читает из DB при каждом rerun.
    """
    def __init__(self, db=None):
        self._thread = None
        self._monitor = None
        self._loop = None
        self._symbols = []
        self._db = db
        self._running = False
        # B-05 fix: Lock защищает self.stats от race condition между
        # WS-потоком (_on_ws_event) и UI-потоком (get_ws_stats).
        self._stats_lock = threading.Lock()
        self.stats = {
            "messages": 0,
            "movers_detected": 0,
            "algos_detected": 0,
            "start_time": 0.0,
        }
        self._wall_snapshots = {}   # symbol → {price: (side, size, ts)}
        # B-04 fix: счётчик последовательных сбоев WS для backoff/остановки
        self._ws_error_count = 0
        self._last_ws_error = ""

    def start(self, symbols: list):
        if not _HAS_WS:
            return False
        if self._running:
            self.update_symbols(symbols)
            return True
        self._symbols = list(symbols)
        if self._db is None:
            self._db = ScannerDB()
        self._running = True
        self.stats["start_time"] = time.time()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="ws-monitor"
        )
        self._thread.start()
        return True

    def stop(self):
        self._running = False
        if self._monitor and self._loop:
            asyncio.run_coroutine_threadsafe(self._monitor.stop(), self._loop)



    def is_running(self) -> bool:
        return self._running and self._thread is not None and self._thread.is_alive()

    def get_ws_stats(self) -> dict:
        # B-05 fix: чтение stats через lock — UI-поток и WS-поток не конкурируют
        with self._stats_lock:
            if self._monitor:
                return self._monitor.stats.copy()
            return self.stats.copy()

    def get_algo_signals(self) -> dict:
        """Получить алго-сигналы накопленные WS (для обогащения session_state)."""
        if self._monitor:
            return dict(self._monitor.algo_signals)
        return {}

    def get_trades_for_symbol(self, symbol: str) -> list:
        """Получить сырые сделки из WS буфера для символа."""
        if self._monitor:
            return self._monitor.trades_buffer.get_trades(symbol, last_sec=600)
        return []

    # ── Internal ────────────────────────────────────────────────

    def _run_loop(self):
        """Точка входа потока: создаём новый event loop и запускаем monitor."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._async_main())
        except Exception as e:
            # B-04 fix: логируем вместо pass — иначе крашы невидимы
            import logging
            logging.getLogger("ws_manager").error(
                f"WS event loop crashed: {type(e).__name__}: {e}", exc_info=True)
            self._last_ws_error = str(e)
        finally:
            self._running = False

    async def _async_main(self):
        """Главный async цикл — перезапускает monitor при смене символов."""
        import logging
        _log = logging.getLogger("ws_manager")

        while self._running:
            symbols = list(self._symbols)
            if not symbols:
                await asyncio.sleep(5)
                continue
            self._monitor = MexcWsMonitor(on_event_callback=self._on_ws_event)
            try:
                await self._monitor.start(symbols)
                # Успешный запуск — сбрасываем счётчик ошибок
                self._ws_error_count = 0
            except Exception as e:
                # B-04 fix: логируем каждый сбой с типом и сообщением
                self._ws_error_count += 1
                self._last_ws_error = f"{type(e).__name__}: {e}"
                _log.warning(
                    f"WS monitor crash #{self._ws_error_count}: {self._last_ws_error}"
                )
                # После 5 последовательных сбоев — останавливаемся
                # чтобы не спамить переподключениями при системной ошибке
                if self._ws_error_count >= 5:
                    _log.error("WS monitor: 5 consecutive crashes, stopping.")
                    self._running = False
                    break

            if self._running:
                # Экспоненциальный backoff: 15с → 30с → 60с → max 120с
                # Пауза перед перезапуском — не менее 15 секунд
                delay = min(15 * (2 ** max(self._ws_error_count - 1, 0)), 120)
                await asyncio.sleep(delay)

    def update_symbols(self, symbols: list):
        """Обновить список монет — только если список существенно изменился."""
        new_set = set(symbols)
        old_set = set(self._symbols)
        # Меняем только если разница > 3 монет (новый скан существенно другой)
        added = new_set - old_set
        removed = old_set - new_set
        if len(added) + len(removed) <= 3:
            return  # незначительное изменение — не перезапускаем
        self._symbols = list(symbols)
        if self._monitor and self._loop and self._running:
            asyncio.run_coroutine_threadsafe(self._monitor.stop(), self._loop)

    async def _on_ws_event(self, event_type: str, event, result):
        """Callback от MexcWsMonitor → пишем в SQLite."""
        if not self._db:
            return
        try:
            if event_type == "MOVER" and result:
                # Записываем обновлённый стакан → range_points
                if result.bid_walls or result.ask_walls:
                    self._db.save_range_points_batch([result])
                # Трекинг recovery: стенка исчезла / появилась
                self._track_wall_recovery(event, result)
                with self._stats_lock:
                    self.stats["movers_detected"] = self.stats.get("movers_detected", 0) + 1

            elif event_type == "ALGO_FOUND":
                self._db.save_algo_signal(event.symbol, event)
                self._db._commit()  # B-11 fix: thread-safe commit через обёртку
                with self._stats_lock:
                    self.stats["algos_detected"] = self.stats.get("algos_detected", 0) + 1

            elif event_type == "NEW_WALL" and result:
                self._db.save_range_points_batch([result])

            with self._stats_lock:
                self.stats["messages"] = self.stats.get("messages", 0) + 1
        except Exception:
            pass

    def _track_wall_recovery(self, mover_event, result):
        """
        Трекинг исчезновения и восстановления стенок.
        Сравниваем текущий стакан с предыдущим снапшотом.
        """
        if not self._db:
            return
        sym = result.symbol
        now = time.time()
        current_walls = {w.price: (w.side, w.size_usdt) for w in result.all_walls}
        prev = self._wall_snapshots.get(sym, {})

        # Стенки которые были раньше но исчезли сейчас
        for price, (side, size, ts) in list(prev.items()):
            if price not in current_walls:
                # Стенка исчезла — записываем открытое событие recovery
                try:
                    self._db.save_recovery_event(
                        symbol=sym, side=side, wall_price=price,
                        wall_size=size, disappeared_at=ts,
                        recovered_at=0.0, recovery_time=0.0, recovered=False
                    )
                except Exception:
                    pass
                del prev[price]

        # Стенки которых не было раньше — появились (восстановление)
        for price, (side, size) in current_walls.items():
            if price not in prev:
                # Ищем незакрытое событие исчезновения для этой стенки
                try:
                    # B-11 fix: используем публичные thread-safe методы ScannerDB
                    row = self._db.find_open_recovery_event(sym, price)
                    if row:
                        recovery_time = now - row["disappeared_at"]
                        self._db.update_recovery_event(
                            event_id=row["id"],
                            recovered_at=now,
                            recovery_time_sec=recovery_time,
                        )
                except Exception:
                    pass

        # Обновляем снапшот
        self._wall_snapshots[sym] = {
            price: (side, size, now) for price, (side, size) in current_walls.items()
        }


# ─── Инициализация session_state ───────────────────────────────────────────
# B-02 fix: ScannerDB создаётся ВНУТРИ session_state — один раз за сессию,
#           не при каждом rerun. Это устраняет утечку SQLite-соединений.
# B-10 fix: _ws_manager устанавливается ПОСЛЕ session_state, не до.
#           Раньше _shared_db создавался до блока инициализации и передавался
#           в default value WsManager — оба объекта создавались при каждом
#           rerun и тут же выбрасывались (session_state уже содержал старые).

# Шаг 1: объекты без зависимостей
for k, v in [("tracker", DensityTracker()), ("scan_results", []),
             ("last_scan", 0.0), ("total_pairs", 0),
             ("client", MexcClientSync()), ("detail_symbol", ""),
             ("favorites", set()), ("blacklist", set()),
             ("cancel_scan", False), ("current_page", 0),
             ("algo_signals", {}), ("algo_last_scan", 0.0),
             ("trades_buffer", TradesBuffer(
                 max_trades=cfg_module.ALGO_TRADES_BUFFER_SIZE,
                 max_age_sec=cfg_module.ALGO_TRADES_MAX_AGE_SEC)),
             ("ws_active", False),
             ("ws_symbols", []),
             ("scan_running", False)]:
    if k not in st.session_state:
        st.session_state[k] = v

# Шаг 2: ScannerDB — создаётся один раз и живёт в session_state
if "db" not in st.session_state:
    st.session_state.db = ScannerDB() if _HAS_WS else None

# Шаг 3: WsManager получает уже существующий db из session_state
if "ws_manager" not in st.session_state:
    st.session_state.ws_manager = (
        WsManager(st.session_state.db) if _HAS_WS else None
    )

# Алиас — устанавливается ПОСЛЕ инициализации session_state (B-10 fix)
# WsManager хранится в session_state и выживает между reruns Streamlit
_ws_manager = st.session_state.get("ws_manager")

# Прицепляем db к tracker чтобы get_recovery_stats мог читать реальные данные
if _HAS_WS and st.session_state.get("db") is not None:
    st.session_state.tracker._db = st.session_state.db

def go_detail(sym):
    # B-09 fix: st.rerun() перемещён внутрь — внешние вызовы rerun избыточны, но не вредны
    st.session_state.detail_symbol = sym
    st.session_state.current_page = 1
    st.rerun()


# ======= Scan =======
def run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n, min_trades_flt=0):
    import config as cfg
    cfg.MIN_DAILY_VOLUME_USDT = min_vol; cfg.MAX_DAILY_VOLUME_USDT = max_vol
    cfg.MIN_SPREAD_PCT = min_spread; cfg.WALL_MULTIPLIER = wall_mult
    cfg.MIN_WALL_SIZE_USDT = min_wall_usd
    client = st.session_state.client
    st.session_state.cancel_scan = False
    # B-25 fix: флаг блокирует навигацию и сайдбар во время скана.
    # finally гарантирует сброс даже при раннем return или исключении —
    # без этого любой ранний выход оставлял навигацию заблокированной навсегда.
    st.session_state.scan_running = True
    progress = st.progress(0, "Подключение к MEXC...")
    try:
        _run_scan_body(cfg, client, progress,
                       min_vol, max_vol, min_spread,
                       top_n, min_trades_flt)
    finally:
        st.session_state.scan_running = False


def _run_scan_body(cfg, client, progress,
                   min_vol, max_vol, min_spread,
                   top_n, min_trades_flt):
    """Тело скана — вынесено чтобы scan_running сбрасывался через finally."""
    progress.progress(3, "Загрузка пар...")
    try: info = client.get_exchange_info()
    except Exception as e: st.error(f"API: {e}"); progress.empty(); return
    if not info or "symbols" not in info:
        st.error(f"MEXC API недоступен: {client.last_error}")
        progress.empty(); return
    bl = st.session_state.blacklist | set(cfg_module.ALGO_BLACKLIST)
    all_sym = []
    for s in info["symbols"]:
        try:
            if s.get("quoteAsset") != "USDT": continue
            sym = s["symbol"]
            if sym in bl: continue
            st_ = s.get("status", "")
            ok = (str(st_) in ("1","ENABLED","True","true") or st_ is True or st_ == 1)
            if ok and s.get("isSpotTradingAllowed", True): all_sym.append(sym)
        except: continue
    if not all_sym:
        for s in info["symbols"]:
            try:
                sym = s.get("symbol","")
                if s.get("quoteAsset") == "USDT" and sym not in bl: all_sym.append(sym)
            except: continue
    if not all_sym: st.error("0 пар"); progress.empty(); return
    progress.progress(5)
    try: tickers = client.get_all_tickers_24h()
    except: st.error("Ошибка тикеров"); progress.empty(); return
    if not tickers: st.error(str(client.last_error)); progress.empty(); return
    tm = {t["symbol"]: t for t in tickers if "symbol" in t}
    cands = [(sym, tm[sym]) for sym in all_sym
             if sym in tm and min_vol <= sf(tm[sym].get("quoteVolume",0)) <= max_vol]
    cands.sort(key=lambda x: sf(x[1].get("quoteVolume",0)), reverse=True)
    if not cands: st.warning("0 в диапазоне"); progress.empty(); return
    progress.progress(15, f"{len(cands)} кандидатов...")
    results, total = [], len(cands)
    filtered_wt = 0  # счётчик отфильтрованных
    for i, (sym, tk) in enumerate(cands):
        if st.session_state.cancel_scan: st.warning(f"Стоп {i}/{total}"); break
        try:
            # ── Пре-фильтр: wash-trade и памп ──────────────────
            tc_pre = extract_tc(tk)
            if tc_pre < 50:
                filtered_wt += 1
                continue  # wash-trade, мёртвая монета или ONUSDT-семейство
            pcp_pre = sf(tk.get("priceChangePercent", 0))
            if abs(pcp_pre) > 300:
                filtered_wt += 1
                continue  # памп/дамп — непригоден для squeeze
            # ───────────────────────────────────────────────────
            book = client.get_order_book(sym, cfg.ORDER_BOOK_DEPTH)
            if book:
                r = analyze_order_book(sym, book, tk)
                if r and r.spread_pct >= min_spread:
                    r.trade_count_24h = extract_tc(tk)
                    results.append(r)
                    try:
                        # Основной метод: openPrice → lastPrice
                        op = sf(tk.get("openPrice", 0))
                        lp = sf(tk.get("lastPrice", 0))
                        if op > 0 and lp > 0:
                            pcp = round((lp - op) / op * 100, 2)
                        else:
                            # Фолбэк: priceChangePercent из MEXC
                            pcp = sf(tk.get("priceChangePercent", 0))
                        r._price_change_pct = pcp
                    except:
                        r._price_change_pct = 0.0
        except: pass
        if (i+1) % 8 == 0 or i == total-1:
            progress.progress(15 + int((i+1)/total*75), f"{i+1}/{total} → {len(results)} найдено")
    results.sort(key=lambda r: r.score, reverse=True)
    top = results[:top_n]
    progress.progress(90, "Обогащение данных...")
    for idx, r in enumerate(top[:30]):
        try:
            if r.trade_count_24h == 0:
                tc = extract_tc(client.get_ticker_24h(r.symbol))
                if tc > 0: r.trade_count_24h = tc
            r._trades_5m = count_trades_5m(client, r.symbol)
        except:
            if not hasattr(r, '_trades_5m'): r._trades_5m = 0
        if (idx+1) % 5 == 0:
            progress.progress(90+int((idx+1)/min(len(top),30)*8))
    # Фильтр по количеству сделок
    if min_trades_flt > 0:
        top = [r for r in top if r.trade_count_24h >= min_trades_flt]
    st.session_state.tracker.update(top)
    st.session_state.scan_results = top
    if filtered_wt > 0:
        st.session_state['_last_filtered_wt'] = filtered_wt
    else:
        st.session_state.pop('_last_filtered_wt', None)
    st.session_state.last_scan = time.time()

    # Аннулируем algo_signals для монет выпавших из топ
    # (устаревший сигнал хуже чем его отсутствие)
    current_syms = {r.symbol for r in top}
    old_signals = st.session_state.get("algo_signals", {})
    if old_signals:
        fresh_signals = {s: sig for s, sig in old_signals.items() if s in current_syms}
        st.session_state.algo_signals = fresh_signals
    st.session_state.total_pairs = total
    st.session_state.cancel_scan = False

    # ── Phase 4: Запускаем / обновляем WS мониторинг ─────────
    if _HAS_WS and _ws_manager is not None:
        ws_syms = [r.symbol for r in top[:20]]
        if st.session_state.get("ws_active"):
            _ws_manager.update_symbols(ws_syms)
        else:
            ok = _ws_manager.start(ws_syms)
            if ok:
                st.session_state.ws_active = True
                st.session_state.ws_symbols = ws_syms

    progress.progress(100, "Готово!"); time.sleep(0.3); progress.empty()
    # scan_running сбрасывается в finally блоке run_scan()


# ═══════════════════════════════════════════════════
# ALGO SCAN — БЕЗ BBO, с mover_events
# ═══════════════════════════════════════════════════
def run_algo_scan():
    results = st.session_state.scan_results
    if not results: st.warning("Сначала запусти основной скан"); return
    client = st.session_state.client
    tb = st.session_state.trades_buffer
    # Чистим буфер от сделок старше max_age_sec перед анализом
    try:
        tb.cleanup()
    except Exception:
        pass
    tracker = st.session_state.tracker
    signals = {}
    candidates = [r for r in results if r.spread_pct >= 0.3]
    if not candidates: candidates = results[:20]
    total = min(len(candidates), 30)
    progress = st.progress(0, "🤖 Алго-скан...")

    for idx, r in enumerate(candidates[:total]):
        sym = r.symbol
        try:
            # Свежие сделки: WS буфер + REST дополнение
            # Сначала берём что уже есть в буфере (WS мог накопить данные)
            ws_trades = tb.get_trades(sym)

            parsed = []
            try:
                # /api/v3/trades приоритет (не требует подписи на MEXC)
                raw = client.get_recent_trades(sym, cfg_module.ALGO_TRADES_REST_LIMIT)
                if raw and isinstance(raw, list) and len(raw) >= 5:
                    parsed = parse_rest_trades(raw)
            except: pass

            if len(parsed) < 10:
                # Fallback: aggTrades — B-07 fix: заменяем parsed только если новый результат длиннее
                try:
                    raw = client.get_agg_trades(sym, cfg_module.ALGO_TRADES_REST_LIMIT)
                    if raw and isinstance(raw, list) and len(raw) >= 5:
                        parsed2 = parse_agg_trades(raw)
                        if len(parsed2) > len(parsed):
                            parsed = parsed2
                except: pass

            # Мержим: REST + WS (без дублей по времени)
            if parsed:
                tb.clear(sym)
                tb.add_trades(sym, parsed)
                # Добавляем WS сделки если они новее последней REST сделки
                if ws_trades:
                    last_rest_ts = max((t.time_ms for t in parsed), default=0)
                    fresh_ws = [t for t in ws_trades if t.time_ms > last_rest_ts]
                    if fresh_ws:
                        tb.add_trades(sym, fresh_ws)
            elif ws_trades and len(ws_trades) >= 5:
                # Только WS данные — тоже подходит
                pass  # tb уже содержит ws_trades

            # Свежий стакан → актуальный спред
            fresh_spread = r.spread_pct
            fresh_mid = r.mid_price
            try:
                ob = client.get_order_book(sym, 5)
                if ob and ob.get("bids") and ob.get("asks"):
                    fb = sf(ob["bids"][0][0]); fa = sf(ob["asks"][0][0])
                    if fb > 0 and fa > 0:
                        fresh_spread = round((fa - fb) / fb * 100, 4)
                        fresh_mid = (fb + fa) / 2
            except: pass

            ladders = r.ladders if hasattr(r, 'ladders') else []
            mover_events = tracker.get_symbol_movers(sym)
            tracked_walls = tracker.get_wall_behavior_raw(sym)
            # Свежие сделки для анализа тайминга (последние 5 минут)
            trades = tb.get_trades(sym, last_sec=300)
            # Fallback: если мало свежих — берём всё из буфера
            if len(trades) < 8:
                trades = tb.get_trades(sym)
            _algo_scan_min = 5  # минимум для кластерного анализа
            if len(trades) >= _algo_scan_min:
                sig = analyze_algo(sym, trades, fresh_spread,
                                   ladders=ladders,
                                   mover_events=mover_events,
                                   current_mid_price=fresh_mid,
                                   tracked_walls=tracked_walls)
                # Сохраняем реальное кол-во сделок с биржи
                tc24 = r.trade_count_24h
                if tc24 == 0:
                    try:
                        tk24 = client.get_ticker_24h(sym)
                        if tk24 and isinstance(tk24, dict):
                            tc24 = si(tk24.get("count", 0))
                    except: pass
                sig._trade_count_24h = tc24
                signals[sym] = sig
        except Exception as _ae:
            # Логируем ошибку — но не прерываем скан
            import logging
            logging.getLogger("algo_scan").debug(f"{sym}: {_ae}")
        if (idx+1) % 3 == 0 or idx == total-1:
            found = sum(1 for s in signals.values() if s.has_algo)
            skipped = (idx+1) - len(signals)
            progress.progress(int((idx+1)/total*100),
                f"🤖 {idx+1}/{total} — {found} алгоритмов | {skipped} пропущено")
    st.session_state.algo_signals = signals
    st.session_state.algo_last_scan = time.time()
    # B-26 fix: сохраняем статистику для информативного отображения при нулевом результате
    st.session_state["_algo_scan_stats"] = {
        "checked": total,
        "no_data": total - len(signals),
        "found": sum(1 for s in signals.values() if s.has_algo),
    }
    progress.progress(100, "Готово!"); time.sleep(0.3); progress.empty()


# ═══════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════
TF_MULT = cfg_module.VOLUME_TIMEFRAMES

with st.sidebar:
    st.markdown("## ⚙️ Параметры сканера")
    # B-25 fix: предупреждение во время скана
    if st.session_state.get("scan_running"):
        st.warning("⏳ Скан выполняется... Изменение параметров перезапустит его.")
    st.markdown("---")
    vol_tf = st.selectbox("Таймфрейм объёма", list(TF_MULT.keys()), index=4,
        key="vol_tf", label_visibility="collapsed")
    min_vol_input = st.number_input(f"Мин объём {vol_tf} ($)", value=100, min_value=0, step=50)
    max_vol_input = st.number_input(f"Макс объём {vol_tf} ($)", value=500_000, min_value=100, step=10000)
    mult = TF_MULT[vol_tf]; min_vol = min_vol_input * mult; max_vol = max_vol_input * mult
    if vol_tf != "24h": st.caption(f"≈ ${min_vol:,.0f} – ${max_vol:,.0f} за 24ч")
    st.markdown("---")
    min_spread = st.slider("Мин спред %", 0.0, 20.0, 0.5, 0.1)
    wall_mult = st.slider("Чувствительность стенок (x)", 2, 50, 5)
    min_wall_usd = st.number_input("Мин стенка ($)", value=50, min_value=1, step=10)
    min_trades_flt = st.number_input("Мин сделок 24ч", value=0, min_value=0, step=100,
        help="Отсечь пары с малым числом сделок. 0 = без фильтра.")
    top_n = st.slider("Макс результатов", 5, 100, 30)
    st.markdown("---")
    auto_on = st.checkbox("Авто-скан", value=False)
    auto_sec = st.select_slider("Интервал (сек)", [30,45,60,90,120,180], value=60)
    if auto_on:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=auto_sec * 1000, key="ar")
        except ImportError:
            st.caption("pip install streamlit-autorefresh")
    c1s, c2s = st.columns(2)
    # B-27 fix: кнопка СКАН недоступна пока скан уже идёт
    _is_scanning = st.session_state.get("scan_running", False)
    scan_btn = c1s.button("🔍 СКАН", width="stretch", type="primary",
                         disabled=_is_scanning)
    if c2s.button("⛔ СТОП", width="stretch"): st.session_state.cancel_scan = True

    st.markdown("---")
    st.markdown("**📦 Экспорт**")
    def _build_full_zip():
        buf = io.BytesIO()
        _results = st.session_state.scan_results; _tracker = st.session_state.tracker
        _client = st.session_state.get("client")
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            if _results:
                sr = [{"Score": r.score, "Symbol": r.symbol, "Spread%": round(r.spread_pct,2),
                    "Vol24h$": round(r.volume_24h_usdt), "Ladders": len(r.ladders) if hasattr(r,'ladders') else 0}
                    for r in _results if r.all_walls]
                if sr: zf.writestr("scan_results.csv", pd.DataFrame(sr).to_csv(index=False))
            asigs = st.session_state.algo_signals
            if asigs:
                ar = [{"Symbol": s, "Score": sig.algo_score, "Verdict": sig.verdict,
                    "Honesty": sig.honesty_str, "Behavior": sig.behavior_str,
                    "Ladder": sig.ladder_str, "Trades24h": getattr(sig, '_trade_count_24h', 0),
                    # Buf5м = свежие сделки за последние 5 мин
                    "Buf5м": len(st.session_state.trades_buffer.get_trades(sig.symbol, last_sec=300)),
                    # B-06 fix: убран API-вызов count_trades_5m из экспортной функции.
                    # Использем закешированное значение — API в ZIP недопустим (блокирует UI).
                    "Trades5m_exchange": getattr(sig, "_trades_5m", 0)}
                    for s, sig in asigs.items()]
                zf.writestr("algo_signals.csv", pd.DataFrame(ar).to_csv(index=False))
            import json
            zf.writestr("meta.json", json.dumps({"exported": datetime.now().isoformat(),
                **_tracker.get_stats()}, indent=2))
        buf.seek(0); return buf.getvalue()

    if st.session_state.scan_results:
        st.download_button("📦 Скачать ВСЁ (ZIP)", data=_build_full_zip(),
            file_name=f"mexc_full_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            mime="application/zip", width="stretch")
    else: st.caption("Сначала запусти скан")

    st.markdown("---")
    with st.expander("🚫 Чёрный список"):
        bl_inp = st.text_input("Добавить", placeholder="XYZUSDT", key="bl_inp")
        if bl_inp:
            for s in bl_inp.upper().replace(" ","").split(","):
                # B-29 fix: ограничение длины + только буквы/цифры (защита от мусора и инъекций)
                s = s[:20].strip()
                if s.endswith("USDT") and s.isalnum():
                    st.session_state.blacklist.add(s)
            st.rerun()
        if st.session_state.blacklist:
            st.caption(", ".join(sorted(st.session_state.blacklist)))
            if st.button("Очистить"): st.session_state.blacklist = set(); st.rerun()
    with st.expander("⭐ Избранное"):
        fav = st.session_state.favorites
        if fav: st.caption(", ".join(sorted(fav)))
        up = st.file_uploader("Import CSV", type=["csv","txt"], key="fi", label_visibility="collapsed")
        if up:
            new = {l.strip().upper() for l in up.getvalue().decode("utf-8").replace(",","\n").split("\n")
                if l.strip().upper().endswith("USDT") and len(l.strip()) > 4}
            if new: st.session_state.favorites.update(new); st.rerun()
        if fav: st.download_button("Export", data="\n".join(sorted(fav)).encode(),
            file_name="fav.csv", mime="text/csv", width="stretch")
    st.markdown("---")
    stats = st.session_state.tracker.get_stats()
    algo_cnt = sum(1 for s in st.session_state.algo_signals.values() if s.has_algo)
    st.caption(f"Сканов: {stats['total_scans']} | Переставок: {stats['total_mover_events']} | Алгоритмов: {algo_cnt}")
    # ── Phase 4: WS мониторинг ──────────────────────────────
    st.markdown("---")
    if _HAS_WS and _ws_manager is not None:
        ws_on = _ws_manager.is_running()
        ws_stats = _ws_manager.get_ws_stats() if ws_on else {}
        if ws_on:
            uptime_sec = int(time.time() - ws_stats.get("start_time", time.time()))
            uptime_str = f"{uptime_sec//60}м {uptime_sec%60}с" if uptime_sec >= 60 else f"{uptime_sec}с"
            n_syms = len(st.session_state.get("ws_symbols", []))
            st.markdown(
                f"🟢 **WebSocket** активен · {n_syms} пар · {uptime_str}\n\n"
                f"📨 {ws_stats.get('messages',0)} сообщ · "
                f"🔀 {ws_stats.get('movers_detected',0)} переставок · "
                f"🤖 {ws_stats.get('algos_detected',0)} алгоритмов"
            )
            # Recovery статистика из DB
            if st.session_state.get("db"):
                try:
                    # B-11 fix: thread-safe метод вместо прямого _conn из UI-потока
                    total_rec, done_rec = st.session_state.db.get_recovery_counts()
                    if total_rec > 0:
                        st.caption(f"💊 Recovery: {done_rec}/{total_rec} событий записано")
                except Exception:
                    pass
            if st.button("⏹ Остановить WS", width="stretch"):
                _ws_manager.stop()
                st.session_state.ws_active = False
                st.rerun()
        else:
            # B-04 fix: показываем причину остановки если WS упал
            _err = getattr(_ws_manager, "_last_ws_error", "")
            _ecnt = getattr(_ws_manager, "_ws_error_count", 0)
            if _ecnt >= 5:
                st.markdown("🔴 **WebSocket** остановлен (5 сбоев подряд)")
                if _err:
                    st.caption(f"Последняя ошибка: {_err[:80]}")
            else:
                st.markdown("⚫ **WebSocket** не активен")
            syms_for_ws = [r.symbol for r in st.session_state.scan_results[:20]]
            if syms_for_ws:
                if st.button("▶ Запустить WS мониторинг", width="stretch"):
                    ok = _ws_manager.start(syms_for_ws)
                    if ok:
                        st.session_state.ws_active = True
                        st.session_state.ws_symbols = syms_for_ws
                        st.rerun()
            else:
                st.caption("Сначала запусти скан")
    else:
        st.caption("WebSocket недоступен (pip install websockets aiohttp)")
    st.markdown("---")

    if st.button("🏓 Test API", width="stretch"):
        ok, msg = st.session_state.client.ping()
        st.success(msg) if ok else st.error(msg)


# ======= Auto-scan =======
# B-08 fix: добавлена проверка last_scan > 0 — автоскан не запускается при cold start
# (когда last_scan == 0.0). Первый запуск только по кнопке СКАН.
need_scan = scan_btn or (
    auto_on and st.session_state.current_page == 0
    and st.session_state.last_scan > 0
    and time.time() - st.session_state.last_scan > max(auto_sec - 3, 10))
if need_scan:
    # Чистим старые сделки перед новым сканом
    try:
        st.session_state.trades_buffer.cleanup()
    except Exception:
        pass
    run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n, min_trades_flt)

# ── Phase 4: Обогащаем algo_signals из WS буфера ─────────────
if _HAS_WS and _ws_manager is not None and _ws_manager.is_running():
    ws_sigs = _ws_manager.get_algo_signals()
    if ws_sigs:
        existing = st.session_state.algo_signals
        # Мерджим: WS сигнал побеждает только если algo_score выше
        for sym, sig in ws_sigs.items():
            prev = existing.get(sym)
            if prev is None or sig.algo_score > prev.algo_score:
                existing[sym] = sig
        st.session_state.algo_signals = existing

    # Обогащаем trades_buffer из WS для символов открытых в стакане
    detail_sym = st.session_state.get("detail_symbol", "")
    if detail_sym:
        ws_trades = _ws_manager.get_trades_for_symbol(detail_sym)
        if ws_trades:
            st.session_state.trades_buffer.add_trades(detail_sym, ws_trades)


# ═══════════════════════════════════════════════════
# НАВИГАЦИЯ
# ═══════════════════════════════════════════════════
# B-30 fix: badge скринера обновляется динамически при каждом rerun
_sc_count = sum(
    1 for r in st.session_state.get("screener_results", [])
    if getattr(r, "is_workable", False)
)
_screener_label = f"🎯 Скринер ({_sc_count} 🔥)" if _sc_count > 0 else "🎯 Скринер"
TAB_LABELS = ["📊 Сканер", "🔍 Стакан", "📈 Переставки", "🤖 Ёршики", _screener_label, "📊 Рендж"]
cp = st.session_state.current_page
# B-27 fix: навигация заблокирована пока идёт скан
_nav_disabled = st.session_state.get("scan_running", False)
st.markdown("""<style>div[data-testid="stHorizontalBlock"]>div>div>button{font-size:1.05rem!important;
font-weight:600!important;padding:0.55rem 0!important;border-radius:8px!important;}</style>""",
    unsafe_allow_html=True)
nav_cols = st.columns(len(TAB_LABELS))
for i, label in enumerate(TAB_LABELS):
    with nav_cols[i]:
        if st.button(label, key=f"nav_{i}", width="stretch",
                     disabled=_nav_disabled,
                     type="primary" if cp == i else "secondary"):
            st.session_state.current_page = i; st.rerun()
st.markdown("---")
page = cp


# ═════════════════════════════════════════════════
# PAGE 0: СКАНЕР
# ═════════════════════════════════════════════════
if page == 0:
    results = st.session_state.scan_results
    tracker = st.session_state.tracker
    if not results:
        if auto_on:
            st.info("⏳ Ожидание первого скана...")
        else:
            st.info("Нажми 🔍 СКАН в боковой панели")
    else:
        mc1,mc2,mc3,mc4 = st.columns(4)
        mc1.metric("Найдено", len(results)); mc2.metric("Проверено", st.session_state.total_pairs)
        mc3.metric("Лучший скор", f"{results[0].score}")
        mc4.metric("Переставки", sum(1 for r in results if r.has_movers))
        rows = []
        for r in results:
            if not r.all_walls: continue
            tw_list = tracker.get_tracked_walls(r.symbol)
            tw_big = tw_list[0] if tw_list else None
            lt = tw_big.lifetime_str if tw_big else "---"
            bt = max(r.bid_walls, key=lambda w: w.size_usdt) if r.bid_walls else None
            at = max(r.ask_walls, key=lambda w: w.size_usdt) if r.ask_walls else None
            t5 = getattr(r, '_trades_5m', 0)
            lc = len(r.ladders) if hasattr(r, 'ladders') else 0
            pch = float(getattr(r, '_price_change_pct', 0) or 0)
            if abs(pch) > 100:
                pch_str = f"🚀 {pch:+.1f}%"
            else:
                pch_str = f"{pch:+.1f}%"
            rows.append({"Скор": r.score, "Пара": r.symbol,
                "Рост24ч": pch_str,
                "Спред%": round(r.spread_pct,2), "Об24ч$": round(r.volume_24h_usdt),
                "BID$": round(bt.size_usdt) if bt else 0, "BIDx": bt.multiplier if bt else 0,
                "Сд5м": int(t5) if t5 else 0,
                "ASK$": round(at.size_usdt) if at else 0, "ASKx": at.multiplier if at else 0,
                "🪜": str(lc) if lc > 0 else "", "Жизнь": lt})
        if rows:
            df = pd.DataFrame(rows).sort_values("Скор", ascending=False).reset_index(drop=True)
            st.caption("🪜 = лесенки. 🚀 = рост > 100% за 24ч.")
            st.dataframe(df, hide_index=True, width="stretch",
                         height=min(len(df)*35+40, 700))
            syms = df["Пара"].tolist()
            nc = min(10, len(syms))
            btn_cols = st.columns(nc)
            for i, sym in enumerate(syms[:nc]):
                with btn_cols[i]:
                    if st.button(sym, key=f"go_{sym}", width="stretch"):
                        go_detail(sym); st.rerun()
            if len(syms) > nc:
                sc, gc = st.columns([3,1])
                with sc: chosen = st.selectbox("Все пары", [""]+syms, key="all_pairs")
                with gc:
                    if chosen and st.button("➡️ Открыть", key="go_ch"): go_detail(chosen); st.rerun()
            # Счётчик отфильтрованных wash-trade / памп монет
            filtered_n = st.session_state.get('_last_filtered_wt', 0)
            if filtered_n > 0:
                st.caption(f"⚡ Отфильтровано: {filtered_n} монет (wash-trade <50 сделок или памп >300%)")
            st.download_button("📥 CSV", data=make_csv(df),
                file_name=f"scan_{datetime.now().strftime('%H%M')}.csv", mime="text/csv")


# ═════════════════════════════════════════════════
# PAGE 1: СТАКАН + ДЕТАЛЬНЫЙ РАЗБОР
# ═════════════════════════════════════════════════
elif page == 1:
    results = st.session_state.scan_results
    sym_list = [r.symbol for r in results] if results else []
    hdr = st.columns([1,3,2,1,1])
    with hdr[0]:
        if st.button("← Назад"): st.session_state.current_page = 0; st.rerun()
    with hdr[1]:
        idx = 0; ds = st.session_state.detail_symbol
        if ds and ds in sym_list: idx = sym_list.index(ds)+1
        target = st.selectbox("Пара", [""]+sym_list, index=idx, key="detail_sel", label_visibility="collapsed")
    with hdr[2]:
        manual = st.text_input("Ввод", placeholder="XYZUSDT", label_visibility="collapsed")
    symbol = manual.strip().upper() if manual.strip() else target
    with hdr[3]:
        if symbol:
            is_fav = symbol in st.session_state.favorites
            if st.button("⭐" if is_fav else "☆", key="fav_d"):
                (st.session_state.favorites.discard if is_fav else st.session_state.favorites.add)(symbol)
                st.rerun()
    with hdr[4]:
        if symbol and st.button("🚫", key="bl_d"):
            st.session_state.blacklist.add(symbol); st.rerun()
    if not symbol: st.info("Выбери пару"); st.stop()
    st.session_state.detail_symbol = symbol
    client = st.session_state.client; tracker = st.session_state.tracker
    with st.spinner(f"Загрузка {symbol}..."):
        try:
            book_raw = client.get_order_book(symbol, 500)
            ticker_raw = client.get_ticker_24h(symbol)
            trades_raw = client.get_agg_trades(symbol, 1000)
            if not trades_raw or not isinstance(trades_raw, list) or len(trades_raw) < 5:
                trades_raw = client.get_recent_trades(symbol, 1000)
            klines_data = {}
            for tf_key, tf_cfg in cfg_module.CHART_INTERVALS.items():
                klines_data[tf_key] = parse_klines(client.get_klines(symbol, tf_cfg["api"], tf_cfg["limit"]))
        except Exception as e: st.error(str(e)); st.stop()
    if not book_raw: st.error(f"Нет стакана {symbol}"); st.stop()
    if not book_raw.get("bids") or not book_raw.get("asks"): st.error("Пустой стакан"); st.stop()
    bids = parse_book(book_raw["bids"]); asks = parse_book(book_raw["asks"])
    if not bids or not asks: st.error("Пустой стакан"); st.stop()
    bb, ba = float(bids[0][0]), float(asks[0][0])
    mid = (bb+ba)/2; spread = (ba-bb)/bb*100
    bdepth = sum(float(p)*float(q) for p,q in bids)
    adepth = sum(float(p)*float(q) for p,q in asks)
    td = ticker_raw
    if isinstance(td, list): td = td[0] if td else {}
    if not isinstance(td, dict): td = {}
    tc24 = extract_tc(td); vol24 = sf(td.get("quoteVolume",0))
    df_5m = klines_data.get("5m", pd.DataFrame()); df_1h = klines_data.get("1h", pd.DataFrame())

    st.markdown(f"### {symbol}  •  {fmt_price(mid)}  •  [MEXC ↗]({mexc_link(symbol)})")
    s5 = kline_stats(df_5m, 1); s4h = kline_stats(df_1h, 4)
    t5cnt = count_trades_5m_from_recent(trades_raw)
    if t5cnt == 0: t5cnt = s5["trades"]
    m1,m2,m3,m4,m5,m6,m7 = st.columns(7)
    m1.metric("Спред", f"{spread:.2f}%"); m2.metric("Bid", f"${bdepth:,.0f}")
    m3.metric("Ask", f"${adepth:,.0f}"); m4.metric("Сд.5м", f"{t5cnt:,}")
    m5.metric("24ч сд", f"{tc24:,}" if tc24 else "—"); m6.metric("24ч $", f"${vol24:,.0f}")
    m7.metric("4ч $", f"${s4h['volume']:,.0f}")

    # График
    st.markdown("#### 📈 Цена")
    tf_labels = list(cfg_module.CHART_INTERVALS.keys())
    tf_cols = st.columns(len(tf_labels))
    if "chart_tf" not in st.session_state: st.session_state.chart_tf = "5m"
    for i, lbl in enumerate(tf_labels):
        with tf_cols[i]:
            if st.button(lbl, key=f"ctf_{lbl}", width="stretch",
                         type="primary" if st.session_state.chart_tf == lbl else "secondary"):
                st.session_state.chart_tf = lbl; st.rerun()
    fig_c = build_candlestick_dual(klines_data.get(st.session_state.chart_tf, pd.DataFrame()),
        symbol, st.session_state.chart_tf, mid)
    if fig_c: st.plotly_chart(fig_c, width="stretch")

    # ── АЛГО-ДЕТЕКТОР ──
    st.markdown("#### 🤖 Алго-детектор v3")
    parsed_trades = []
    if trades_raw and isinstance(trades_raw, list):
        # Определяем формат: aggTrades (поле "p") или trades (поле "price")
        sample = trades_raw[0] if trades_raw else {}
        if isinstance(sample, dict) and "p" in sample:
            parsed_trades = parse_agg_trades(trades_raw)
        else:
            parsed_trades = parse_rest_trades(trades_raw)
    from analyzer import detect_ladders
    detail_ladders = detect_ladders(bids, asks, mid)
    mover_events = tracker.get_symbol_movers(symbol)
    tracked_walls = tracker.get_wall_behavior_raw(symbol)

    if len(parsed_trades) >= cfg_module.ALGO_MIN_TRADES:
        tb = st.session_state.trades_buffer
        tb.clear(symbol); tb.add_trades(symbol, parsed_trades)
        algo_sig = analyze_algo(symbol, parsed_trades, spread,
                                ladders=detail_ladders,
                                mover_events=mover_events,
                                current_mid_price=mid,
                                tracked_walls=tracked_walls)

        # Метрики 7 колонок
        ac = st.columns(7)
        ac[0].metric("Скор", f"{algo_sig.algo_score:.0f}/100")
        ac[1].metric("Вердикт", algo_sig.verdict)
        ac[2].metric("Сайз", algo_sig.cluster_size_str)
        ac[3].metric("Тайминг", algo_sig.timing_str)
        ac[4].metric("Честность", algo_sig.honesty_str)
        ac[5].metric("Лесенки", algo_sig.ladder_str)
        ac[6].metric("Поведение", algo_sig.behavior_str)

        # Честность v2 — детали
        if algo_sig.honesty and algo_sig.honesty.total_checked > 0:
            hc = algo_sig.honesty
            if not hc.is_honest:
                st.error(f"❌ **Self-trade / Wash trading** — скор честности {hc.honesty_score:.0f}/70. {hc.details}")
            elif hc.honesty_score < getattr(cfg_module, "HONESTY_SCORE_HONEST", 55):
                st.warning(f"⚠️ **Под вопросом** — скор {hc.honesty_score:.0f}/70. {hc.details}")
            else:
                st.success(f"✅ **Честный мейкер** — скор {hc.honesty_score:.0f}/70. {hc.details}")

            # Компоненты честности
            with st.expander("📊 Разбор честности"):
                hcc = st.columns(3)
                hcc[0].metric("Сторона", f"{hc.dominant_side} {hc.dominant_side_pct:.0f}%",
                    f"{hc.side_score:.0f}/40 баллов")
                hcc[1].metric("Цен.уровней", f"{hc.unique_price_levels} на {hc.total_checked} сд",
                    f"{hc.price_score:.0f}/30 баллов")
                hcc[2].metric("Self-trade", f"{hc.selftrade_pairs} пар ({hc.selftrade_pct:.0f}%)",
                    f"-{hc.selftrade_penalty:.0f} штраф")
                st.caption(
                    "Side consistency: если 80%+ сделок одного типа → робот ставит лимитки (честный). "
                    "Price clustering: мало ценовых уровней = лимитные ордера. "
                    "Self-trade: buy+sell одна цена за 1.5с = wash trading.")

        # Поведение стенки — детали
        if algo_sig.wall_behavior and algo_sig.wall_behavior.total_events >= getattr(cfg_module, "WALL_BEHAVIOR_MIN_EVENTS", 3):
            wb = algo_sig.wall_behavior
            st.markdown(f"**{wb.emoji} Поведение стенки:** {wb.description}")
            st.info(wb.trade_advice)
            with st.expander("📊 Разбор поведения"):
                wbc = st.columns(3)
                wbc[0].metric("🏃 Убегает", f"{wb.retreat_count} ({wb.retreat_pct:.0f}%)")
                wbc[1].metric("🦈 Давит", f"{wb.advance_count} ({wb.advance_pct:.0f}%)")
                wbc[2].metric("↔ Вбок", f"{wb.same_count}")
                st.caption(
                    "Retreat = стенка удаляется от цены (поджим работает). "
                    "Advance = стенка приближается (робот давит). "
                    "Анализ по истории переставок.")

        # Scatter
        fig_sc = build_trades_scatter(parsed_trades, algo_sig)
        if fig_sc: st.plotly_chart(fig_sc, width="stretch")

        # Кластеры
        if algo_sig.clusters:
            st.markdown("**Кластеры объёмов:**")
            cl_rows = [{"#": i+1, "Центр $": f"${c.center_usdt:.2f}", "Сделок": c.count,
                "% всех": f"{c.pct_of_total:.1f}%", "Диапазон": c.size_range_str,
                "Тайминг": analyze_timing(c).description, "Доминант": "✅" if c.is_dominant else ""}
                for i, c in enumerate(algo_sig.clusters[:5])]
            st.dataframe(pd.DataFrame(cl_rows), hide_index=True, width="stretch")

        dc = algo_sig.dominant_cluster
        if dc and len(dc.trades) >= 5:
            h1,h2 = st.columns(2)
            with h1:
                fh = build_timing_histogram(dc)
                if fh: st.plotly_chart(fh, width="stretch")
            with h2:
                fs = build_seconds_heatmap(dc)
                if fs: st.plotly_chart(fs, width="stretch")
    else:
        st.caption(f"Мало сделок ({len(parsed_trades)} < {cfg_module.ALGO_MIN_TRADES})")

    # Лесенки
    if detail_ladders:
        st.markdown("#### 🪜 Лесенки")
        lr = [{"Сторона": "🟢 BID" if l.side == "BID" else "🔴 ASK",
               "Ступеней": l.steps, "Шаг%": f"{l.price_step_pct:.3f}%",
               "$/ступ": fmt_usd(l.avg_size_usdt), "Всего": fmt_usd(l.total_usdt),
               "Ровность": f"{100-l.size_std_pct:.0f}%",
               "Сила": "💪" if l.is_strong else ""} for l in detail_ladders]
        st.dataframe(pd.DataFrame(lr), hide_index=True, width="stretch")

    # Последние сделки
    st.markdown("#### 📋 Последние сделки")
    if trades_raw and isinstance(trades_raw, list) and len(trades_raw) > 0:
        trade_rows = []
        for t in trades_raw[:50]:
            try:
                p = sf(t.get("price", t.get("p", 0)))
                q = sf(t.get("qty", t.get("q", 0)))
                ts = sf(t.get("time", t.get("T", 0)))
                is_buy = not t.get("isBuyerMaker", t.get("m", True))
                tm_str = pd.to_datetime(ts, unit="ms").strftime("%H:%M:%S") if ts > 0 else "---"
                usdt_val = round(p * q, 2)
                trade_rows.append({
                    "Время": tm_str,
                    "Цена": fmt_price(p),
                    "Кол-во": f"{q:g}",
                    "Объём $": f"${usdt_val:.2f}",
                    "Тип": "🟢 BUY" if is_buy else "🔴 SELL"
                })
            except: continue
        if trade_rows:
            st.dataframe(pd.DataFrame(trade_rows), hide_index=True, width="stretch",
                         height=min(len(trade_rows)*35+40, 500))
        else:
            st.caption("Нет данных по сделкам")
    else:
        st.caption("Нет данных по сделкам")

    # Стенки
    st.markdown("#### 🧱 Стенки")
    tw_list = tracker.get_tracked_walls(symbol)
    if tw_list:
        st.dataframe(pd.DataFrame([{"Сторона": "🟢 BID" if tw.side=="BID" else "🔴 ASK",
            "Цена": fmt_price(tw.price), "Объём": fmt_usd(tw.size_usdt),
            "Множ.": f"{tw.multiplier}x", "Жизнь": tw.lifetime_str, "Сканов": tw.seen_count}
            for tw in tw_list]), hide_index=True, width="stretch")

    # Объёмы
    st.markdown("#### 📊 Объёмы")
    s15 = kline_stats(df_5m,3); s60 = kline_stats(df_5m,12)
    vc = st.columns(5)
    vc[0].metric("5м", f"${s5['volume']:,.0f}"); vc[1].metric("15м", f"${s15['volume']:,.0f}")
    vc[2].metric("1ч", f"${s60['volume']:,.0f}"); vc[3].metric("4ч", f"${s4h['volume']:,.0f}")
    vc[4].metric("24ч", f"${vol24:,.0f}")

    # Стакан + Хитмап
    st.markdown("#### 📕 Стакан")
    dv = st.select_slider("Глубина", [20,30,50,100], value=50, key="ob_depth")
    c1,c2 = st.columns(2)
    with c1:
        fg = build_orderbook_chart(bids, asks, mid, dv)
        if fg: st.plotly_chart(fg, width="stretch")
    with c2:
        fh = build_heatmap(bids, asks, mid, 30)
        if fh: st.plotly_chart(fh, width="stretch")

    # Экспорт
    st.markdown("---")
    ob_df = pd.DataFrame([{"Side": s, "Price": float(p), "Qty": float(q), "USDT": round(float(p*q),4)}
        for s, data in [("BID",bids),("ASK",asks)] for p,q in data])
    ec = st.columns(3)
    with ec[0]: st.download_button("📕 Стакан", data=make_csv(ob_df),
        file_name=f"{symbol}_ob.csv", mime="text/csv", width="stretch")
    with ec[1]: st.download_button("📦 ВСЁ", data=_build_full_zip(),
        file_name="mexc_all.zip", mime="application/zip", width="stretch")


# ═════════════════════════════════════════════════
# PAGE 2: ПЕРЕСТАВКИ
# ═════════════════════════════════════════════════
elif page == 2:
    tracker = st.session_state.tracker
    st.markdown("### 📈 Монитор переставок")
    sub_labels = ["📋 Журнал", "🏆 Рейтинг"]
    if "mover_subtab" not in st.session_state: st.session_state.mover_subtab = 0
    sc = st.columns(2)
    for i, sl in enumerate(sub_labels):
        with sc[i]:
            if st.button(sl, key=f"msub_{i}", width="stretch",
                         type="primary" if st.session_state.mover_subtab==i else "secondary"):
                st.session_state.mover_subtab = i; st.rerun()
    st.markdown("---")
    if st.session_state.mover_subtab == 0:
        movers = tracker.get_active_movers(7200)
        if not movers: st.info("Нет переставок.")
        else:
            st.success(f"📊 {len(movers)} переставок за 2ч")
            mr = [{"Время": datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S"),
                   "Пара": e.symbol, "Сторона": e.side, "Объём": fmt_usd(e.size_usdt),
                   "Было": fmt_price(e.old_price), "Стало": fmt_price(e.new_price),
                   "Сдвиг%": f"{e.shift_pct:+.3f}%",
                   "Напр.": "⬆" if e.direction=="UP" else "⬇"} for e in reversed(movers)]
            mdf = pd.DataFrame(mr)
            st.dataframe(mdf, hide_index=True, width="stretch")
            sc2, gc = st.columns([3,1])
            with sc2: ch = st.selectbox("Перейти", [""]+sorted({e.symbol for e in movers}), key="m_sel")
            with gc:
                if ch and st.button("→", key="m_go"): go_detail(ch); st.rerun()
            st.download_button("📥 CSV", data=make_csv(mdf), file_name="movers.csv", mime="text/csv")
    else:
        top_movers = tracker.get_top_movers(20)
        if top_movers:
            for i, (sym, cnt) in enumerate(top_movers):
                rc = st.columns([3,1,1])
                medal = "🥇" if i==0 else ("🥈" if i==1 else ("🥉" if i==2 else f"{i+1}."))
                rc[0].markdown(f"**{medal} {sym}** — {cnt} переставок")
                if rc[1].button("🔍", key=f"r_{sym}"): go_detail(sym); st.rerun()
                if rc[2].button("⭐", key=f"f_{sym}"): st.session_state.favorites.add(sym); st.rerun()
        else: st.info("Нет данных.")


# ═════════════════════════════════════════════════
# PAGE 3: ЁРШИКИ v3
# ═════════════════════════════════════════════════
elif page == 3:
    st.markdown("### 🤖 Детектор алгоритмов v3")
    st.caption(
        "v3: честность анализируется по ВНУТРЕННИМ сигналам сделок (не по BBO). "
        "Нет ложных срабатываний из-за рассинхрона стакана/ленты. "
        "+ анализ поведения стенок (убегает / давит / стоит).")

    if st.button("🔍 Запустить алго-скан", type="primary", width="stretch"):
        run_algo_scan()

    signals = st.session_state.algo_signals
    if not signals:
        if st.session_state.algo_last_scan > 0:
            # B-26 fix: показываем детальную статистику вместо пустого сообщения
            _as = st.session_state.get("_algo_scan_stats", {})
            if _as:
                st.info(
                    f"Проверено пар: {_as['checked']} · "
                    f"Без данных: {_as['no_data']} · "
                    f"Алгоритмов не найдено"
                )
            else:
                st.info("Алгоритмы не найдены")
        else: st.info("Нажми кнопку. Сначала нужен основной скан.")
    else:
        sorted_sigs = sorted(signals.values(), key=lambda s: s.algo_score, reverse=True)
        fire = sum(1 for s in sorted_sigs if s.algo_score >= cfg_module.ALGO_SCORE_FIRE)
        robots = sum(1 for s in sorted_sigs if cfg_module.ALGO_SCORE_ROBOT <= s.algo_score < cfg_module.ALGO_SCORE_FIRE)
        scams = sum(1 for s in sorted_sigs if s.honesty and not s.honesty.is_honest)
        retreaters = sum(1 for s in sorted_sigs if s.wall_behavior and s.wall_behavior.behavior_type == "retreater")

        am = st.columns(5)
        am[0].metric("🔥 Жирные", fire)
        am[1].metric("⚠️ Роботы", robots)
        am[2].metric("❌ Жулики", scams)
        am[3].metric("🏃 Убегают", retreaters)
        am[4].metric("Всего", len(sorted_sigs))

        # Таблица
        algo_rows = []
        for sig in sorted_sigs:
            if sig.algo_score < 10 and (not sig.honesty or sig.honesty.is_honest): continue
            row = {"Скор": sig.algo_score, "Пара": sig.symbol, "Вердикт": sig.verdict,
                "Спред%": round(sig.spread_pct,2), "Сайз $": sig.cluster_size_str,
                "Тайминг": sig.timing_str, "Честность": sig.honesty_str,
                "Поведение": sig.behavior_str, "Лесенка": sig.ladder_str,
                "Сд24ч": getattr(sig, '_trade_count_24h', 0),
                "Буфер": sig.total_trades}
            if sig.dominant_cluster:
                row["Кластер%"] = f"{sig.dominant_cluster.pct_of_total:.0f}%"
            else: row["Кластер%"] = "—"
            if sig.honesty and sig.honesty.total_checked > 0:
                row["Self-trade"] = f"{sig.honesty.selftrade_pct:.0f}%"
            else: row["Self-trade"] = "—"
            algo_rows.append(row)

        if algo_rows:
            # Жулики отдельно
            scam_rows = [r for r in algo_rows if "❌" in r.get("Вердикт","")]
            if scam_rows:
                st.markdown("##### ❌ Жулики (self-trade / wash trading)")
                st.dataframe(pd.DataFrame(scam_rows), hide_index=True, width="stretch")
                st.markdown("---")

            # Честные
            honest_rows = [r for r in algo_rows if "❌" not in r.get("Вердикт","")]
            if honest_rows:
                st.markdown("##### ✅ Алгоритмы")
                st.dataframe(pd.DataFrame(honest_rows), hide_index=True, width="stretch",
                             height=min(len(honest_rows)*35+40, 600))

            # Кнопки
            st.markdown("##### Перейти к анализу")
            top_syms = [r["Пара"] for r in honest_rows[:12]]
            nc = min(6, len(top_syms))
            if nc > 0:
                acols = st.columns(nc)
                for i, sym in enumerate(top_syms[:nc]):
                    with acols[i]:
                        sc = next((r["Скор"] for r in algo_rows if r["Пара"]==sym), 0)
                        if st.button(f"{sym}\n({sc})", key=f"ag_{sym}", width="stretch"):
                            go_detail(sym); st.rerun()

            # Лучший — scatter
            if sorted_sigs and sorted_sigs[0].algo_score >= cfg_module.ALGO_SCORE_ROBOT:
                best = sorted_sigs[0]
                st.markdown(f"#### 🔍 Лучший: {best.symbol} (скор {best.algo_score})")
                if best.honesty and best.honesty.total_checked > 0:
                    hc = best.honesty
                    if hc.is_honest: st.success(f"✅ Честный — {hc.details}")
                    else: st.error(f"❌ Жулик — {hc.details}")
                if best.wall_behavior and best.wall_behavior.total_events >= getattr(cfg_module, "WALL_BEHAVIOR_MIN_EVENTS", 3):
                    wb = best.wall_behavior
                    st.info(f"{wb.emoji} **{wb.description}** — {wb.trade_advice}")

                bt = st.session_state.trades_buffer.get_trades(best.symbol)
                if bt:
                    fig = build_trades_scatter(bt, best)
                    if fig: st.plotly_chart(fig, width="stretch")
                    if best.dominant_cluster and len(best.dominant_cluster.trades) >= 5:
                        h1,h2 = st.columns(2)
                        with h1:
                            fh = build_timing_histogram(best.dominant_cluster)
                            if fh: st.plotly_chart(fh, width="stretch")
                        with h2:
                            fs = build_seconds_heatmap(best.dominant_cluster)
                            if fs: st.plotly_chart(fs, width="stretch")

            st.download_button("📥 Алгоритмы CSV", data=make_csv(pd.DataFrame(algo_rows)),
                file_name=f"algos_{datetime.now().strftime('%H%M')}.csv", mime="text/csv")
        else:
            st.info("Алгоритмы не обнаружены.")

        st.markdown("---")
        st.caption(f"Алго-скан: {datetime.fromtimestamp(st.session_state.algo_last_scan).strftime('%H:%M:%S') if st.session_state.algo_last_scan else '—'}")


# ═════════════════════════════════════════════════
# PAGE 4: ЁРШ-СКРИНЕР
# ═════════════════════════════════════════════════
elif page == 4:
    if render_robot_screener_tab is not None:
        render_robot_screener_tab(
            tracker=st.session_state.tracker,
            algo_signals=st.session_state.get("algo_signals", {}),
            client=st.session_state.get("client"),
        )
    else:
        st.error("❌ robot_screener.py не найден. Поместите файл рядом с app.py.")
        st.info("Требуемые файлы: robot_screener.py, history.py, algo_detector.py, book_quality.py, hunter_detector.py")


# ═════════════════════════════════════════════════
# PAGE 5: RANGE BOUNCE SCANNER
# ═════════════════════════════════════════════════
elif page == 5:
    from range_bounce_scanner import render_range_bounce_tab
    render_range_bounce_tab(
        client=st.session_state.client,
        trades_buffer=st.session_state.trades_buffer,
    )


# WS статус бейдж в футере
if _HAS_WS and _ws_manager is not None and _ws_manager.is_running():
    ws_stats_f = _ws_manager.get_ws_stats()
    msgs = ws_stats_f.get("messages", 0)
    movers = ws_stats_f.get("movers_detected", 0)
    st.caption(
        f"MEXC Scanner v7.2 — Algo Detector v3.1 + Robot Screener v4.1 + "
        f"🟢 WS Live ({msgs} сообщ, {movers} переставок)"
    )
else:
    st.caption("MEXC Scanner v7.2 — Algo Detector v3.1 + Robot Screener v4.1 | ⚫ WS offline")
