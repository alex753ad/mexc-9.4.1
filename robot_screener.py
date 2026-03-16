"""
Robot Screener v4.0 — Скринер «Рабочий Ёрш» (Phase 3)
=====================================================
Улучшения v4 (поверх v3):
- Multi-timeframe validation: проверка ренджа на 1h и 4h свечах
- Storage integration: сохранение/загрузка через ScannerDB
- Константы из config.py (SCREENER_*) вместо локальных
- Новый флаг FLAG_TREND_CONFLICT — рендж не подтверждён на старших TF
- trend_alignment_score — насколько свечной рендж совпадает со стаканным

Зависимости:
    history.py v5.0, algo_detector.py v3.1, config.py v7, storage.py v1
"""

import time
import math
import statistics
from dataclasses import dataclass, field
from typing import Optional

import config
from analyzer import ScanResult, WallInfo, LadderPattern
from algo_detector import AlgoSignal, WallBehavior, HonestyCheck
from history import DensityTracker, SymbolHistory, RangeBoundaryPoint, WallCountPoint


def _safe_int(v, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def estimate_trades_5m_exchange(client, symbol: str) -> int:
    """
    Реальные сделки за последние ~5 минут (по бирже).
    Приоритет: klines 5m (поле trades count), fallback: recent trades по времени.
    """
    if client is None or not symbol:
        return 0
    try:
        kl = client.get_klines(symbol, "5m", 1)
        if kl and isinstance(kl, list) and len(kl) > 0:
            last = kl[-1]
            if isinstance(last, (list, tuple)) and len(last) > 8:
                cnt = _safe_int(last[8], 0)
                if cnt > 0:
                    return cnt
    except Exception:
        pass
    try:
        trades = client.get_recent_trades(symbol, 500)
        if trades and isinstance(trades, list):
            now_ms = time.time() * 1000
            cutoff = now_ms - 5 * 60 * 1000
            cnt = 0
            for t in trades:
                try:
                    ts = float(t.get("time", 0))
                except Exception:
                    ts = 0
                if ts >= cutoff:
                    cnt += 1
            return cnt
    except Exception:
        return 0
    return 0

# Попытка импортировать storage (опционально)
try:
    from storage import ScannerDB
    _HAS_STORAGE = True
except ImportError:
    _HAS_STORAGE = False

# Phase 3.1: Book quality filter
try:
    from book_quality import check_book_quality, BookQuality
    _HAS_BOOK_QUALITY = True
except ImportError:
    _HAS_BOOK_QUALITY = False
    BookQuality = None

# Phase 3.1: Hunter pattern detector
try:
    from hunter_detector import detect_hunter_pattern, HunterPattern
    _HAS_HUNTER = True
except ImportError:
    _HAS_HUNTER = False
    HunterPattern = None


# ═══════════════════════════════════════════════════════════
# КОНСТАНТЫ — единственный источник истины: config.py (SCREENER_*)
#
# B-23 fix: убраны захардкоженные дефолты из _cfg(name, default).
# Раньше локальные копии могли расходиться с config.py при обновлении
# (например TIMING_NANO_CV_MAX имел дефолт 0.15 пока config уже 0.25).
# Теперь: config.py — единственный источник. Если константы нет в config —
# ImportError при старте, что лучше чем скрытое расхождение значений.
# ═══════════════════════════════════════════════════════════

RANGE_MIN_WIDTH_PCT        = config.SCREENER_RANGE_MIN_WIDTH_PCT
RANGE_MAX_WIDTH_PCT        = config.SCREENER_RANGE_MAX_WIDTH_PCT
RANGE_IDEAL_MIN_PCT        = config.SCREENER_RANGE_IDEAL_MIN_PCT
RANGE_IDEAL_MAX_PCT        = config.SCREENER_RANGE_IDEAL_MAX_PCT
RANGE_STABILITY_MIN_SCANS  = config.SCREENER_RANGE_STABILITY_MIN_SCANS
RANGE_DRIFT_THRESHOLD_PCT  = config.SCREENER_RANGE_DRIFT_THRESHOLD_PCT
RANGE_CLUSTER_TOP_N        = config.SCREENER_RANGE_CLUSTER_TOP_N
RANGE_LONG_DRIFT_MIN_POINTS = config.SCREENER_RANGE_LONG_DRIFT_MIN_POINTS

ROBOT_MIN_SCANS_PRESENT    = config.SCREENER_ROBOT_MIN_SCANS_PRESENT
ROBOT_RETREAT_MIN_PCT      = config.SCREENER_ROBOT_RETREAT_MIN_PCT

TIMING_NANO_CV_MAX         = config.SCREENER_TIMING_NANO_CV_MAX
TIMING_OK_AVG_MIN_SEC      = config.SCREENER_TIMING_OK_AVG_MIN_SEC

SELFTRADE_EXCLUDE_PCT      = config.SCREENER_SELFTRADE_EXCLUDE_PCT

SQUEEZE_EASY_RATIO         = config.SCREENER_SQUEEZE_EASY_RATIO
SQUEEZE_HARD_RATIO         = config.SCREENER_SQUEEZE_HARD_RATIO

VOLUME_SYMMETRY_THRESHOLD  = config.SCREENER_VOLUME_SYMMETRY_THRESHOLD

RECOVERY_EXCELLENT_SEC     = config.SCREENER_RECOVERY_EXCELLENT_SEC
RECOVERY_GOOD_SEC          = config.SCREENER_RECOVERY_GOOD_SEC
RECOVERY_POOR_SEC          = config.SCREENER_RECOVERY_POOR_SEC

ENTRY_BUFFER_PCT           = config.SCREENER_ENTRY_BUFFER_PCT

MTF_ENABLED                = config.SCREENER_MTF_ENABLED
MTF_TREND_RATIO            = config.SCREENER_MTF_TREND_RATIO

MIN_SCANS_FOR_FULL_SCORE   = config.SCREENER_MIN_SCANS_FOR_FULL_SCORE


# ═══════════════════════════════════════════════════════════
# ФЛАГИ
# ═══════════════════════════════════════════════════════════

FLAG_RANGE_DRIFT     = "RANGE_DRIFT"
FLAG_LONG_DRIFT      = "LONG_DRIFT"       # v3: долгосрочный дрейф
FLAG_BOT_OFFLINE     = "BOT_OFFLINE"
FLAG_NANO_TIMING     = "NANO_TIMING"
FLAG_HIGH_SELFTRADE  = "HIGH_SELFTRADE"
FLAG_AGGRESSOR_BOT   = "AGGRESSOR_BOT"
FLAG_NO_RANGE        = "NO_RANGE"
FLAG_RANGE_TOO_WIDE  = "RANGE_TOO_WIDE"
FLAG_NO_ALGO         = "NO_ALGO"
FLAG_SCAMMER         = "SCAMMER"
FLAG_UNSQUEEZABLE    = "UNSQUEEZABLE"
FLAG_VOLUME_TRAP     = "VOLUME_TRAP"
FLAG_NO_RECOVERY     = "NO_RECOVERY"      # v3: стенка не восстанавливается
FLAG_ALTERNATING     = "ALTERNATING"      # v3: чередующийся wash trading
FLAG_TREND_CONFLICT  = "TREND_CONFLICT"   # v4: рендж не подтверждён на старших TF
FLAG_DEAD_BOOK       = "DEAD_BOOK"        # v4.1: мёртвый стакан (HONEY/DEFI тип)
FLAG_HUNTER_ACTIVE   = "HUNTER_ACTIVE"    # v4.1: кто-то уже торгует (позитивный!)

FLAG_DESCRIPTIONS = {
    FLAG_RANGE_DRIFT:    "🔴 Рендж дрейфует вниз",
    FLAG_LONG_DRIFT:     "🔴 Долгосрочный дрейф",
    FLAG_BOT_OFFLINE:    "🔴 Робот отключается",
    FLAG_NANO_TIMING:    "🔴 Нано-тайминг",
    FLAG_HIGH_SELFTRADE: "🔴 Self-trade > 20%",
    FLAG_AGGRESSOR_BOT:  "🔴 Агрессор",
    FLAG_NO_RANGE:       "⚠️ Нет ренджа",
    FLAG_RANGE_TOO_WIDE: "⚠️ Рендж слишком широкий",
    FLAG_NO_ALGO:        "⚠️ Нет алгоритма",
    FLAG_SCAMMER:        "🔴 Жулик",
    FLAG_UNSQUEEZABLE:   "⚠️ Стенка непродавливаемая",
    FLAG_VOLUME_TRAP:    "⚠️ Односторонний объём",
    FLAG_NO_RECOVERY:    "⚠️ Стенка не восстанавливается",
    FLAG_ALTERNATING:    "🔴 Чередующийся wash",
    FLAG_TREND_CONFLICT: "⚠️ Тренд на старшем TF",
    FLAG_DEAD_BOOK:      "🔴 Мёртвый стакан",
    FLAG_HUNTER_ACTIVE:  "🟢 Торгуется трейдерами",
}


# ═══════════════════════════════════════════════════════════
# СТРУКТУРЫ ДАННЫХ v3
# ═══════════════════════════════════════════════════════════

@dataclass
class RangeInfo:
    lower_wall: Optional[WallInfo]
    upper_wall: Optional[WallInfo]
    lower_price: float
    upper_price: float
    mid_price: float
    width_pct: float
    is_valid: bool
    stability_score: float
    drift_detected: bool
    lower_history: list = field(default_factory=list)
    upper_history: list = field(default_factory=list)
    lower_cluster_size: float = 0
    upper_cluster_size: float = 0
    lower_walls_count: int = 0
    upper_walls_count: int = 0
    # v3: long-term drift
    long_drift_detected: bool = False
    long_drift_direction: str = ""    # "DOWN" / "UP" / ""
    long_drift_pct: float = 0.0       # % дрейфа за период
    # v4: multi-timeframe
    trend_alignment_score: float = 1.0  # 0-1: совпадение со свечным ренджем
    trend_conflict: bool = False        # True = тренд на старших TF

    @property
    def width_label(self):
        if self.width_pct < RANGE_IDEAL_MIN_PCT:
            return f"⚠️ {self.width_pct:.1f}% (узкий)"
        elif self.width_pct > RANGE_IDEAL_MAX_PCT:
            return f"⚠️ {self.width_pct:.1f}% (широкий)"
        return f"✅ {self.width_pct:.1f}%"

    @property
    def lower_label(self):
        return f"{self.lower_price:.8g}" if self.lower_price > 0 else "—"

    @property
    def upper_label(self):
        return f"{self.upper_price:.8g}" if self.upper_price > 0 else "—"


@dataclass
class SqueezeInfo:
    lower_wall_usdt: float
    upper_wall_usdt: float
    hourly_volume_usdt: float
    squeeze_ratio_lower: float
    squeeze_ratio_upper: float
    is_feasible: bool
    estimated_squeeze_time_min: float
    feasibility_label: str

    @property
    def label(self):
        return self.feasibility_label


@dataclass
class VolumeProfile:
    bid_depth_usdt: float
    ask_depth_usdt: float
    symmetry_ratio: float
    is_symmetric: bool
    dominant_side: str
    trap_risk: bool

    @property
    def label(self):
        if self.is_symmetric:
            return f"✅ Баланс {self.symmetry_ratio:.2f}"
        return f"⚠️ {self.dominant_side} ({self.symmetry_ratio:.2f})"


@dataclass
class RecoveryProfile:
    """v3: Профиль восстановления стенок"""
    has_data: bool
    total_disappearances: int = 0
    total_recovered: int = 0
    recovery_rate: float = 0.0      # 0-1
    avg_recovery_sec: float = 0.0
    median_recovery_sec: float = 0.0
    min_recovery_sec: float = 0.0
    max_recovery_sec: float = 0.0
    is_reliable: bool = False        # True = восстанавливается стабильно

    @property
    def label(self):
        if not self.has_data:
            return "— нет данных"
        if self.total_recovered == 0:
            if self.total_disappearances > 0:
                return f"🔴 Не восстанавливается ({self.total_disappearances} исчезн.)"
            return "— нет событий"
        rate_str = f"{self.recovery_rate*100:.0f}%"
        if self.avg_recovery_sec < RECOVERY_EXCELLENT_SEC:
            return f"✅ Быстро: {self.avg_recovery_sec:.0f}с (rate={rate_str})"
        if self.avg_recovery_sec < RECOVERY_GOOD_SEC:
            return f"🟡 Умеренно: {self.avg_recovery_sec:.0f}с (rate={rate_str})"
        return f"⚠️ Медленно: {self.avg_recovery_sec:.0f}с (rate={rate_str})"


@dataclass
class EntryZone:
    """v3: Рекомендуемая зона входа/выхода"""
    buy_zone_low: float     # Нижняя граница зоны покупки
    buy_zone_high: float    # Верхняя граница зоны покупки
    sell_zone_low: float    # Нижняя граница зоны продажи
    sell_zone_high: float   # Верхняя граница зоны продажи
    range_width_pct: float
    expected_profit_pct: float
    notes: str = ""

    @property
    def label(self):
        return f"BUY: {self.buy_zone_low:.8g}–{self.buy_zone_high:.8g} → SELL: {self.sell_zone_low:.8g}–{self.sell_zone_high:.8g} (~{self.expected_profit_pct:.1f}%)"


@dataclass
class RobotProfile:
    is_present: bool
    behavior_type: str
    retreat_pct: float
    advance_pct: float
    total_mover_events: int
    scans_present: int
    disappeared_after_squeeze: bool
    wall_persistence: float

    @property
    def behavior_emoji(self):
        return {"retreater": "🏃", "aggressor": "🦈",
                "mixed": "🔄", "passive": "🧱"}.get(self.behavior_type, "❓")

    @property
    def is_workable(self):
        return (self.is_present and
                self.behavior_type in ("retreater", "passive") and
                not self.disappeared_after_squeeze)


@dataclass
class TimingProfile:
    is_nano: bool
    avg_interval_sec: float
    cv: float
    pattern_type: str
    is_catchable: bool

    @property
    def label(self):
        if self.is_nano:
            return f"🔴 Нано ({self.avg_interval_sec:.2f}с, CV={self.cv:.2f})"
        if self.is_catchable:
            return f"✅ {self.avg_interval_sec:.1f}с (CV={self.cv:.2f})"
        return f"⚠️ {self.avg_interval_sec:.1f}с (CV={self.cv:.2f})"


@dataclass
class RobotScreenResult:
    symbol: str
    timestamp: float
    range_info: Optional[RangeInfo]
    robot_profile: Optional[RobotProfile]
    timing_profile: Optional[TimingProfile]
    squeeze_info: Optional[SqueezeInfo]
    volume_profile: Optional[VolumeProfile]
    recovery_profile: Optional[RecoveryProfile]     # v3
    entry_zone: Optional[EntryZone]                  # v3
    honesty_score: float
    selftrade_pct: float
    algo_score: float
    screener_score: float
    grade: str
    exclude_flags: list
    warning_flags: list
    is_workable: bool
    recommendation: str
    trades5m_exchange: int = 0

    @property
    def has_exclude_flags(self):
        return len(self.exclude_flags) > 0

    @property
    def flag_labels(self):
        return [FLAG_DESCRIPTIONS.get(f, f) for f in self.exclude_flags + self.warning_flags]

    @property
    def score_label(self):
        return f"{self.grade} ({self.screener_score:.0f})"

    @property
    def spread_pct(self):
        return self.range_info.width_pct if self.range_info else 0.0


# ═══════════════════════════════════════════════════════════
# RANGE DETECTION v3 (with long-term drift)
# ═══════════════════════════════════════════════════════════

def _cluster_boundary_price(walls, max_n=3):
    if not walls:
        return 0.0, 0.0, 0
    sorted_walls = sorted(walls, key=lambda w: w.size_usdt, reverse=True)[:max_n]
    total_size = sum(w.size_usdt for w in sorted_walls)
    if total_size <= 0:
        return 0.0, 0.0, 0
    vwap = sum(w.price * w.size_usdt for w in sorted_walls) / total_size
    return vwap, total_size, len(sorted_walls)


def detect_range(result: ScanResult, history: SymbolHistory, tracker: DensityTracker = None) -> RangeInfo:
    lower_price, lower_size, lower_count = _cluster_boundary_price(result.bid_walls, RANGE_CLUSTER_TOP_N)
    upper_price, upper_size, upper_count = _cluster_boundary_price(result.ask_walls, RANGE_CLUSTER_TOP_N)

    best_bid_wall = max(result.bid_walls, key=lambda w: w.size_usdt) if result.bid_walls else None
    best_ask_wall = max(result.ask_walls, key=lambda w: w.size_usdt) if result.ask_walls else None

    if lower_price <= 0:
        lower_price = result.best_bid
    if upper_price <= 0:
        upper_price = result.best_ask

    mid = result.mid_price
    if mid <= 0 or upper_price <= lower_price:
        return RangeInfo(
            lower_wall=None, upper_wall=None,
            lower_price=0, upper_price=0, mid_price=mid,
            width_pct=0, is_valid=False,
            stability_score=0, drift_detected=False)

    width_pct = (upper_price - lower_price) / lower_price * 100

    # Snapshot-based history
    lower_history = []
    upper_history = []
    for snap in list(history.snapshots)[-20:]:
        if snap.bid_walls:
            lp, _, _ = _cluster_boundary_price(snap.bid_walls, RANGE_CLUSTER_TOP_N)
            if lp > 0: lower_history.append(lp)
        if snap.ask_walls:
            up, _, _ = _cluster_boundary_price(snap.ask_walls, RANGE_CLUSTER_TOP_N)
            if up > 0: upper_history.append(up)

    # Short-term drift (как в v2)
    drift_detected = False
    stability_score = 0.0
    if len(lower_history) >= RANGE_STABILITY_MIN_SCANS:
        n = len(lower_history)
        drifts_down = 0
        for i in range(1, n):
            if lower_history[i-1] > 0:
                drop_pct = (lower_history[i-1] - lower_history[i]) / lower_history[i-1] * 100
                if drop_pct > RANGE_DRIFT_THRESHOLD_PCT:
                    drifts_down += 1
        drift_ratio = drifts_down / (n - 1) if n > 1 else 0
        if drift_ratio >= 0.5:
            drift_detected = True

        if len(lower_history) >= 2:
            lower_std = statistics.stdev(lower_history)
            lower_mean = statistics.mean(lower_history)
            lower_cv = lower_std / lower_mean if lower_mean > 0 else 1
            stability_score = max(0, 20 * (1 - lower_cv * 50))
            if len(upper_history) >= 2:
                upper_std = statistics.stdev(upper_history)
                upper_mean = statistics.mean(upper_history)
                upper_cv = upper_std / upper_mean if upper_mean > 0 else 1
                upper_stability = max(0, 20 * (1 - upper_cv * 50))
                stability_score = stability_score * 0.6 + upper_stability * 0.4
    elif len(lower_history) >= 1:
        stability_score = 6.0

    # v3: Long-term drift using range_history
    long_drift_detected = False
    long_drift_direction = ""
    long_drift_pct = 0.0

    if tracker:
        try:
            range_hist = tracker.get_range_history(result.symbol)
        except Exception:
            range_hist = []
        if len(range_hist) >= RANGE_LONG_DRIFT_MIN_POINTS:
            # Берём первую и последнюю треть, сравниваем средние нижней границы
            n = len(range_hist)
            third = max(n // 3, 1)
            first_third = [p.lower_price for p in range_hist[:third] if p.lower_price > 0]
            last_third = [p.lower_price for p in range_hist[-third:] if p.lower_price > 0]

            if first_third and last_third:
                avg_first = sum(first_third) / len(first_third)
                avg_last = sum(last_third) / len(last_third)
                if avg_first > 0:
                    drift = (avg_last - avg_first) / avg_first * 100
                    long_drift_pct = round(drift, 2)
                    if drift < -1.5:  # Нижняя граница опустилась > 1.5%
                        long_drift_detected = True
                        long_drift_direction = "DOWN"
                    elif drift > 3.0:  # Нижняя граница поднялась > 3% (необычно)
                        long_drift_detected = True
                        long_drift_direction = "UP"

    is_valid = (RANGE_MIN_WIDTH_PCT <= width_pct <= RANGE_MAX_WIDTH_PCT and
                (best_bid_wall is not None or lower_count > 0) and
                (best_ask_wall is not None or upper_count > 0))

    return RangeInfo(
        lower_wall=best_bid_wall, upper_wall=best_ask_wall,
        lower_price=lower_price, upper_price=upper_price,
        mid_price=mid, width_pct=round(width_pct, 2),
        is_valid=is_valid, stability_score=round(min(stability_score, 20), 1),
        drift_detected=drift_detected,
        lower_history=lower_history, upper_history=upper_history,
        lower_cluster_size=round(lower_size, 0), upper_cluster_size=round(upper_size, 0),
        lower_walls_count=lower_count, upper_walls_count=upper_count,
        long_drift_detected=long_drift_detected,
        long_drift_direction=long_drift_direction,
        long_drift_pct=long_drift_pct,
    )


# ═══════════════════════════════════════════════════════════
# SQUEEZE FEASIBILITY (from v2)
# ═══════════════════════════════════════════════════════════

def assess_squeeze_feasibility(range_info, result):
    hourly_vol = result.volume_24h_usdt / 24 if result.volume_24h_usdt > 0 else 0.001
    lower_wall_usdt = range_info.lower_cluster_size if range_info else 0
    upper_wall_usdt = range_info.upper_cluster_size if range_info else 0
    if lower_wall_usdt <= 0 and range_info and range_info.lower_wall:
        lower_wall_usdt = range_info.lower_wall.size_usdt
    if upper_wall_usdt <= 0 and range_info and range_info.upper_wall:
        upper_wall_usdt = range_info.upper_wall.size_usdt
    ratio_lower = lower_wall_usdt / hourly_vol if hourly_vol > 0 else 999
    ratio_upper = upper_wall_usdt / hourly_vol if hourly_vol > 0 else 999
    min_ratio = min(ratio_lower, ratio_upper)
    is_feasible = min_ratio <= SQUEEZE_HARD_RATIO
    est_time_min = min_ratio * 60
    if min_ratio <= SQUEEZE_EASY_RATIO:
        label = f"✅ Легко ({min_ratio:.1f}x)"
    elif min_ratio <= 5.0:
        label = f"🟡 Умеренно ({min_ratio:.1f}x, ~{est_time_min:.0f}м)"
    elif min_ratio <= SQUEEZE_HARD_RATIO:
        label = f"⚠️ Трудно ({min_ratio:.1f}x, ~{est_time_min/60:.1f}ч)"
    else:
        label = f"🔴 Нереально ({min_ratio:.0f}x)"
    return SqueezeInfo(
        lower_wall_usdt=round(lower_wall_usdt, 0), upper_wall_usdt=round(upper_wall_usdt, 0),
        hourly_volume_usdt=round(hourly_vol, 0),
        squeeze_ratio_lower=round(ratio_lower, 2), squeeze_ratio_upper=round(ratio_upper, 2),
        is_feasible=is_feasible, estimated_squeeze_time_min=round(est_time_min, 1),
        feasibility_label=label)


# ═══════════════════════════════════════════════════════════
# VOLUME PROFILE (from v2)
# ═══════════════════════════════════════════════════════════

def analyze_volume_profile(result):
    bid_depth = result.total_bid_depth_usdt
    ask_depth = result.total_ask_depth_usdt
    if bid_depth <= 0 and ask_depth <= 0:
        return VolumeProfile(0, 0, 0, False, "NONE", False)
    if ask_depth > 0:
        ratio = bid_depth / ask_depth
    elif bid_depth > 0:
        ratio = 99.0
    else:
        ratio = 1.0
    is_symmetric = VOLUME_SYMMETRY_THRESHOLD <= ratio <= (1.0 / VOLUME_SYMMETRY_THRESHOLD)
    if ratio > 2.0:
        dominant = "BID"; trap_risk = ratio > 3.0
    elif ratio < 0.5:
        dominant = "ASK"; trap_risk = ratio < 0.33
    else:
        dominant = "BALANCED"; trap_risk = False
    return VolumeProfile(round(bid_depth, 0), round(ask_depth, 0), round(ratio, 2),
                         is_symmetric, dominant, trap_risk)


# ═══════════════════════════════════════════════════════════
# RECOVERY PROFILE (v3 NEW)
# ═══════════════════════════════════════════════════════════

def build_recovery_profile(tracker: DensityTracker, symbol: str) -> RecoveryProfile:
    """Строит профиль восстановления стенок из данных tracker v5"""
    stats = tracker.get_recovery_stats(symbol)
    if not stats.get("has_data", False):
        return RecoveryProfile(has_data=False)

    avg = stats.get("avg_recovery_sec", 0)
    rate = stats.get("recovery_rate", 0)
    is_reliable = (rate >= 0.6 and avg < RECOVERY_GOOD_SEC and
                   stats.get("total_recovered", 0) >= 2)

    return RecoveryProfile(
        has_data=True,
        total_disappearances=stats.get("total_disappearances", 0),
        total_recovered=stats.get("total_recovered", 0),
        recovery_rate=rate,
        avg_recovery_sec=avg,
        median_recovery_sec=stats.get("median_recovery_sec", 0),
        min_recovery_sec=stats.get("min_recovery_sec", 0),
        max_recovery_sec=stats.get("max_recovery_sec", 0),
        is_reliable=is_reliable,
    )


# ═══════════════════════════════════════════════════════════
# ENTRY ZONE CALCULATOR (v3 NEW)
# ═══════════════════════════════════════════════════════════

def calculate_entry_zone(
    range_info: RangeInfo,
    robot_profile: Optional[RobotProfile],
    recovery_profile: Optional[RecoveryProfile],
) -> Optional[EntryZone]:
    """
    Вычисляет рекомендуемые зоны входа/выхода.
    
    Для retreater: покупать чуть выше нижней стенки (стенка отступит),
    продавать чуть ниже верхней стенки.
    
    Для passive: покупать на нижней стенке, продавать на верхней.
    """
    if not range_info or not range_info.is_valid:
        return None
    if range_info.width_pct < RANGE_MIN_WIDTH_PCT:
        return None

    lower = range_info.lower_price
    upper = range_info.upper_price
    if lower <= 0 or upper <= 0 or upper <= lower:
        return None

    # Базовый буфер от стенки
    buffer = lower * ENTRY_BUFFER_PCT / 100

    notes_parts = []

    if robot_profile and robot_profile.behavior_type == "retreater":
        # Retreater: стенка отступит, можем покупать ближе
        buy_low = lower + buffer * 0.5       # Агрессивный вход
        buy_high = lower + buffer * 2.0      # Консервативный
        sell_low = upper - buffer * 2.0      # Консервативный выход
        sell_high = upper - buffer * 0.5     # Агрессивный
        notes_parts.append("Retreater: стенка отступит при давлении")
    elif robot_profile and robot_profile.behavior_type == "passive":
        # Passive: стенка стоит, покупаем прямо у неё
        buy_low = lower + buffer
        buy_high = lower + buffer * 3
        sell_low = upper - buffer * 3
        sell_high = upper - buffer
        notes_parts.append("Passive MM: стенка стабильна")
    else:
        buy_low = lower + buffer
        buy_high = lower + buffer * 4
        sell_low = upper - buffer * 4
        sell_high = upper - buffer
        notes_parts.append("Осторожно: поведение робота неопределено")

    # Корректировка по recovery time
    if recovery_profile and recovery_profile.has_data and recovery_profile.total_recovered > 0:
        if recovery_profile.avg_recovery_sec < RECOVERY_EXCELLENT_SEC:
            notes_parts.append(f"Recovery {recovery_profile.avg_recovery_sec:.0f}с — быстро")
        elif recovery_profile.avg_recovery_sec > RECOVERY_POOR_SEC:
            notes_parts.append(f"Recovery {recovery_profile.avg_recovery_sec:.0f}с — МЕДЛЕННО, окно для входа")
            # Если recovery медленный, можем быть агрессивнее
            buy_low = lower - buffer  # Покупаем даже ниже стенки (она может не вернуться быстро)

    # Ожидаемая прибыль
    avg_buy = (buy_low + buy_high) / 2
    avg_sell = (sell_low + sell_high) / 2
    expected_profit = (avg_sell - avg_buy) / avg_buy * 100 if avg_buy > 0 else 0

    # Учитываем комиссию MEXC (~0.1% x 2 = 0.2%)
    net_profit = expected_profit - 0.2
    if net_profit < 0.5:
        notes_parts.append("⚠️ Прибыль после комиссий < 0.5%")

    return EntryZone(
        buy_zone_low=buy_low,
        buy_zone_high=buy_high,
        sell_zone_low=sell_low,
        sell_zone_high=sell_high,
        range_width_pct=range_info.width_pct,
        expected_profit_pct=round(net_profit, 2),
        notes="; ".join(notes_parts),
    )


# ═══════════════════════════════════════════════════════════
# ROBOT PROFILE (with improved bot_offline using wall_count_history)
# ═══════════════════════════════════════════════════════════

def build_robot_profile(symbol, algo_signal, history, current_result, tracker=None):
    is_present = (algo_signal is not None and
                  algo_signal.has_algo and
                  algo_signal.algo_score >= config.ALGO_SCORE_WEAK)

    behavior_type = "passive"
    retreat_pct = advance_pct = 0.0
    total_events = 0

    if algo_signal and algo_signal.wall_behavior:
        wb = algo_signal.wall_behavior
        behavior_type = wb.behavior_type
        retreat_pct = wb.retreat_pct
        advance_pct = wb.advance_pct
        total_events = wb.total_events

    scans_present = 0
    if history.tracked_walls:
        scans_present = max((tw.seen_count for tw in history.tracked_walls.values()), default=0)

    # v3: improved bot_offline using wall_count_history
    disappeared = _detect_bot_offline_v3(history, current_result, tracker)

    wall_persistence = 0.0
    if history.total_scans > 0 and scans_present > 0:
        wall_persistence = min(scans_present / history.total_scans, 1.0)

    return RobotProfile(
        is_present=is_present, behavior_type=behavior_type,
        retreat_pct=round(retreat_pct, 1), advance_pct=round(advance_pct, 1),
        total_mover_events=total_events, scans_present=scans_present,
        disappeared_after_squeeze=disappeared,
        wall_persistence=round(wall_persistence, 2))


def _detect_bot_offline_v3(history, current, tracker=None):
    """
    v3: Использует wall_count_history для точной детекции.
    Паттерн: активный период (много стенок + mover events) →
    тихий период (мало стенок, нет events) + движение цены.
    """
    wch = history.wall_count_history if hasattr(history, 'wall_count_history') else []
    if len(wch) < 5:
        return _detect_bot_offline_fallback(history, current)

    # Ищем переход active → silent
    recent = wch[-15:]
    range_width_pct = current.spread_pct if current.spread_pct > 0 else 2.0
    price_threshold = range_width_pct * 0.2

    active_count = 0
    silent_count = 0
    last_active_price = None

    for wc in recent:
        is_active = (wc.total_walls >= 2 and wc.mover_events_count > 0)
        if is_active:
            active_count += 1
            silent_count = 0
            last_active_price = wc.mid_price
        elif active_count >= 3:
            silent_count += 1

    if active_count >= 3 and silent_count >= 2 and last_active_price:
        price_shift = abs(current.mid_price - last_active_price) / last_active_price * 100
        return price_shift >= price_threshold

    return False


def _detect_bot_offline_fallback(history, current):
    """Fallback for when wall_count_history is not available"""
    snaps = list(history.snapshots)
    if len(snaps) < 4:
        return False
    range_width_pct = current.spread_pct if current.spread_pct > 0 else 2.0
    price_shift_threshold = range_width_pct * 0.2
    active_streak = silent_after = 0
    last_active_price = None
    price_shift = 0.0
    for snap in snaps[-15:]:
        if snap.mover_events:
            active_streak += 1
            silent_after = 0
            last_active_price = snap.mid_price
        elif active_streak >= 3:
            silent_after += 1
            if last_active_price and snap.mid_price > 0:
                price_shift = abs(snap.mid_price - last_active_price) / last_active_price * 100
    return active_streak >= 3 and silent_after >= 2 and price_shift >= price_shift_threshold


def build_timing_profile(algo_signal):
    if not algo_signal or not algo_signal.timing:
        return None
    tp = algo_signal.timing
    if tp.avg_interval_sec <= 0:
        return None
    cv = tp.std_interval_sec / tp.avg_interval_sec if tp.avg_interval_sec > 0 else 999
    is_nano = cv < TIMING_NANO_CV_MAX and tp.avg_interval_sec < 0.5
    is_catchable = not is_nano and tp.avg_interval_sec >= TIMING_OK_AVG_MIN_SEC
    return TimingProfile(is_nano=is_nano, avg_interval_sec=round(tp.avg_interval_sec, 2),
                         cv=round(cv, 3), pattern_type=tp.pattern_type, is_catchable=is_catchable)


# ═══════════════════════════════════════════════════════════
# MULTI-TIMEFRAME VALIDATION (v4 NEW)
# ═══════════════════════════════════════════════════════════

def validate_range_multi_timeframe(
    symbol: str,
    range_info: RangeInfo,
    client = None,
) -> tuple:
    """
    Проверяет рендж на старших таймфреймах (1h, 4h свечи).
    
    Если high-low диапазон на 1h > 2× ширина ренджа по стакану → тренд,
    значит стаканный рендж — временная консолидация внутри тренда.
    
    Возвращает (trend_alignment_score, trend_conflict).
    trend_alignment: 1.0 = идеальное совпадение, 0.0 = полное несоответствие.
    """
    if not MTF_ENABLED or not range_info or not range_info.is_valid or not client:
        return 1.0, False
    
    if range_info.width_pct <= 0:
        return 1.0, False
    
    try:
        # Загружаем 1h свечи
        klines_1h = client.get_klines(symbol, "60m", 24)
        if not klines_1h or not isinstance(klines_1h, list) or len(klines_1h) < 6:
            return 1.0, False
        
        # Считаем high-low за последние 24 свечи 1h
        highs = []
        lows = []
        closes = []
        for k in klines_1h:
            if isinstance(k, (list, tuple)) and len(k) >= 5:
                h = float(k[2]) if k[2] else 0
                l = float(k[3]) if k[3] else 0
                c = float(k[4]) if k[4] else 0
                if h > 0 and l > 0:
                    highs.append(h)
                    lows.append(l)
                    closes.append(c)
        
        if len(highs) < 6:
            return 1.0, False
        
        candle_high = max(highs)
        candle_low = min(lows)
        candle_range_pct = (candle_high - candle_low) / candle_low * 100 if candle_low > 0 else 0
        
        # Сравниваем с ренджем по стакану
        ratio = candle_range_pct / range_info.width_pct if range_info.width_pct > 0 else 999
        
        # Trend alignment: чем ближе ratio к 1.0, тем лучше
        if ratio <= 1.5:
            alignment = 1.0       # Свечной рендж ≈ стаканный → подтверждён
        elif ratio <= MTF_TREND_RATIO:
            alignment = 0.7       # Немного шире — нормально
        elif ratio <= 3.0:
            alignment = 0.4       # Значительно шире — подозрительно
        else:
            alignment = 0.1       # Гораздо шире — скорее всего тренд
        
        # Проверяем тренд по направлению (последние 6 закрытий)
        recent_closes = closes[-6:]
        if len(recent_closes) >= 4:
            ups = sum(1 for i in range(1, len(recent_closes)) if recent_closes[i] > recent_closes[i-1])
            downs = len(recent_closes) - 1 - ups
            trend_strength = max(ups, downs) / (len(recent_closes) - 1)
            if trend_strength >= 0.75:
                alignment *= 0.7  # Чёткий тренд → снижаем доверие к ренджу
        
        conflict = ratio > MTF_TREND_RATIO
        
        return round(alignment, 2), conflict
    
    except Exception:
        return 1.0, False


# ═══════════════════════════════════════════════════════════
# SCORING v4: А=20, Б=25, В=15, Г=15, Д=15, Е=10
# ═══════════════════════════════════════════════════════════

def compute_screener_score(
    range_info, robot_profile, timing_profile, squeeze_info,
    volume_profile, recovery_profile, honesty_score, selftrade_pct,
    alternating_score, data_scans,
):
    score = 0.0
    exclude_flags = []
    warning_flags = []

    # ── А: Рендж (0–20) ──
    range_score = 0.0
    if not range_info or not range_info.is_valid:
        warning_flags.append(FLAG_NO_RANGE)
    else:
        w = range_info.width_pct
        if RANGE_IDEAL_MIN_PCT <= w <= RANGE_IDEAL_MAX_PCT:
            range_score += 10
        elif RANGE_MIN_WIDTH_PCT <= w < RANGE_IDEAL_MIN_PCT:
            range_score += 5
        elif RANGE_IDEAL_MAX_PCT < w <= RANGE_MAX_WIDTH_PCT:
            range_score += 6
            warning_flags.append(FLAG_RANGE_TOO_WIDE)
        else:
            warning_flags.append(FLAG_RANGE_TOO_WIDE)

        range_score += min(range_info.stability_score * 0.5, 10)

        if range_info.drift_detected:
            exclude_flags.append(FLAG_RANGE_DRIFT)
            range_score *= 0.3
        if range_info.long_drift_detected and range_info.long_drift_direction == "DOWN":
            exclude_flags.append(FLAG_LONG_DRIFT)
            range_score *= 0.5
        # v4: Multi-timeframe conflict
        if range_info.trend_conflict:
            warning_flags.append(FLAG_TREND_CONFLICT)
            range_score *= 0.6  # Штраф, но не исключение

    score += min(range_score, 20)

    # ── Б: Робот (0–25) ──
    robot_score = 0.0
    if not robot_profile or not robot_profile.is_present:
        warning_flags.append(FLAG_NO_ALGO)
    else:
        bt = robot_profile.behavior_type
        if bt == "retreater":
            robot_score += 15
            if robot_profile.retreat_pct >= 75: robot_score += 3
        elif bt == "aggressor":
            exclude_flags.append(FLAG_AGGRESSOR_BOT)
            robot_score -= 10
        elif bt == "mixed":
            robot_score += 3
        elif bt == "passive":
            robot_score += 7

        sp = robot_profile.scans_present
        if sp >= ROBOT_MIN_SCANS_PRESENT:
            robot_score += {"retreater": 7, "passive": 4, "mixed": 2}.get(bt, 0)
        elif sp >= 2:
            robot_score += 2

        if robot_profile.disappeared_after_squeeze:
            exclude_flags.append(FLAG_BOT_OFFLINE)
            robot_score *= 0.2

    score += min(max(robot_score, 0), 25)

    # ── В: Честность (0–15) ──
    honesty_component = 0.0
    if selftrade_pct >= SELFTRADE_EXCLUDE_PCT:
        exclude_flags.append(FLAG_HIGH_SELFTRADE)
    elif selftrade_pct >= 10:
        warning_flags.append(FLAG_HIGH_SELFTRADE)
        honesty_component = 3
    else:
        honesty_component = min(honesty_score / 70 * 15, 15)

    if honesty_score < config.HONESTY_SCORE_SUSPECT and honesty_score > 0:
        exclude_flags.append(FLAG_SCAMMER)
        honesty_component = 0

    # v3: Alternating penalty
    if alternating_score >= 8:
        exclude_flags.append(FLAG_ALTERNATING)
        honesty_component = 0
    elif alternating_score >= 4:
        honesty_component *= 0.5

    score += honesty_component

    # ── Г: Тайминг (0–15) ──
    timing_score = 0.0
    if timing_profile:
        if timing_profile.is_nano:
            exclude_flags.append(FLAG_NANO_TIMING)
        elif timing_profile.is_catchable:
            timing_score = 15 if timing_profile.pattern_type in ("metronome", "second_locked") else 10
        else:
            timing_score = 4
    else:
        timing_score = 5
    score += min(timing_score, 15)

    # ── Д: Squeeze (0–15) ──
    squeeze_score = 0.0
    if squeeze_info:
        min_ratio = min(squeeze_info.squeeze_ratio_lower, squeeze_info.squeeze_ratio_upper)
        if min_ratio <= SQUEEZE_EASY_RATIO: squeeze_score = 15
        elif min_ratio <= 5.0: squeeze_score = 10
        elif min_ratio <= SQUEEZE_HARD_RATIO: squeeze_score = 5
        else:
            warning_flags.append(FLAG_UNSQUEEZABLE)
    else:
        squeeze_score = 5
    if volume_profile and volume_profile.trap_risk:
        warning_flags.append(FLAG_VOLUME_TRAP)
        squeeze_score *= 0.5
    score += min(squeeze_score, 15)

    # ── Е: Recovery (0–10) ──
    recovery_score = 0.0
    if recovery_profile and recovery_profile.has_data:
        if recovery_profile.total_recovered == 0 and recovery_profile.total_disappearances >= 2:
            warning_flags.append(FLAG_NO_RECOVERY)
            recovery_score = 0
        elif recovery_profile.is_reliable:
            if recovery_profile.avg_recovery_sec < RECOVERY_EXCELLENT_SEC:
                recovery_score = 10
            elif recovery_profile.avg_recovery_sec < RECOVERY_GOOD_SEC:
                recovery_score = 7
            else:
                recovery_score = 3
        elif recovery_profile.total_recovered > 0:
            recovery_score = 4
    else:
        recovery_score = 5  # Нет данных — нейтрально
    score += min(recovery_score, 10)

    # Дедупликация
    exclude_flags = list(dict.fromkeys(exclude_flags))
    warning_flags = [f for f in dict.fromkeys(warning_flags) if f not in exclude_flags]

    # Штраф за недостаток данных
    if data_scans < MIN_SCANS_FOR_FULL_SCORE:
        score *= (0.5 + 0.1 * data_scans)

    return round(min(max(score, 0), 100), 1), exclude_flags, warning_flags


def _grade(score, has_exclude):
    if has_exclude: return "F"
    if score >= 75: return "S"
    if score >= 60: return "A"
    if score >= 45: return "B"
    if score >= 30: return "C"
    if score >= 15: return "D"
    return "F"


def _recommendation(grade, robot_profile, range_info, squeeze_info, entry_zone, exclude_flags):
    if exclude_flags:
        flags_str = ", ".join(FLAG_DESCRIPTIONS.get(f, f) for f in exclude_flags[:2])
        return f"❌ Исключён: {flags_str}"
    if grade == "S":
        w = f"{range_info.width_pct:.1f}%" if range_info else "?"
        ez = ""
        if entry_zone and entry_zone.expected_profit_pct > 0:
            ez = f", ожидаемая прибыль ~{entry_zone.expected_profit_pct:.1f}%"
        return f"🔥 Топ выбор. Рендж {w}{ez}."
    if grade == "A":
        return "✅ Хороший кандидат. Наблюдай 2–3 цикла."
    if grade == "B":
        return "⚠️ Умеренный. Дополнительный мониторинг."
    if grade in ("C", "D"):
        return "🟡 Слабый. Много неопределённости."
    return "❌ Не рекомендуется."


# ═══════════════════════════════════════════════════════════
# MAIN SCREENER CLASS
# ═══════════════════════════════════════════════════════════

class RobotScreener:
    def __init__(self, tracker: DensityTracker, client=None, storage=None):
        self.tracker = tracker
        self.client = client       # MexcClientSync for MTF validation
        self.storage = storage     # ScannerDB for persistent storage
        self._last_results = []
        self._last_run_time = 0.0

    def evaluate_symbol(self, symbol, algo_signal=None, trades_buffer=None):
        history = self.tracker.get_symbol_history(symbol)
        if not history:
            return None
        snaps = list(getattr(history, 'snapshots', []))
        if not snaps:
            return None
        # total_scans может быть 0 если монета только появилась в топе,
        # но снапшот уже есть — обрабатываем
        current = snaps[-1]

        # v4.1: Book quality check (HONEY/DEFI/ZTC filter)
        book_quality = None
        if _HAS_BOOK_QUALITY and check_book_quality:
            try:
                # Приблизительный стакан из стенок: (price, qty)
                bids_raw = [(w.price, w.size_usdt / w.price if w.price > 0 else 0)
                            for w in (current.bid_walls or [])]
                asks_raw = [(w.price, w.size_usdt / w.price if w.price > 0 else 0)
                            for w in (current.ask_walls or [])]
                if bids_raw or asks_raw:
                    book_quality = check_book_quality(bids_raw, asks_raw, current.mid_price)
            except Exception:
                book_quality = None

        range_info = detect_range(current, history, self.tracker)
        robot_profile = build_robot_profile(symbol, algo_signal, history, current, self.tracker)
        timing_profile = build_timing_profile(algo_signal)
        squeeze_info = assess_squeeze_feasibility(range_info, current)
        volume_profile = analyze_volume_profile(current)
        recovery_profile = build_recovery_profile(self.tracker, symbol)
        entry_zone = calculate_entry_zone(range_info, robot_profile, recovery_profile)

        # v4: Multi-timeframe validation
        if MTF_ENABLED and self.client and range_info and range_info.is_valid:
            alignment, conflict = validate_range_multi_timeframe(
                symbol, range_info, self.client)
            range_info.trend_alignment_score = alignment
            range_info.trend_conflict = conflict

        # v4.1: Hunter pattern detection
        hunter = None
        if _HAS_HUNTER and detect_hunter_pattern and trades_buffer and range_info and range_info.is_valid:
            try:
                trades = trades_buffer.get_trades(symbol)
                if trades and len(trades) >= 30:
                    hunter = detect_hunter_pattern(
                        trades, range_info.lower_price, range_info.upper_price,
                        range_info.width_pct)
            except Exception:
                pass

        honesty_score = selftrade_pct = 0.0
        alternating_score = 0.0
        if algo_signal and algo_signal.honesty and algo_signal.honesty.total_checked >= config.HONESTY_MIN_TRADES:
            honesty_score = algo_signal.honesty.honesty_score
            selftrade_pct = algo_signal.honesty.selftrade_pct
            alternating_score = getattr(algo_signal.honesty, 'alternating_score', 0.0)

        algo_score = algo_signal.algo_score if algo_signal else 0.0

        score, exclude_flags, warning_flags = compute_screener_score(
            range_info, robot_profile, timing_profile, squeeze_info,
            volume_profile, recovery_profile, honesty_score, selftrade_pct,
            alternating_score, history.total_scans)

        # v4.1: Dead book exclusion
        if book_quality and hasattr(book_quality, 'is_dead') and book_quality.is_dead:
            exclude_flags.append(FLAG_DEAD_BOOK)

        # v4.1: Hunter bonus
        hunter_bonus = 0
        if hunter and hunter.found:
            hunter_bonus = min(hunter.score * 0.33, 10)  # max +10
            score += hunter_bonus
            if hunter.is_active:
                warning_flags.append(FLAG_HUNTER_ACTIVE)  # Позитивный флаг!

        score = round(min(max(score, 0), 100), 1)

        grade = _grade(score, bool([f for f in exclude_flags if f != FLAG_HUNTER_ACTIVE]))
        recommendation = _recommendation(grade, robot_profile, range_info,
                                         squeeze_info, entry_zone,
                                         [f for f in exclude_flags if f != FLAG_HUNTER_ACTIVE])

        is_workable = (grade in ("S", "A") and
                       not [f for f in exclude_flags if f != FLAG_HUNTER_ACTIVE] and
                       robot_profile is not None and robot_profile.is_workable)

        trades5m_exchange = estimate_trades_5m_exchange(self.client, symbol) if self.client else 0

        return RobotScreenResult(
            symbol=symbol, timestamp=time.time(),
            range_info=range_info, robot_profile=robot_profile,
            timing_profile=timing_profile, squeeze_info=squeeze_info,
            volume_profile=volume_profile, recovery_profile=recovery_profile,
            entry_zone=entry_zone,
            honesty_score=round(honesty_score, 1), selftrade_pct=round(selftrade_pct, 1),
            algo_score=round(algo_score, 1), screener_score=score, grade=grade,
            exclude_flags=exclude_flags, warning_flags=warning_flags,
            is_workable=is_workable, recommendation=recommendation,
            trades5m_exchange=int(trades5m_exchange or 0))

    def run(self, algo_signals=None, trades_buffer=None):
        if algo_signals is None:
            algo_signals = {}
        results = []
        _errors = []
        for symbol in self.tracker.histories.keys():
            sig = algo_signals.get(symbol)
            try:
                result = self.evaluate_symbol(symbol, sig, trades_buffer=trades_buffer)
                if result:
                    results.append(result)
            except Exception as _e:
                _errors.append(f"{symbol}: {type(_e).__name__}: {_e}")
        if _errors:
            import streamlit as _st
            try:
                _st.caption(f"⚠️ Ошибки скринера ({len(_errors)}): " + " | ".join(_errors[:3]))
            except Exception:
                pass
        results.sort(key=lambda r: (
            0 if r.is_workable else (1 if not r.has_exclude_flags else 2),
            -r.screener_score))
        self._last_results = results
        self._last_run_time = time.time()

        # v4: Persist to storage
        if self.storage and _HAS_STORAGE:
            try:
                self.storage.save_screener_results_batch(results)
            except Exception:
                pass

        return results

    def get_workable(self):
        return [r for r in self._last_results if r.is_workable]

    def get_stats(self):
        total = len(self._last_results)
        workable = len(self.get_workable())
        excluded = sum(1 for r in self._last_results if r.has_exclude_flags)
        return {"total": total, "workable": workable, "excluded": excluded,
                "pending": total - workable - excluded}


# ═══════════════════════════════════════════════════════════
# STREAMLIT RENDER v3
# ═══════════════════════════════════════════════════════════

def render_robot_screener_tab(tracker, algo_signals=None, client=None, storage=None):
    import streamlit as st
    import pandas as pd

    st.markdown("## 🎯 Скринер «Рабочий Ёрш» v4.1")
    st.markdown(
        "v4.1: Dead Book фильтр, Hunter Pattern, MTF, entry zones, recovery."
    )

    if not tracker or not tracker.histories:
        st.warning("⏳ Нет данных. Запусти сканирование во вкладке «Скан».")
        return

    trades_buffer = st.session_state.get("trades_buffer") if hasattr(st, 'session_state') else None

    # DB stats
    if storage and _HAS_STORAGE:
        try:
            db_stats = storage.get_db_stats()
            with st.expander("💾 Storage"):
                sc1, sc2, sc3, sc4 = st.columns(4)
                sc1.metric("Сканы", db_stats.get("scans", 0))
                sc2.metric("Алго", db_stats.get("algo_signals", 0))
                sc3.metric("Recovery", db_stats.get("recovery_events", 0))
                sc4.metric("БД", f"{db_stats.get('db_size_mb', 0)} MB")
        except Exception:
            pass

    screener = RobotScreener(tracker, client=client, storage=storage)
    with st.spinner("Анализирую монеты..."):
        results = screener.run(algo_signals or {}, trades_buffer=trades_buffer)

    if not results:
        st.warning("Нет данных для анализа.")
        return

    stats = screener.get_stats()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всего", stats["total"])
    c2.metric("🔥 Рабочих", stats["workable"])
    c3.metric("⚠️ Рассмотрение", stats["pending"])
    c4.metric("❌ Исключено", stats["excluded"])

    st.divider()

    cf1, cf2, cf3 = st.columns(3)
    with cf1:
        show_excluded = st.checkbox("Показать исключённые", value=False)
    with cf2:
        min_grade = st.selectbox("Мин. оценка", ["Все", "S", "A", "B", "C"], index=0)
    with cf3:
        sort_by = st.selectbox("Сортировка",
                               ["Скор", "Рендж", "Squeeze", "Recovery"], index=0)

    filtered = results
    if not show_excluded:
        filtered = [r for r in filtered if not r.has_exclude_flags]
    grade_order = {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4, "F": 5}
    if min_grade != "Все":
        filtered = [r for r in filtered if grade_order.get(r.grade, 5) <= grade_order[min_grade]]
    if sort_by == "Рендж":
        filtered.sort(key=lambda r: r.range_info.width_pct if r.range_info else 0)
    elif sort_by == "Squeeze":
        filtered.sort(key=lambda r: (r.squeeze_info.estimated_squeeze_time_min if r.squeeze_info else 9999))
    elif sort_by == "Recovery":
        filtered.sort(key=lambda r: (r.recovery_profile.avg_recovery_sec if r.recovery_profile and r.recovery_profile.has_data else 9999))

    if not filtered:
        st.info("Нет монет по фильтрам.")
        return

    rows = []
    for r in filtered:
        ri, rp, tp, si, vp, rec, ez = (
            r.range_info, r.robot_profile, r.timing_profile,
            r.squeeze_info, r.volume_profile, r.recovery_profile, r.entry_zone)
        rows.append({
            "Оценка": r.score_label,
            "Монета": r.symbol,
            "Рендж": ri.width_label if ri else "—",
            "Робот": f"{rp.behavior_emoji} {rp.behavior_type}" if rp else "—",
            "Поджим": f"{rp.retreat_pct:.0f}%" if rp else "—",
            "Сд5м": int(getattr(r, "trades5m_exchange", 0) or 0),
            "Squeeze": si.label if si else "—",
            "Recovery": rec.label if rec else "—",
            "Баланс": vp.label if vp else "—",
            "Тайминг": tp.label if tp else "—",
            "Прибыль": f"~{ez.expected_profit_pct:.1f}%" if ez else "—",
            "TF": f"{ri.trend_alignment_score:.0%}" if ri and ri.trend_alignment_score < 1.0 else "✅" if ri else "—",
            "Флаги": " | ".join(FLAG_DESCRIPTIONS.get(f, f)
                                for f in (r.exclude_flags + r.warning_flags)[:2]),
            "Вывод": r.recommendation[:50],
        })

    df = pd.DataFrame(rows)

    def _color_row(row):
        grade = row["Оценка"][0] if row["Оценка"] else "F"
        colors = {"S": "#0d3b1a", "A": "#1a3b0d", "B": "#1a2b0d",
                  "C": "#2b2b0d", "D": "#2b1a0d", "F": "#3b0d0d"}
        bg = colors.get(grade, "")
        return [f"background-color: {bg}" for _ in row]

    styled = df.style.apply(_color_row, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # Navigation buttons → перейти в стакан
    st.markdown("##### 🔍 Перейти в стакан")
    top_syms = [r.symbol for r in filtered[:12]]
    nc = min(8, len(top_syms))
    if nc > 0:
        nav_cols = st.columns(nc)
        for i, sym in enumerate(top_syms[:nc]):
            with nav_cols[i]:
                grade_sym = next((r.grade for r in filtered if r.symbol == sym), "?")
                if st.button(f"{sym}\n({grade_sym})", key=f"scr_go_{sym}", use_container_width=True):
                    st.session_state.detail_symbol = sym
                    st.session_state.current_page = 1
                    st.rerun()

    # Detail card
    st.divider()
    st.markdown("### 🔍 Детальный разбор")
    all_symbols = [r.symbol for r in filtered]
    workable_symbols = [r.symbol for r in filtered if r.is_workable]
    default_sym = workable_symbols[0] if workable_symbols else (all_symbols[0] if all_symbols else None)
    if not all_symbols:
        return
    selected = st.selectbox("Монета", all_symbols,
        index=all_symbols.index(default_sym) if default_sym and default_sym in all_symbols else 0)
    detail = next((r for r in filtered if r.symbol == selected), None)
    if detail:
        _render_detail_card_v3(detail)


def _render_detail_card_v3(r):
    import streamlit as st

    grade_colors = {"S": "🟢", "A": "🟢", "B": "🟡", "C": "🟡", "D": "🔴", "F": "🔴"}
    emoji = grade_colors.get(r.grade, "⚪")

    st.markdown(f"#### {emoji} {r.symbol} — **{r.grade}** ({r.screener_score:.0f}/100)")
    st.markdown(f"> {r.recommendation}")

    if r.exclude_flags:
        st.error("**Исключения:** " + " | ".join(FLAG_DESCRIPTIONS.get(f, f) for f in r.exclude_flags))
    if r.warning_flags:
        st.warning("**Предупреждения:** " + " | ".join(FLAG_DESCRIPTIONS.get(f, f) for f in r.warning_flags))

    # Entry zone (prominent if available)
    ez = r.entry_zone
    if ez and r.is_workable:
        st.success(f"**🎯 Зона входа:** {ez.label}")
        if ez.notes:
            st.caption(ez.notes)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**📐 Рендж**")
        ri = r.range_info
        if ri and ri.is_valid:
            st.markdown(f"- Ширина: {ri.width_label}")
            st.markdown(f"- Низ: `{ri.lower_label}` ({ri.lower_walls_count} стен, ${ri.lower_cluster_size:,.0f})")
            st.markdown(f"- Верх: `{ri.upper_label}` ({ri.upper_walls_count} стен, ${ri.upper_cluster_size:,.0f})")
            st.markdown(f"- Стабильность: {ri.stability_score:.0f}/20")
            if ri.drift_detected:
                st.markdown("- ⚠️ **Краткосрочный дрейф**")
            if ri.long_drift_detected:
                st.markdown(f"- 🔴 **Долгосрочный дрейф:** {ri.long_drift_direction} {ri.long_drift_pct:+.1f}%")
            if ri.trend_conflict:
                st.markdown(f"- ⚠️ **Тренд на старшем TF** (alignment={ri.trend_alignment_score:.0%})")
            elif ri.trend_alignment_score < 1.0:
                st.markdown(f"- Multi-TF alignment: {ri.trend_alignment_score:.0%}")
        else:
            st.markdown("❌ Нет чёткого ренджа")

    with col2:
        st.markdown("**🤖 Робот**")
        rp = r.robot_profile
        if rp:
            st.markdown(f"- {rp.behavior_emoji} **{rp.behavior_type}**")
            st.markdown(f"- Retreat: {rp.retreat_pct:.0f}% | Advance: {rp.advance_pct:.0f}%")
            st.markdown(f"- Живёт: {rp.scans_present} сканов")
            if rp.disappeared_after_squeeze:
                st.markdown("- ⚠️ **Отключался!**")

        st.markdown("**💪 Squeeze**")
        si = r.squeeze_info
        if si:
            st.markdown(f"- {si.label}")
            st.markdown(f"- Часовой объём: ${si.hourly_volume_usdt:,.0f}")

    with col3:
        st.markdown("**🔄 Recovery**")
        rec = r.recovery_profile
        if rec and rec.has_data:
            st.markdown(f"- {rec.label}")
            if rec.total_recovered > 0:
                st.markdown(f"- Avg: {rec.avg_recovery_sec:.0f}с, Med: {rec.median_recovery_sec:.0f}с")
                st.markdown(f"- Recovered: {rec.total_recovered}/{rec.total_disappearances}")
        else:
            st.markdown("— нет данных")

        st.markdown("**🔍 Честность**")
        st.markdown(f"- Self-trade: {r.selftrade_pct:.0f}%")
        st.markdown(f"- Score: {r.honesty_score:.0f}/70")

        st.markdown("**🕐 Тайминг**")
        tp = r.timing_profile
        if tp:
            st.markdown(f"- {tp.label}")
        else:
            st.markdown("— нет данных")

        st.markdown("**📈 Сделки 5м (биржа)**")
        st.markdown(f"- {int(getattr(r, 'trades5m_exchange', 0) or 0)}")

    with st.expander("📊 Детализация скора v3"):
        st.markdown(f"""
| Блок | Баллы | Макс |
|------|-------|------|
| А. Рендж | — | 20 |
| Б. Робот | — | 25 |
| В. Честность | — | 15 |
| Г. Тайминг | — | 15 |
| Д. Squeeze | — | 15 |
| Е. Recovery | — | 10 |
| **ИТОГО** | **{r.screener_score:.0f}** | **100** |
        """)
        st.caption("Точные баллы блоков рассчитываются внутри compute_screener_score().")
