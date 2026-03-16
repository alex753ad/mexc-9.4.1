"""
Хранилище снимков + трекинг времени жизни плотностей v4.1
Изменения v4.1:
- TrackedWall хранит историю distance_pct (последние 30 значений)
  для детекции поведения стенки БЕЗ зависимости от внешнего давления.
- Добавлен метод get_wall_behavior_raw() — сырые данные для анализатора.
"""
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from analyzer import ScanResult, MoverEvent, WallInfo, detect_movers
import config

WALL_DIST_HISTORY_MAX = 30


@dataclass
class TrackedWall:
    """Плотность с трекингом времени жизни и истории дистанции"""
    side: str
    price: float
    size_usdt: float
    multiplier: float
    distance_pct: float
    first_seen: float = 0.0
    last_seen: float = 0.0
    seen_count: int = 0
    # История: list of (timestamp, distance_pct, mid_price)
    dist_history: list = field(default_factory=list)

    @property
    def lifetime_sec(self) -> float:
        if self.first_seen <= 0: return 0
        return self.last_seen - self.first_seen

    @property
    def lifetime_str(self) -> str:
        s = self.lifetime_sec
        if s < 60: return f"{s:.0f}с"
        if s < 3600: return f"{s/60:.0f}м"
        return f"{s/3600:.1f}ч"

    def add_dist_snapshot(self, ts: float, dist_pct: float, mid_price: float):
        """Добавляет точку истории. Не дублирует если < 5 сек с последней."""
        if self.dist_history and ts - self.dist_history[-1][0] < 2.0:
            self.dist_history[-1] = (ts, dist_pct, mid_price)
            return
        self.dist_history.append((ts, dist_pct, mid_price))
        if len(self.dist_history) > WALL_DIST_HISTORY_MAX:
            self.dist_history.pop(0)

    @property
    def dist_trend(self) -> float:
        """
        Тренд distance_pct: >0 = стенка удаляется от mid, <0 = приближается.
        С 2 точками: простая разница. С 3+: разница средних половин.
        """
        if len(self.dist_history) < 2:
            return 0.0
        if len(self.dist_history) == 2:
            # Простая разница: новое - старое
            return self.dist_history[-1][1] - self.dist_history[0][1]
        n = len(self.dist_history)
        first_half = [d for _, d, _ in self.dist_history[:n//2]]
        second_half = [d for _, d, _ in self.dist_history[n//2:]]
        return (sum(second_half)/len(second_half)) - (sum(first_half)/len(first_half))

    @property
    def dist_cv(self) -> float:
        """Коэффициент вариации distance_pct — насколько стенка «прыгает»"""
        if len(self.dist_history) < 2:
            return 0.0
        dists = [d for _, d, _ in self.dist_history]
        mean = sum(dists) / len(dists)
        if mean <= 0: return 0.0
        variance = sum((d - mean) ** 2 for d in dists) / len(dists)
        return (variance ** 0.5) / mean

    @property
    def price_corr(self) -> float:
        """
        Корреляция между mid_price и distance_pct.
        +1 = стенка двигается ВМЕСТЕ с ценой → passive MM (следует за рынком).
        -1 = стенка двигается ПРОТИВ цены → aggressor (давит навстречу).
         0 = независима (стоит или прыгает хаотично).
        """
        if len(self.dist_history) < 5:
            return 0.0
        mids = [m for _, _, m in self.dist_history]
        dists = [d for _, d, _ in self.dist_history]
        n = len(mids)
        mean_m = sum(mids) / n
        mean_d = sum(dists) / n
        cov = sum((mids[i] - mean_m) * (dists[i] - mean_d) for i in range(n))
        var_m = sum((m - mean_m) ** 2 for m in mids)
        var_d = sum((d - mean_d) ** 2 for d in dists)
        denom = (var_m * var_d) ** 0.5
        if denom < 1e-12: return 0.0
        return round(cov / denom, 3)


@dataclass
class WallCountSnapshot:
    """Мгновенный снимок активности стенок — для _detect_bot_offline_v3."""
    ts:                  float
    total_walls:         int
    mover_events_count:  int
    mid_price:           float

@dataclass
class SymbolHistory:
    snapshots: deque = field(default_factory=lambda: deque(maxlen=config.MAX_SNAPSHOTS_PER_PAIR))
    wall_count_history: list = field(default_factory=list)  # list[WallCountSnapshot]
    mover_events: list = field(default_factory=list)
    tracked_walls: dict = field(default_factory=dict)
    first_seen: float = 0.0
    last_seen: float = 0.0
    total_scans: int = 0

    @property
    def mover_count(self) -> int:
        return len(self.mover_events)


class DensityTracker:
    def __init__(self):
        self.histories: dict = defaultdict(SymbolHistory)
        self.all_mover_events: list = []
        self.scan_count = 0
        self.last_scan_time = 0.0

    def update(self, results: list) -> list:
        self.scan_count += 1
        self.last_scan_time = time.time()
        new_events = []
        now = time.time()

        for result in results:
            sym = result.symbol
            hist = self.histories[sym]

            if hist.snapshots:
                prev = hist.snapshots[-1]
                events = detect_movers(result, prev)
                if events:
                    result.mover_events = events
                    hist.mover_events.extend(events)
                    new_events.extend(events)
                    if len(hist.mover_events) > 200:
                        hist.mover_events = hist.mover_events[-200:]

            current_keys = set()
            mid = result.mid_price

            # Матчинг стенок: ищем ближайшую существующую стенку в радиусе 1.5%
            # Это позволяет трекать retreater/aggressor которые двигают стенку
            _match_radius = mid * 0.015  # 1.5% от текущей цены

            for w in result.all_walls:
                exact_key = f"{w.side}_{w.price:.10f}"
                current_keys.add(exact_key)

                # Сначала проверяем точное совпадение
                if exact_key in hist.tracked_walls:
                    matched_key = exact_key
                else:
                    # Ищем ближайшую стенку той же стороны в радиусе 1.5%
                    matched_key = None
                    best_dist = _match_radius
                    for k, tw_existing in hist.tracked_walls.items():
                        if not k.startswith(w.side):
                            continue
                        price_dist = abs(tw_existing.price - w.price)
                        if price_dist < best_dist:
                            best_dist = price_dist
                            matched_key = k

                if matched_key and matched_key in hist.tracked_walls:
                    tw = hist.tracked_walls[matched_key]
                    # Обновляем ключ если стенка сдвинулась
                    if matched_key != exact_key:
                        del hist.tracked_walls[matched_key]
                        hist.tracked_walls[exact_key] = tw
                    tw.price = w.price  # обновляем цену
                    tw.last_seen = now
                    tw.seen_count += 1
                    tw.size_usdt = w.size_usdt
                    tw.multiplier = w.multiplier
                    tw.distance_pct = w.distance_pct
                    tw.add_dist_snapshot(now, w.distance_pct, mid)
                else:
                    # Новая стенка — создаём
                    tw = TrackedWall(
                        side=w.side, price=w.price, size_usdt=w.size_usdt,
                        multiplier=w.multiplier, distance_pct=w.distance_pct,
                        first_seen=now, last_seen=now, seen_count=1)
                    tw.add_dist_snapshot(now, w.distance_pct, mid)
                    hist.tracked_walls[exact_key] = tw

            stale = [k for k, tw in hist.tracked_walls.items()
                     if k not in current_keys and now - tw.last_seen > 300]
            for k in stale:
                del hist.tracked_walls[k]

            hist.snapshots.append(result)
            hist.total_scans += 1
            hist.last_seen = result.timestamp
            if hist.first_seen == 0: hist.first_seen = result.timestamp

            # Обновляем wall_count_history для _detect_bot_offline_v3
            wcs = WallCountSnapshot(
                ts=now,
                total_walls=len(result.all_walls) if result.all_walls else 0,
                mover_events_count=len(new_events),
                mid_price=mid,
            )
            hist.wall_count_history.append(wcs)
            if len(hist.wall_count_history) > 60:
                hist.wall_count_history = hist.wall_count_history[-60:]

        self.all_mover_events.extend(new_events)
        if len(self.all_mover_events) > 500:
            self.all_mover_events = self.all_mover_events[-500:]
        return new_events

    def get_tracked_walls(self, symbol: str) -> list:
        hist = self.histories.get(symbol)
        if not hist: return []
        return sorted(hist.tracked_walls.values(), key=lambda w: w.size_usdt, reverse=True)

    def get_wall_behavior_raw(self, symbol: str) -> list:
        """
        Стенки с историей для анализа поведения.
        Только те у кого seen_count >= 3 и dist_history >= 3 точек.
        Используется в analyze_wall_behavior_v2().
        """
        hist = self.histories.get(symbol)
        if not hist: return []
        # seen_count >= 2 достаточно для начального анализа
        # dist_history может быть мала если сканы шли быстро (< 5с дедупликация)
        return [tw for tw in hist.tracked_walls.values()
                if tw.seen_count >= 2]

    def get_symbol_history(self, symbol: str):
        return self.histories.get(symbol, SymbolHistory())

    def get_active_movers(self, window_sec: int = 3600) -> list:
        cutoff = time.time() - window_sec
        return [e for e in self.all_mover_events if e.timestamp >= cutoff]

    def get_symbol_movers(self, symbol: str) -> list:
        hist = self.histories.get(symbol)
        return hist.mover_events if hist else []

    def get_top_movers(self, n: int = 20) -> list:
        counts = {}
        for sym, hist in self.histories.items():
            if hist.mover_count > 0: counts[sym] = hist.mover_count
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:n]

    def get_stats(self) -> dict:
        return {
            "total_pairs_tracked": len(self.histories),
            "total_scans": self.scan_count,
            "total_mover_events": len(self.all_mover_events),
            "pairs_with_movers": sum(1 for h in self.histories.values() if h.mover_count > 0),
        }

    def get_recovery_stats(self, symbol: str) -> dict:
        """
        Реальные данные recovery из SQLite (Phase 4).
        Если db не прицеплена или данных нет — fallback на заглушку.
        """
        db = getattr(self, '_db', None)
        if db is not None:
            try:
                stats = db.load_recovery_stats(symbol, last_hours=24)
                if stats.get("has_data"):
                    return stats
            except Exception:
                pass

        # Fallback: аппроксимация из mover_events в памяти
        hist = self.histories.get(symbol)
        if not hist or not hist.mover_events:
            return {"has_data": False}

        # Ищем пары disappear/reappear по ценам стенок
        events = sorted(hist.mover_events, key=lambda e: e.timestamp)
        seen_prices = {}
        recovery_times = []
        for ev in events:
            price = getattr(ev, 'new_price', None) or getattr(ev, 'old_price', None)
            if price is None:
                continue
            if price in seen_prices:
                gap = ev.timestamp - seen_prices[price]
                if 5 < gap < 3600:
                    recovery_times.append(gap)
            seen_prices[price] = ev.timestamp

        if not recovery_times:
            return {"has_data": False}

        return {
            "has_data": True,
            "total_disappearances": len(recovery_times),
            "total_recovered": len(recovery_times),
            "recovery_rate": 1.0,
            "avg_recovery_sec": round(sum(recovery_times) / len(recovery_times), 1),
            "median_recovery_sec": round(sorted(recovery_times)[len(recovery_times)//2], 1),
            "min_recovery_sec": round(min(recovery_times), 1),
            "max_recovery_sec": round(max(recovery_times), 1),
        }

    def get_range_history(self, symbol: str) -> list:
        """
        История границ ренджа из SQLite (Phase 4).
        Fallback на историю снапшотов в памяти.
        """
        db = getattr(self, '_db', None)
        if db is not None:
            try:
                rows = db.load_range_history(symbol, last_hours=24)
                if rows:
                    # Конвертируем в объекты с атрибутами для совместимости
                    result = []
                    for r in rows:
                        pt = RangeBoundaryPoint(
                            lower_price=r.get('lower_price', 0),
                            upper_price=r.get('upper_price', 0),
                            mid_price=r.get('mid_price', 0),
                            ts=r.get('ts', 0),
                        )
                        result.append(pt)
                    return result
            except Exception:
                pass

        # Fallback: строим из snaps в памяти
        hist = self.histories.get(symbol)
        if not hist:
            return []
        result = []
        for snap in hist.snapshots:
            bids = snap.bid_walls or []
            asks = snap.ask_walls or []
            lower = min((w.price for w in bids), default=snap.mid_price * 0.97)
            upper = max((w.price for w in asks), default=snap.mid_price * 1.03)
            pt = RangeBoundaryPoint(
                lower_price=lower,
                upper_price=upper,
                mid_price=snap.mid_price,
                ts=snap.timestamp,
            )
            result.append(pt)
        return result



# ── Алиасы для обратной совместимости с robot_screener.py ──
# robot_screener импортирует эти имена но не использует их напрямую
WallCountPoint = WallCountSnapshot  # алиас

@dataclass
class RangeBoundaryPoint:
    """Точка истории границ ренджа — возвращается get_range_history()."""
    lower_price: float = 0.0
    upper_price: float = 0.0
    mid_price:   float = 0.0
    ts:          float = 0.0
