"""
Анализатор плотностей + детектор переставляшей + детектор лесенок
"""
import statistics
import time
from dataclasses import dataclass, field
from typing import Optional
import config


# ═══════════════════════════════════════════════════
# Структуры данных
# ═══════════════════════════════════════════════════

@dataclass
class WallInfo:
    """Информация о плотности (стенке)"""
    side: str
    price: float
    size_usdt: float
    multiplier: float
    distance_pct: float
    levels_count: int = 1


@dataclass
class MoverEvent:
    """Событие переставляша — плотность переместилась"""
    symbol: str
    side: str
    old_price: float
    new_price: float
    size_usdt: float
    shift_pct: float
    timestamp: float
    direction: str  # "UP" или "DOWN"


@dataclass
class LadderPattern:
    """
    Лесенка = 3+ лимиток подряд с одинаковым шагом цены
    и похожим объёмом на одной стороне стакана.
    Признак алгоритмической расстановки.
    """
    side: str                   # BID / ASK
    steps: int                  # кол-во ступеней (3+)
    price_step: float           # абсолютный шаг цены
    price_step_pct: float       # шаг в % от mid_price
    avg_size_usdt: float        # средний объём ступени
    total_usdt: float           # суммарный объём всей лесенки
    start_price: float          # начальная цена (ближе к mid)
    end_price: float            # конечная цена (дальше от mid)
    size_std_pct: float         # разброс объёмов между ступенями (%)
    distance_from_mid_pct: float  # расстояние первой ступени от mid

    @property
    def label(self) -> str:
        return (f"{self.side} {self.steps} ступеней, "
                f"шаг {self.price_step_pct:.2f}%, "
                f"${self.avg_size_usdt:.0f}/ступ")

    @property
    def is_strong(self) -> bool:
        """Сильная лесенка: ≥4 ступени, близко к mid, ровные объёмы"""
        return (self.steps >= 4 and
                self.distance_from_mid_pct < 5.0 and
                self.size_std_pct < 25)


@dataclass
class ScanResult:
    """Результат сканирования одной пары"""
    symbol: str
    mid_price: float
    best_bid: float
    best_ask: float
    spread_pct: float
    volume_24h_usdt: float
    bid_walls: list[WallInfo] = field(default_factory=list)
    ask_walls: list[WallInfo] = field(default_factory=list)
    total_bid_depth_usdt: float = 0.0
    total_ask_depth_usdt: float = 0.0
    score: float = 0.0
    mover_events: list[MoverEvent] = field(default_factory=list)
    timestamp: float = 0.0
    trade_count_24h: int = 0
    # Новое: лесенки
    ladders: list[LadderPattern] = field(default_factory=list)

    @property
    def all_walls(self) -> list[WallInfo]:
        return self.bid_walls + self.ask_walls

    @property
    def biggest_wall(self) -> Optional[WallInfo]:
        walls = self.all_walls
        return max(walls, key=lambda w: w.size_usdt) if walls else None

    @property
    def wall_count(self) -> int:
        return len(self.all_walls)

    @property
    def has_movers(self) -> bool:
        return len(self.mover_events) > 0

    @property
    def has_ladders(self) -> bool:
        return len(self.ladders) > 0

    @property
    def best_ladder(self) -> Optional[LadderPattern]:
        if not self.ladders:
            return None
        return max(self.ladders, key=lambda l: l.steps)


def _safe_float(val, default=0.0) -> float:
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════
# Анализ стакана
# ═══════════════════════════════════════════════════

def analyze_order_book(
    symbol: str,
    order_book: dict,
    ticker_data: dict,
) -> Optional[ScanResult]:
    """Анализирует стакан на наличие плотностей и лесенок"""
    bids_raw = order_book.get("bids", [])
    asks_raw = order_book.get("asks", [])

    if not bids_raw or not asks_raw:
        return None

    try:
        bids = [(_safe_float(b[0]), _safe_float(b[1])) for b in bids_raw]
        asks = [(_safe_float(a[0]), _safe_float(a[1])) for a in asks_raw]
    except (IndexError, KeyError):
        return None

    bids = [(p, q) for p, q in bids if p > 0]
    asks = [(p, q) for p, q in asks if p > 0]

    if not bids or not asks:
        return None

    best_bid_price = bids[0][0]
    best_ask_price = asks[0][0]
    mid_price = (best_bid_price + best_ask_price) / 2

    if mid_price <= 0:
        return None

    spread_pct = (best_ask_price - best_bid_price) / best_bid_price * 100
    volume_24h = _safe_float(ticker_data.get("quoteVolume", 0))

    bid_levels_usdt = [(p, q * p) for p, q in bids]
    ask_levels_usdt = [(p, q * p) for p, q in asks]

    total_bid_depth = sum(u for _, u in bid_levels_usdt)
    total_ask_depth = sum(u for _, u in ask_levels_usdt)

    all_sizes = [u for _, u in bid_levels_usdt + ask_levels_usdt if u > 0]
    if len(all_sizes) < 5:
        return None

    median_size = statistics.median(all_sizes)
    if median_size <= 0:
        median_size = 1.0

    bid_walls = _find_walls(bid_levels_usdt, "BID", mid_price, median_size)
    ask_walls = _find_walls(ask_levels_usdt, "ASK", mid_price, median_size)

    if not bid_walls and not ask_walls:
        return None

    # Детекция лесенок
    ladders = detect_ladders(bids, asks, mid_price)

    result = ScanResult(
        symbol=symbol,
        mid_price=mid_price,
        best_bid=best_bid_price,
        best_ask=best_ask_price,
        spread_pct=spread_pct,
        volume_24h_usdt=volume_24h,
        bid_walls=bid_walls,
        ask_walls=ask_walls,
        total_bid_depth_usdt=total_bid_depth,
        total_ask_depth_usdt=total_ask_depth,
        timestamp=time.time(),
        ladders=ladders,
    )
    result.score = _calculate_score(result)
    return result


def _find_walls(levels, side, mid_price, median_size) -> list[WallInfo]:
    walls = []
    for price, size_usdt in levels:
        if size_usdt < config.MIN_WALL_SIZE_USDT:
            continue
        multiplier = size_usdt / median_size
        if multiplier < config.WALL_MULTIPLIER:
            continue
        distance_pct = abs(price - mid_price) / mid_price * 100
        if distance_pct > config.MAX_WALL_DISTANCE_PCT:
            continue
        walls.append(WallInfo(
            side=side, price=price, size_usdt=size_usdt,
            multiplier=round(multiplier, 1),
            distance_pct=round(distance_pct, 2),
        ))

    walls = _merge_adjacent_walls(walls, [p for p, _ in levels])
    walls.sort(key=lambda w: w.size_usdt, reverse=True)
    return walls[:5]


def _merge_adjacent_walls(walls, prices) -> list[WallInfo]:
    if len(walls) <= 1:
        return walls
    merged = []
    used = set()
    for i, wall in enumerate(walls):
        if i in used:
            continue
        cluster = [wall]
        used.add(i)
        try:
            idx = prices.index(wall.price)
        except ValueError:
            merged.append(wall)
            continue
        for j, other in enumerate(walls):
            if j in used:
                continue
            try:
                oidx = prices.index(other.price)
            except ValueError:
                continue
            if abs(idx - oidx) <= 3:
                cluster.append(other)
                used.add(j)
        if len(cluster) > 1:
            total = sum(w.size_usdt for w in cluster)
            avg_m = sum(w.multiplier for w in cluster) / len(cluster)
            center = min(cluster, key=lambda w: w.distance_pct)
            merged.append(WallInfo(
                side=center.side, price=center.price,
                size_usdt=total, multiplier=round(avg_m, 1),
                distance_pct=center.distance_pct,
                levels_count=len(cluster),
            ))
        else:
            merged.append(wall)
    return merged


def _calculate_score(result: ScanResult) -> float:
    import math
    score = 0.0
    biggest = result.biggest_wall

    # А. Размер стенки (0-30 очков)
    if biggest:
        score += min(biggest.size_usdt / 50, 30)

    # Б. Спред — ШТРАФ за широкий (>2%), бонус за tight (0.3-2%)
    # Было: бонус до +25 за любой спред. Теперь: оптимум 0.5-1.5%
    sp = result.spread_pct
    if sp <= 0.3:
        score += 5    # слишком tight — маленький диапазон для squeeze
    elif sp <= 2.0:
        score += 5 + (sp - 0.3) / 1.7 * 20   # 5-25 очков в диапазоне 0.3-2%
    else:
        score += max(25 - (sp - 2.0) * 10, 0)  # штраф: -10 очков за каждый % >2%

    # В. Дистанция стенки (0-20 очков)
    if biggest:
        score += max(20 - biggest.distance_pct * 2, 0)

    # Г. Двусторонний стакан (15 очков)
    if result.bid_walls and result.ask_walls:
        score += 15

    # Д. Реальная торговая активность (0-15 очков)
    # Логарифмическая шкала: 50 сделок=5, 500=10, 5000=13, 50000=15
    tc = getattr(result, 'trade_count_24h', 0)
    if tc > 0:
        score += min(math.log10(tc + 1) * 4, 15)

    # Е. Объём 24ч (0-5 очков) — снижен, т.к. wash-trade накручивает объём
    if result.volume_24h_usdt > 0:
        score += min(result.volume_24h_usdt / 50000, 5)

    # Ж. Активность стенок — переставления (0-15 очков)
    mover_count = len(result.mover_events) if hasattr(result, 'mover_events') and result.mover_events else 0
    if result.has_movers:
        score += min(5 + mover_count * 1.5, 15)

    # З. Лесенки (0-8 очков)
    if result.has_ladders:
        best = result.best_ladder
        if best and best.is_strong:
            score += 8
        else:
            score += 4

    return round(score, 1)


# ═══════════════════════════════════════════════════
# Детектор переставляшей (Фаза 2)
# ═══════════════════════════════════════════════════

def detect_movers(
    current: ScanResult,
    previous: ScanResult,
) -> list[MoverEvent]:
    events = []
    tolerance = config.MOVER_SIZE_TOLERANCE
    events += _compare_walls(
        current.symbol, prev_walls=previous.bid_walls,
        curr_walls=current.bid_walls, side="BID",
        mid_price=current.mid_price, tolerance=tolerance,
        timestamp=current.timestamp)
    events += _compare_walls(
        current.symbol, prev_walls=previous.ask_walls,
        curr_walls=current.ask_walls, side="ASK",
        mid_price=current.mid_price, tolerance=tolerance,
        timestamp=current.timestamp)
    return events


def _compare_walls(
    symbol, prev_walls, curr_walls, side, mid_price, tolerance, timestamp
) -> list[MoverEvent]:
    events = []
    curr_prices = {w.price for w in curr_walls}
    prev_prices = {w.price for w in prev_walls}
    disappeared = [w for w in prev_walls if w.price not in curr_prices]
    appeared = [w for w in curr_walls if w.price not in prev_prices]

    for old_w in disappeared:
        for new_w in appeared:
            size_diff = abs(old_w.size_usdt - new_w.size_usdt) / max(old_w.size_usdt, 1)
            if size_diff > tolerance:
                continue
            price_shift = (new_w.price - old_w.price) / old_w.price * 100
            if abs(price_shift) < config.MOVER_MIN_PRICE_SHIFT:
                continue
            events.append(MoverEvent(
                symbol=symbol, side=side, old_price=old_w.price,
                new_price=new_w.price, size_usdt=new_w.size_usdt,
                shift_pct=round(price_shift, 3), timestamp=timestamp,
                direction="UP" if price_shift > 0 else "DOWN"))
            break
    return events


# ═══════════════════════════════════════════════════
# Детектор лесенок (Этап 3)
# ═══════════════════════════════════════════════════

def detect_ladders(
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    mid_price: float,
) -> list[LadderPattern]:
    """
    Ищет «лесенки» — серии из 3+ лимиток подряд с:
    - Одинаковым шагом цены (±tolerance)
    - Похожим объёмом (±tolerance)
    - На одной стороне стакана

    Пример (POWERAIUSDT):
      BID 0.00120  $50
      BID 0.00119  $48  ← шаг 0.00001
      BID 0.00118  $51  ← шаг 0.00001
      BID 0.00117  $49  ← шаг 0.00001
      = ЛЕСЕНКА из 4 ступеней
    """
    ladders = []

    # Анализируем BID и ASK отдельно
    bid_ladders = _find_ladders_side(bids, "BID", mid_price)
    ask_ladders = _find_ladders_side(asks, "ASK", mid_price)

    ladders.extend(bid_ladders)
    ladders.extend(ask_ladders)

    # Сортируем: больше ступеней = лучше
    ladders.sort(key=lambda l: l.steps, reverse=True)
    return ladders


def _find_ladders_side(
    levels: list[tuple[float, float]],
    side: str,
    mid_price: float,
) -> list[LadderPattern]:
    """Ищет лесенки на одной стороне стакана"""
    min_steps = config.LADDER_MIN_STEPS
    step_tol = config.LADDER_STEP_TOLERANCE
    size_tol = config.LADDER_SIZE_TOLERANCE
    max_dist = config.LADDER_MAX_DISTANCE_PCT
    min_size = config.LADDER_MIN_SIZE_USDT

    if len(levels) < min_steps:
        return []

    # Превращаем в (price, usdt_volume) — уже отсортировано API
    steps_data = []
    for price, qty in levels:
        if price <= 0 or qty <= 0:
            continue
        usdt = price * qty
        dist = abs(price - mid_price) / mid_price * 100
        if dist > max_dist:
            continue
        if usdt < min_size:
            continue
        steps_data.append((price, qty, usdt, dist))

    if len(steps_data) < min_steps:
        return []

    ladders = []
    n = len(steps_data)
    used_ranges = []  # Чтобы не дублировать перекрывающиеся лесенки

    for start_i in range(n - min_steps + 1):
        # Пропускаем, если начало уже использовано в другой лесенке
        if any(start_i >= a and start_i <= b for a, b in used_ranges):
            continue

        p0, q0, u0, d0 = steps_data[start_i]
        p1, q1, u1, d1 = steps_data[start_i + 1]

        # Вычисляем шаг между первыми двумя уровнями
        base_step = abs(p1 - p0)
        if base_step <= 0:
            continue

        # Проверяем объёмы первых двух
        if u0 <= 0:
            continue
        size_diff_01 = abs(u1 - u0) / u0
        if size_diff_01 > size_tol:
            continue

        # Расширяем лесенку вперёд
        chain = [(p0, q0, u0, d0), (p1, q1, u1, d1)]
        avg_usdt = (u0 + u1) / 2

        for j in range(start_i + 2, n):
            pj, qj, uj, dj = steps_data[j]
            prev_p = chain[-1][0]

            # Проверяем шаг
            actual_step = abs(pj - prev_p)
            if base_step > 0:
                step_ratio = abs(actual_step - base_step) / base_step
            else:
                break
            if step_ratio > step_tol:
                break

            # Проверяем объём
            if avg_usdt > 0:
                vol_ratio = abs(uj - avg_usdt) / avg_usdt
            else:
                break
            if vol_ratio > size_tol:
                break

            chain.append((pj, qj, uj, dj))
            # Обновляем среднее для сравнения
            avg_usdt = sum(c[2] for c in chain) / len(chain)

        if len(chain) >= min_steps:
            sizes = [c[2] for c in chain]
            mean_size = sum(sizes) / len(sizes)
            if len(sizes) > 1:
                import statistics as _st
                std_size = _st.stdev(sizes)
                size_std_pct = (std_size / mean_size * 100) if mean_size > 0 else 0
            else:
                size_std_pct = 0.0

            step_pct = base_step / mid_price * 100 if mid_price > 0 else 0

            # Определяем start/end
            prices_chain = [c[0] for c in chain]
            if side == "BID":
                start_p = max(prices_chain)  # Ближе к mid
                end_p = min(prices_chain)
            else:
                start_p = min(prices_chain)  # Ближе к mid
                end_p = max(prices_chain)

            ladder = LadderPattern(
                side=side,
                steps=len(chain),
                price_step=round(base_step, 10),
                price_step_pct=round(step_pct, 4),
                avg_size_usdt=round(mean_size, 2),
                total_usdt=round(sum(sizes), 2),
                start_price=start_p,
                end_price=end_p,
                size_std_pct=round(size_std_pct, 1),
                distance_from_mid_pct=round(chain[0][3], 2),
            )
            ladders.append(ladder)
            used_ranges.append((start_i, start_i + len(chain) - 1))

    return ladders
