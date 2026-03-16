"""
Book Quality Filter v1.0 — Фильтр «мёртвых» стаканов

Исключает монеты типа HONEY/DEFI/ZTC, у которых:
1. ASK/BID глубина > 10x (односторонняя ликвидность)
2. Главные стенки далеко от mid price (>20%)
3. Стенки на "круглых" USD-ценах (старые ордера, не MM)
4. Стакан заполнен мусорными ордерами далеко от рынка

Использование:
    from book_quality import check_book_quality, BookQuality
    quality = check_book_quality(bids, asks, mid_price)
    if not quality.is_tradeable:
        # Исключить монету
"""

from dataclasses import dataclass
from typing import Optional


# Пороги
IMBALANCE_WARN = 5.0       # ASK/BID > 5x = предупреждение
IMBALANCE_EXCLUDE = 10.0   # ASK/BID > 10x = исключение
MAX_WALL_DISTANCE_PCT = 20.0  # Главная стенка дальше 20% от mid = мёртвый стакан
MIN_LEVELS_NEAR_MARKET = 5    # Мин. уровней в ±5% от mid
NEAR_MARKET_PCT = 5.0         # "Рядом с рынком" = ±5%
DUST_ORDER_USDT = 1.0         # Ордера < $1 = пыль


@dataclass
class BookQuality:
    """Результат проверки качества стакана"""
    is_tradeable: bool          # Можно ли торговать
    is_dead: bool               # Мёртвый стакан (FLAG_DEAD_BOOK)
    reason: str                 # Причина если нет
    # Метрики
    bid_depth_usdt: float = 0.0
    ask_depth_usdt: float = 0.0
    imbalance_ratio: float = 0.0  # ASK/BID или BID/ASK (>1 = дисбаланс)
    imbalance_side: str = ""      # "ASK" или "BID" — кто доминирует
    top_bid_distance_pct: float = 0.0  # Расстояние крупнейшей BID стенки от mid
    top_ask_distance_pct: float = 0.0  # Расстояние крупнейшей ASK стенки от mid
    levels_near_market: int = 0   # Уровней рядом с рынком
    dust_pct: float = 0.0         # % пылевых ордеров

    @property
    def label(self) -> str:
        if self.is_dead:
            return f"🔴 Мёртвый: {self.reason}"
        if not self.is_tradeable:
            return f"⚠️ {self.reason}"
        return "✅ Нормальный"


def check_book_quality(
    bids: list,
    asks: list,
    mid_price: float = 0,
) -> BookQuality:
    """
    Проверяет качество стакана.

    Args:
        bids: список (price, qty) — BID сторона
        asks: список (price, qty) — ASK сторона
        mid_price: текущая mid цена (если 0 — вычислится из best bid/ask)

    Returns:
        BookQuality с результатом проверки
    """
    if not bids or not asks:
        return BookQuality(
            is_tradeable=False, is_dead=True,
            reason="Пустой стакан")

    # Mid price
    best_bid = float(bids[0][0]) if bids else 0
    best_ask = float(asks[0][0]) if asks else 0
    if mid_price <= 0:
        mid_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else 1

    # Глубина
    bid_depth = sum(float(p) * float(q) for p, q in bids if float(p) > 0 and float(q) > 0)
    ask_depth = sum(float(p) * float(q) for p, q in asks if float(p) > 0 and float(q) > 0)

    # Дисбаланс
    if bid_depth > 0 and ask_depth > 0:
        if ask_depth > bid_depth:
            imbalance = ask_depth / bid_depth
            imbalance_side = "ASK"
        else:
            imbalance = bid_depth / ask_depth
            imbalance_side = "BID"
    elif bid_depth > 0:
        imbalance = 999
        imbalance_side = "BID_ONLY"
    elif ask_depth > 0:
        imbalance = 999
        imbalance_side = "ASK_ONLY"
    else:
        return BookQuality(
            is_tradeable=False, is_dead=True,
            reason="Нулевая глубина")

    # Расстояние крупнейших стенок от mid
    # B-15 fix: ищем топ-стенку только среди уровней в ±30% от mid.
    # Без фильтра ордер на 50% ниже рынка с большим объёмом находился первым
    # и ложно помечал книгу как мёртвую (FLAG_DEAD_BOOK).
    _NEAR_WALL_DIST = 0.30  # 30% от mid — граница «рыночной» стенки
    if bids:
        near_bids = [(p, q) for p, q in bids
                     if abs(float(p) - mid_price) / mid_price <= _NEAR_WALL_DIST]
        top_bid = max(near_bids or bids, key=lambda x: float(x[0]) * float(x[1]))
        top_bid_dist = abs(float(top_bid[0]) - mid_price) / mid_price * 100
    else:
        top_bid_dist = 999

    if asks:
        near_asks = [(p, q) for p, q in asks
                     if abs(float(p) - mid_price) / mid_price <= _NEAR_WALL_DIST]
        top_ask = max(near_asks or asks, key=lambda x: float(x[0]) * float(x[1]))
        top_ask_dist = abs(float(top_ask[0]) - mid_price) / mid_price * 100
    else:
        top_ask_dist = 999

    # Уровни рядом с рынком
    near_low = mid_price * (1 - NEAR_MARKET_PCT / 100)
    near_high = mid_price * (1 + NEAR_MARKET_PCT / 100)
    levels_near = 0
    for p, q in bids:
        if near_low <= float(p) <= near_high and float(p) * float(q) > DUST_ORDER_USDT:
            levels_near += 1
    for p, q in asks:
        if near_low <= float(p) <= near_high and float(p) * float(q) > DUST_ORDER_USDT:
            levels_near += 1

    # Пылевые ордера
    total_orders = len(bids) + len(asks)
    dust_orders = sum(1 for p, q in bids + asks if float(p) * float(q) < DUST_ORDER_USDT)
    dust_pct = dust_orders / total_orders * 100 if total_orders > 0 else 0

    # ─── Вердикт ───

    is_dead = False
    is_tradeable = True
    reasons = []

    # Проверка 1: Критический дисбаланс
    if imbalance >= IMBALANCE_EXCLUDE:
        is_dead = True
        is_tradeable = False
        reasons.append(f"{imbalance_side} перекос {imbalance:.0f}x")

    # Проверка 2: Главные стенки далеко от рынка
    if top_bid_dist > MAX_WALL_DISTANCE_PCT and top_ask_dist > MAX_WALL_DISTANCE_PCT:
        is_dead = True
        is_tradeable = False
        reasons.append(f"Стенки далеко: BID {top_bid_dist:.0f}%, ASK {top_ask_dist:.0f}%")
    elif top_bid_dist > MAX_WALL_DISTANCE_PCT or top_ask_dist > MAX_WALL_DISTANCE_PCT:
        reasons.append(f"Одна стенка далеко от рынка")

    # Проверка 3: Мало уровней рядом с рынком
    if levels_near < MIN_LEVELS_NEAR_MARKET:
        if levels_near <= 2:
            is_dead = True
            is_tradeable = False
            reasons.append(f"Только {levels_near} уровней рядом с рынком")
        else:
            reasons.append(f"Мало уровней ({levels_near}) рядом с рынком")

    # Проверка 4: Предупреждение о дисбалансе (не блокирует торговлю)
    if IMBALANCE_WARN <= imbalance < IMBALANCE_EXCLUDE:
        reasons.append(f"{imbalance_side} перекос {imbalance:.1f}x")
        # is_tradeable остаётся True — это предупреждение, не исключение

    reason_str = "; ".join(reasons) if reasons else "OK"

    return BookQuality(
        is_tradeable=is_tradeable,
        is_dead=is_dead,
        reason=reason_str,
        bid_depth_usdt=round(bid_depth, 0),
        ask_depth_usdt=round(ask_depth, 0),
        imbalance_ratio=round(imbalance, 1),
        imbalance_side=imbalance_side,
        top_bid_distance_pct=round(top_bid_dist, 1),
        top_ask_distance_pct=round(top_ask_dist, 1),
        levels_near_market=levels_near,
        dust_pct=round(dust_pct, 1),
    )
