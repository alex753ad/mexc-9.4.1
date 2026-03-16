"""
Algo Detector v3.0 — Поиск алгоритмов (ёршиков) в ленте сделок.

v3 изменения:
- HonestyCheck v2: НЕ зависит от текущего BBO (решает проблему рассинхрона
  стакана и ленты). Анализирует только внутренние сигналы сделок:
  1) Side consistency (соотношение buy/sell)
  2) Price level clustering (кластеризация цен → лимитки vs рынок)
  3) Self-trade detection (buy+sell одна цена за 1.5 сек)

- WallBehavior: классификация поведения стенок (убегает / давит / стоит).
  Использует историю MoverEvent, НЕ требует реал-тайм стакана.
"""
import math
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Optional

from trades_buffer import Trade
import config


# ═══════════════════════════════════════════════════
# Структуры данных
# ═══════════════════════════════════════════════════

@dataclass
class SizeCluster:
    """Кластер сделок с одинаковым объёмом"""
    center_usdt: float
    count: int
    pct_of_total: float
    avg_size: float
    min_size: float
    max_size: float
    std_size: float
    trades: list[Trade] = field(default_factory=list, repr=False)

    @property
    def is_dominant(self) -> bool:
        return self.pct_of_total >= config.ALGO_MIN_CLUSTER_PCT and self.count >= config.ALGO_MIN_TRADES_CLUSTER

    @property
    def size_range_str(self) -> str:
        return f"${self.min_size:.1f}–${self.max_size:.1f}"

    @property
    def label(self) -> str:
        return f"${self.center_usdt:.1f} ({self.count}×, {self.pct_of_total:.0f}%)"


@dataclass
class TimingPattern:
    """Паттерн таймингов между сделками кластера"""
    pattern_type: str          # "metronome" / "second_locked" / "random" / "none"
    avg_interval_sec: float
    std_interval_sec: float
    min_interval_sec: float
    max_interval_sec: float
    locked_seconds: list[int] = field(default_factory=list)
    regularity_score: float = 0.0
    description: str = ""

    @property
    def is_regular(self) -> bool:
        return self.pattern_type in ("metronome", "second_locked")


@dataclass
class HonestyCheck:
    """
    Проверка честности алгоритма v2.

    НЕ зависит от BBO (решает проблему рассинхрона потоков).
    Анализирует только внутренние свойства сделок:

    1. Side consistency: если 80%+ сделок — одна сторона (например все BUY),
       значит робот ставит лимитки на другой стороне и их заполняют тейкеры.
       Это ЧЕСТНЫЙ мейкер.

    2. Price clustering: если сделки происходят на 1–3 ценовых уровнях
       из 50+ сделок → лимитные ордера заполняются по конкретным ценам.
       Много уровней → хаотичные рыночные ордера.

    3. Self-trade: если buy и sell проходят по ОДНОЙ цене в окне 1.5 сек →
       вероятный wash trading (торговля с собой). Опасно.
    """
    total_checked: int

    # Компонент 1: Side consistency
    buy_count: int
    sell_count: int
    dominant_side: str           # "BUY" / "SELL" / "MIXED"
    dominant_side_pct: float     # % доминирующей стороны
    side_score: float            # 0–40

    # Компонент 2: Price clustering
    unique_price_levels: int
    price_levels_ratio: float    # unique_levels / total_trades
    price_score: float           # 0–30

    # Компонент 3: Self-trade detection
    selftrade_pairs: int         # кол-во подозрительных пар
    selftrade_pct: float         # % сделок в self-trade парах
    selftrade_penalty: float     # 0–30 штраф

    # Итог
    honesty_score: float         # 0–70 (side + price - penalty)
    is_honest: bool
    verdict: str
    details: str = ""

    @property
    def verdict_emoji(self) -> str:
        if self.is_honest and self.honesty_score >= config.HONESTY_SCORE_HONEST:
            return "✅"
        if self.honesty_score >= config.HONESTY_SCORE_SUSPECT:
            return "⚠️"
        return "❌"


@dataclass
class WallBehavior:
    """
    Поведение стенки — как робот реагирует на рыночные условия.

    Анализирует MoverEvent (историю переставок):
    - retreat = стенка УДАЛЯЕТСЯ от текущей цены (поджим работает)
    - advance = стенка ПРИБЛИЖАЕТСЯ к цене (агрессивный ММ)
    - passive = мало переставок (стенка стоит)

    Практическое значение для трейдера:
    - 🏃 Убегает → можно давить, поджим работает
    - 🦈 Давит  → опасно, робот давит навстречу
    - 🔄 Микс   → непредсказуемый, осторожно
    - 🧱 Стоит  → стабильный, можно работать
    """
    symbol: str
    total_events: int
    retreat_count: int         # стенка удалилась от mid
    advance_count: int         # стенка приблизилась к mid
    same_count: int            # переставка вбок (без изменения расстояния)

    retreat_pct: float
    advance_pct: float

    behavior_type: str         # "retreater" / "aggressor" / "mixed" / "passive"
    description: str
    trade_advice: str          # совет для трейдера

    @property
    def emoji(self) -> str:
        return {"retreater": "🏃", "aggressor": "🦈",
                "mixed": "🔄", "passive": "🧱"}.get(self.behavior_type, "❓")

    @property
    def label(self) -> str:
        return f"{self.emoji} {self.description}"


@dataclass
class AlgoSignal:
    """Результат анализа алгоритма по паре"""
    symbol: str
    timestamp: float
    total_trades: int
    spread_pct: float

    clusters: list[SizeCluster] = field(default_factory=list)
    dominant_cluster: Optional[SizeCluster] = None
    timing: Optional[TimingPattern] = None
    honesty: Optional[HonestyCheck] = None
    wall_behavior: Optional[WallBehavior] = None

    has_ladder: bool = False
    ladder_steps: int = 0
    ladder_side: str = ""

    algo_score: float = 0.0
    verdict: str = ""

    @property
    def has_algo(self) -> bool:
        return self.dominant_cluster is not None and self.dominant_cluster.is_dominant

    @property
    def cluster_size_str(self) -> str:
        if self.dominant_cluster:
            return f"${self.dominant_cluster.center_usdt:.1f}"
        return "—"

    @property
    def timing_str(self) -> str:
        if self.timing and self.timing.description:
            return self.timing.description
        return "—"

    @property
    def honesty_str(self) -> str:
        if self.honesty:
            return self.honesty.verdict
        return "—"

    @property
    def ladder_str(self) -> str:
        if self.has_ladder:
            return f"{self.ladder_side} {self.ladder_steps}ст"
        return "—"

    @property
    def behavior_str(self) -> str:
        if self.wall_behavior:
            return self.wall_behavior.label
        return "—"


# ═══════════════════════════════════════════════════
# 1. КЛАСТЕРИЗАЦИЯ ОБЪЁМОВ (без изменений)
# ═══════════════════════════════════════════════════

def find_size_clusters(trades: list[Trade],
                       tolerance: float = None) -> list[SizeCluster]:
    if not trades or len(trades) < 5:
        return []
    if tolerance is None:
        tolerance = config.ALGO_SIZE_TOLERANCE

    valid_trades = [t for t in trades if t.usdt > 0.01]
    if len(valid_trades) < 5:
        return []

    sorted_trades = sorted(valid_trades, key=lambda t: t.usdt)
    total = len(sorted_trades)
    clusters: list[list[Trade]] = []
    used = set()

    for i, trade in enumerate(sorted_trades):
        if i in used:
            continue
        cluster = [trade]
        used.add(i)
        center = trade.usdt

        for j in range(i + 1, len(sorted_trades)):
            if j in used:
                continue
            other = sorted_trades[j]
            diff = abs(other.usdt - center) / max(center, 0.01)
            if diff <= tolerance:
                cluster.append(other)
                used.add(j)
                center = sum(t.usdt for t in cluster) / len(cluster)
            elif other.usdt > center * (1 + tolerance):
                break

        if len(cluster) >= 3:
            clusters.append(cluster)

    result = []
    for cluster_trades in clusters:
        sizes = [t.usdt for t in cluster_trades]
        sc = SizeCluster(
            center_usdt=round(statistics.mean(sizes), 2),
            count=len(cluster_trades),
            pct_of_total=round(len(cluster_trades) / total * 100, 1),
            avg_size=round(statistics.mean(sizes), 2),
            min_size=round(min(sizes), 2),
            max_size=round(max(sizes), 2),
            std_size=round(statistics.stdev(sizes), 2) if len(sizes) > 1 else 0.0,
            trades=sorted(cluster_trades, key=lambda t: t.time_ms),
        )
        result.append(sc)

    result.sort(key=lambda c: c.count, reverse=True)
    return result


# ═══════════════════════════════════════════════════
# 2. АНАЛИЗ ТАЙМИНГОВ (без изменений)
# ═══════════════════════════════════════════════════

def analyze_timing(cluster: SizeCluster) -> TimingPattern:
    trades = cluster.trades
    if len(trades) < 5:
        return TimingPattern(
            pattern_type="none", avg_interval_sec=0, std_interval_sec=0,
            min_interval_sec=0, max_interval_sec=0, description="Мало данных")

    sorted_t = sorted(trades, key=lambda t: t.time_ms)
    deltas = []
    for i in range(len(sorted_t) - 1):
        d = (sorted_t[i + 1].time_ms - sorted_t[i].time_ms) / 1000.0
        if 0 < d < 600:
            deltas.append(d)

    if len(deltas) < 3:
        return TimingPattern(
            pattern_type="none", avg_interval_sec=0, std_interval_sec=0,
            min_interval_sec=0, max_interval_sec=0, description="Мало интервалов")

    avg_d = statistics.mean(deltas)
    std_d = statistics.stdev(deltas) if len(deltas) > 1 else 0.0
    min_d, max_d = min(deltas), max(deltas)
    cv = std_d / avg_d if avg_d > 0 else 999

    # Секундный паттерн
    # B-22 fix: учитываем дрейф часов биржи (±ALGO_TIMING_CLOCK_DRIFT_SEC секунд).
    # Без допуска робот, торгующий каждые N секунд, размазывается по соседним
    # секундам из-за нестабильности серверного времени MEXC (типично ±500мс).
    # С допуском считаем сделки не только на секунде S, но и на S±drift.
    _drift = getattr(config, "ALGO_TIMING_CLOCK_DRIFT_SEC", 1)
    seconds = [t.second_of_minute for t in sorted_t]
    sec_counts = Counter(seconds)
    total_trades = len(sorted_t)
    hot_seconds = []
    for sec, cnt in sec_counts.most_common(5):
        # Расширяем окно: считаем сделки на sec ± drift (по модулю 60)
        drift_total = sum(
            sec_counts.get((sec + d) % 60, 0)
            for d in range(-_drift, _drift + 1)
        )
        if drift_total >= max(3, total_trades * 0.12):
            hot_seconds.append(sec)
    hot_total = sum(
        sec_counts.get((s + d) % 60, 0)
        for s in hot_seconds
        for d in range(-_drift, _drift + 1)
    )
    # Убираем задвоения если соседние секунды оба попали в hot_seconds
    hot_total = min(hot_total, total_trades)
    is_second_locked = (len(hot_seconds) >= 1 and hot_total >= total_trades * 0.35 and total_trades >= 10)
    is_metronome = cv < 0.35 and avg_d < 120 and len(deltas) >= 5

    if is_second_locked:
        hot_seconds.sort()
        desc = f"На {'/'.join(str(s).zfill(2) for s in hot_seconds[:4])} сек"
        regularity = min(hot_total / total_trades, 1.0)
        return TimingPattern(pattern_type="second_locked",
            avg_interval_sec=round(avg_d, 1), std_interval_sec=round(std_d, 1),
            min_interval_sec=round(min_d, 1), max_interval_sec=round(max_d, 1),
            locked_seconds=hot_seconds[:4], regularity_score=round(regularity, 2),
            description=desc)
    if is_metronome:
        desc = f"Каждые {avg_d:.0f}±{std_d:.0f}с"
        regularity = max(0, 1.0 - cv)
        return TimingPattern(pattern_type="metronome",
            avg_interval_sec=round(avg_d, 1), std_interval_sec=round(std_d, 1),
            min_interval_sec=round(min_d, 1), max_interval_sec=round(max_d, 1),
            regularity_score=round(regularity, 2), description=desc)

    desc = f"~{avg_d:.0f}с (разброс)"
    regularity = max(0, 0.5 - cv * 0.3)
    return TimingPattern(pattern_type="random",
        avg_interval_sec=round(avg_d, 1), std_interval_sec=round(std_d, 1),
        min_interval_sec=round(min_d, 1), max_interval_sec=round(max_d, 1),
        regularity_score=round(max(regularity, 0), 2), description=desc)


# ═══════════════════════════════════════════════════
# 3. ПРОВЕРКА ЧЕСТНОСТИ v2 (ПОЛНОСТЬЮ ПЕРЕПИСАНА)
#    Больше НЕ использует BBO. Только внутренние сигналы.
# ═══════════════════════════════════════════════════

def check_honesty(cluster: SizeCluster) -> HonestyCheck:
    """
    Проверка честности по ВНУТРЕННИМ сигналам сделок.
    Не зависит от BBO → нет проблемы рассинхрона.

    Три компонента:

    1) Side consistency (0–40 баллов):
       Честный мейкер ставит лимитки на одной стороне (BID или ASK).
       Тейкеры бьют в них → все сделки одного типа (isBuyerMaker).
       Если 80% сделок = BUY → робот ставит лимитные SELL-ы.
       Если ~50/50 → либо два робота, либо wash trading.

    2) Price clustering (0–30 баллов):
       Лимитные ордера стоят на конкретных ценах → сделки кластеризуются
       вокруг 1–3 уровней. Рыночные ордера → цены рассеяны.
       Мало уровней / много сделок = лимитный мейкер = честный.

    3) Self-trade detection (0–30 штраф):
       Если BUY и SELL проходят по ОДНОЙ цене в окне 1.5 сек →
       вероятная торговля с собой (wash trading).
       >25% таких пар → жулик.
    """
    trades = cluster.trades
    total = len(trades)

    if total < config.HONESTY_MIN_TRADES:
        return HonestyCheck(
            total_checked=0, buy_count=0, sell_count=0,
            dominant_side="—", dominant_side_pct=0, side_score=0,
            unique_price_levels=0, price_levels_ratio=0, price_score=0,
            selftrade_pairs=0, selftrade_pct=0, selftrade_penalty=0,
            honesty_score=0, is_honest=True, verdict="— Мало данных",
            details="Недостаточно сделок для анализа")

    # ─── Компонент 1: Side consistency ───
    buys = sum(1 for t in trades if t.is_buy)
    sells = total - buys
    dominant_count = max(buys, sells)
    dominant_pct = dominant_count / total * 100
    dominant_side = "BUY" if buys >= sells else "SELL"

    # Скоринг: чем больше перевес → тем честнее
    if dominant_pct >= 85:
        side_score = 40.0   # Явный мейкер на одной стороне
    elif dominant_pct >= 75:
        side_score = 32.0
    elif dominant_pct >= 65:
        side_score = 22.0
    elif dominant_pct >= 55:
        side_score = 12.0   # Лёгкий перевес
    else:
        side_score = 3.0    # ~50/50 — подозрительно

    if dominant_pct < 55:
        dominant_side = "MIXED"

    # ─── Компонент 2: Price level clustering ───
    # Округляем цены до значащей точности (8 знаков)
    price_levels = set()
    for t in trades:
        # Группируем цены с точностью до 0.05% друг от друга
        rounded = round(t.price, 10)
        price_levels.add(rounded)

    unique_levels = len(price_levels)
    ratio = unique_levels / total if total > 0 else 1.0

    # Мало уровней → лимитные ордера → честный
    if ratio <= 0.05:
        price_score = 30.0   # 1–2 уровня на 40+ сделок
    elif ratio <= 0.10:
        price_score = 25.0
    elif ratio <= 0.15:
        price_score = 18.0
    elif ratio <= 0.25:
        price_score = 10.0
    elif ratio <= 0.40:
        price_score = 5.0
    else:
        price_score = 0.0    # Много разных цен → хаотичные ордера

    # ─── Компонент 3: Self-trade detection ───
    window_ms = config.HONESTY_SELFTRADE_WINDOW_MS
    selftrade_pairs = 0
    selftrade_trade_ids = set()  # Индексы сделок в self-trade

    # Сортируем по времени
    sorted_t = sorted(trades, key=lambda t: t.time_ms)

    # Скользящее окно: ищем buy+sell одна цена
    for i in range(len(sorted_t)):
        ti = sorted_t[i]
        for j in range(i + 1, len(sorted_t)):
            tj = sorted_t[j]
            dt = tj.time_ms - ti.time_ms
            if dt > window_ms:
                break  # Вышли за окно
            if dt < 0:
                continue

            # Разные стороны + одна цена (±0.1%)
            if ti.is_buy != tj.is_buy:
                price_diff = abs(ti.price - tj.price) / max(ti.price, 0.0001)
                if price_diff < 0.001:  # Та же цена (±0.1%)
                    selftrade_pairs += 1
                    selftrade_trade_ids.add(i)
                    selftrade_trade_ids.add(j)
                    break  # Один match на сделку достаточно

    selftrade_trades = len(selftrade_trade_ids)
    selftrade_pct = selftrade_trades / total * 100 if total > 0 else 0

    # Штраф
    if selftrade_pct >= config.HONESTY_SELFTRADE_SCAM_PCT:
        selftrade_penalty = 30.0
    elif selftrade_pct >= config.HONESTY_SELFTRADE_WARN_PCT:
        selftrade_penalty = 15.0
    elif selftrade_pct >= 5:
        selftrade_penalty = 5.0
    else:
        selftrade_penalty = 0.0

    # ─── Итоговый скор ───
    honesty_score = side_score + price_score - selftrade_penalty
    honesty_score = max(0, min(70, honesty_score))

    # Вердикт
    if honesty_score >= config.HONESTY_SCORE_HONEST:
        verdict = "✅ Честный"
        is_honest = True
    elif honesty_score >= config.HONESTY_SCORE_SUSPECT:
        verdict = "⚠️ Под вопросом"
        is_honest = True  # Пока не жулик
    else:
        verdict = "❌ Жулик"
        is_honest = False

    # Детали
    details_parts = []
    if dominant_pct >= 75:
        details_parts.append(f"мейкер {dominant_side} {dominant_pct:.0f}%")
    elif dominant_pct < 55:
        details_parts.append(f"баланс BUY/SELL ≈50/50")
    details_parts.append(f"{unique_levels} цен.уровней на {total} сделок")
    if selftrade_pairs > 0:
        details_parts.append(f"self-trade: {selftrade_pairs} пар ({selftrade_pct:.0f}%)")

    return HonestyCheck(
        total_checked=total,
        buy_count=buys, sell_count=sells,
        dominant_side=dominant_side, dominant_side_pct=round(dominant_pct, 1),
        side_score=round(side_score, 1),
        unique_price_levels=unique_levels,
        price_levels_ratio=round(ratio, 3),
        price_score=round(price_score, 1),
        selftrade_pairs=selftrade_pairs,
        selftrade_pct=round(selftrade_pct, 1),
        selftrade_penalty=round(selftrade_penalty, 1),
        honesty_score=round(honesty_score, 1),
        is_honest=is_honest, verdict=verdict,
        details="; ".join(details_parts))


# ═══════════════════════════════════════════════════
# 4. ПОВЕДЕНИЕ СТЕНОК (НОВОЕ)
# ═══════════════════════════════════════════════════

def analyze_wall_behavior(
    symbol: str,
    mover_events: list = None,       # устарело, оставлено для совместимости
    current_mid_price: float = 0,
    tracked_walls: list = None,      # list[TrackedWall] из history.get_wall_behavior_raw()
) -> WallBehavior:
    """
    Классифицирует поведение стенок v2.

    ПОЧЕМУ СТАРЫЙ МЕТОД БЫЛ НЕПРАВИЛЬНЫМ:
    Старая версия сравнивала old_distance vs new_distance используя ТЕКУЩИЙ mid_price.
    Проблема: если монета живёт сама по себе (никто не поджимает), цена просто
    болтается внутри ренджа. Робот двигает стенку ВМЕСТЕ с ценой — нормальное
    маркет-мейкерское поведение. Функция не знала: стенка приблизилась потому что
    робот агрессивен, или потому что цена сама к ней пришла?

    НОВЫЙ МЕТОД — анализ TrackedWall.dist_history:
    Каждый скан записывает (timestamp, distance_pct, mid_price) для каждой стенки.
    Это позволяет отделить движение стенки от движения цены:

    1. price_corr (корреляция mid_price ↔ distance_pct):
       +1 = стенка двигается ВМЕСТЕ с ценой → passive MM (следует за рынком).
            Это нейтральный робот, не агрессивный.
       -1 = стенка двигается ПРОТИВ цены → aggressor (давит навстречу).
            Цена растёт → стенка на ASK приближается. Опасно.
        0 = стенка независима от цены.

    2. dist_trend (тренд distance_pct):
       >0 = стенка в среднем удаляется от mid → retreater (убегает).
       <0 = стенка в среднем приближается к mid → aggressor.
       ≈0 = стоит на месте → passive.

    3. dist_cv (коэффициент вариации distance_pct):
       Высокий CV → стенка "прыгает" (переставляется часто) → активный ММ.
       Низкий CV → стенка стоит стабильно.

    КЛАССИФИКАЦИЯ по обоим сигналам вместе:
    - retreater: dist_trend > +порог ИЛИ (price_corr > 0.5 и dist_trend >= 0)
    - aggressor: dist_trend < -порог ИЛИ price_corr < -0.5
    - passive MM: |dist_trend| мал, price_corr > 0.4 (следует за рынком)
    - passive: нет данных или стенка стоит
    """
    # ── Если нет tracked_walls — фолбэк на старый метод через mover_events ──
    if not tracked_walls:
        return _analyze_wall_behavior_legacy(symbol, mover_events, current_mid_price)

    # Фильтруем только стенки с достаточной историей
    # seen_count >= 2: стенка видна в 2+ сканах — достаточно для тренда
    walls = [tw for tw in tracked_walls if tw.seen_count >= 2]
    if not walls:
        return _analyze_wall_behavior_legacy(symbol, mover_events, current_mid_price)

    # ── Агрегируем сигналы по всем стенкам ──
    trends = []
    corrs = []
    cvs = []
    total_events = sum(tw.seen_count for tw in walls)

    for tw in walls:
        trends.append(tw.dist_trend)
        corrs.append(tw.price_corr)
        cvs.append(tw.dist_cv)

    avg_trend = sum(trends) / len(trends)
    avg_corr = sum(corrs) / len(corrs)
    avg_cv = sum(cvs) / len(cvs)

    # Разбиваем стенки по поведению
    retreat_count = 0
    advance_count = 0
    passive_count = 0

    TREND_THRESH = 0.3   # % изменения distance_pct считается значимым
    CORR_THRESH = 0.45   # порог корреляции

    for tw in walls:
        trend = tw.dist_trend
        corr = tw.price_corr

        if trend > TREND_THRESH:
            # Стенка удаляется от mid
            retreat_count += 1
        elif trend < -TREND_THRESH:
            if corr < -CORR_THRESH:
                # Движется ПРОТИВ цены → настоящий агрессор
                advance_count += 1
            elif corr > CORR_THRESH:
                # Движется вместе с ценой (цена идёт к стенке, стенка уходит)
                # → это на самом деле passive MM, не агрессор
                passive_count += 1
            else:
                advance_count += 1
        else:
            passive_count += 1

    total_walls = retreat_count + advance_count + passive_count
    if total_walls == 0:
        total_walls = 1

    retreat_pct = retreat_count / total_walls * 100
    advance_pct = advance_count / total_walls * 100

    retreat_thresh = config.WALL_BEHAVIOR_RETREAT_THRESHOLD * 100
    advance_thresh = config.WALL_BEHAVIOR_ADVANCE_THRESHOLD * 100

    # ── Классификация ──
    if retreat_pct >= retreat_thresh:
        btype = "retreater"
        desc = f"Убегает ({retreat_pct:.0f}%)"
        advice = "🟢 Поджим работает. Стенка отступает от цены сама по себе."
    elif advance_pct >= advance_thresh:
        btype = "aggressor"
        desc = f"Давит ({advance_pct:.0f}%)"
        advice = "🔴 Опасно. Стенка движется ПРОТИВ цены — активный агрессор."
    elif avg_corr > CORR_THRESH and abs(avg_trend) < TREND_THRESH:
        # Стенка просто следует за ценой с постоянным расстоянием
        btype = "passive"
        desc = f"Passive MM (corr={avg_corr:.2f})"
        advice = "🧱 Следует за рынком. Стабильный. Можно работать рядом."
    elif retreat_pct > 35 and advance_pct > 35:
        btype = "mixed"
        desc = f"Микс ({retreat_pct:.0f}%↔{advance_pct:.0f}%)"
        advice = "🟡 Непредсказуемо. Робот чередует стратегии."
    else:
        btype = "mixed"
        desc = f"Переставляет (trend={avg_trend:+.2f})"
        advice = "🟡 Нет чёткого паттерна. Наблюдай."

    # Дополнительный контекст в совете
    if avg_cv > 0.5:
        advice += " Высокая активность переставок."

    return WallBehavior(
        symbol=symbol, total_events=total_events,
        retreat_count=retreat_count, advance_count=advance_count,
        same_count=passive_count,
        retreat_pct=round(retreat_pct, 1), advance_pct=round(advance_pct, 1),
        behavior_type=btype, description=desc, trade_advice=advice)


def _analyze_wall_behavior_legacy(
    symbol: str,
    mover_events: list = None,
    current_mid_price: float = 0,
) -> WallBehavior:
    """
    Старый метод через MoverEvent — используется как фолбэк
    когда нет истории TrackedWall (мало сканов).
    Менее точен: не отличает passive MM от retreater.
    """
    min_events = config.WALL_BEHAVIOR_MIN_EVENTS

    if not mover_events or len(mover_events) < min_events:
        return WallBehavior(
            symbol=symbol, total_events=len(mover_events) if mover_events else 0,
            retreat_count=0, advance_count=0, same_count=0,
            retreat_pct=0, advance_pct=0,
            behavior_type="passive",
            description="Мало данных",
            trade_advice="⏳ Нужно больше сканов для точного анализа.")

    if current_mid_price <= 0:
        all_prices = []
        for e in mover_events:
            all_prices.extend([e.old_price, e.new_price])
        current_mid_price = sum(all_prices) / len(all_prices) if all_prices else 1.0

    retreat = advance = same = 0
    for e in mover_events:
        old_dist = abs(e.old_price - current_mid_price) / current_mid_price
        new_dist = abs(e.new_price - current_mid_price) / current_mid_price
        diff = new_dist - old_dist
        if diff > 0.0005:
            retreat += 1
        elif diff < -0.0005:
            advance += 1
        else:
            same += 1

    total = retreat + advance + same or 1
    retreat_pct = retreat / total * 100
    advance_pct = advance / total * 100

    retreat_thresh = config.WALL_BEHAVIOR_RETREAT_THRESHOLD * 100
    advance_thresh = config.WALL_BEHAVIOR_ADVANCE_THRESHOLD * 100

    if retreat_pct >= retreat_thresh:
        btype, desc = "retreater", f"Убегает ({retreat_pct:.0f}%) [legacy]"
        advice = "🟢 Предварительно: поджим работает. Нужно больше сканов."
    elif advance_pct >= advance_thresh:
        btype, desc = "aggressor", f"Давит ({advance_pct:.0f}%) [legacy]"
        advice = "🔴 Предварительно: агрессор. Нужно больше сканов."
    elif retreat_pct > 40 and advance_pct > 40:
        btype, desc = "mixed", f"Микс ({retreat_pct:.0f}%↔{advance_pct:.0f}%)"
        advice = "🟡 Непредсказуемо."
    else:
        btype, desc = "mixed", "Переставляет"
        advice = "🟡 Нет чёткого паттерна."

    return WallBehavior(
        symbol=symbol, total_events=len(mover_events),
        retreat_count=retreat, advance_count=advance, same_count=same,
        retreat_pct=round(retreat_pct, 1), advance_pct=round(advance_pct, 1),
        behavior_type=btype, description=desc, trade_advice=advice)


# ═══════════════════════════════════════════════════
# 5. СКОРИНГ (обновлённый)
# ═══════════════════════════════════════════════════

def compute_algo_score(signal: AlgoSignal) -> float:
    score = 0.0

    # 1. Повторяющийся объём (0–30)
    dc = signal.dominant_cluster
    if dc and dc.is_dominant:
        base = 15
        bonus = min((dc.pct_of_total - 30) * 0.5, 15)
        score += base + max(bonus, 0)

    # 2. Тайминг (0–25)
    tp = signal.timing
    if tp:
        if tp.pattern_type == "second_locked":
            score += 25
        elif tp.pattern_type == "metronome":
            score += 15 + tp.regularity_score * 5
        elif tp.pattern_type == "random":
            score += 5

    # 3. Спред (0–20)
    sp = signal.spread_pct
    if 2.0 <= sp <= 4.0:
        score += 10
    elif 4.0 < sp <= 8.0:
        score += 20
    elif 8.0 < sp <= 12.0:
        score += 15
    elif 1.0 <= sp < 2.0:
        score += 3

    # 4. Честность v2 (-30 / +15)
    hc = signal.honesty
    if hc and hc.total_checked >= config.HONESTY_MIN_TRADES:
        if hc.honesty_score >= config.HONESTY_SCORE_HONEST:
            score += 15
        elif hc.honesty_score >= config.HONESTY_SCORE_SUSPECT:
            score += 5
        else:
            score -= 30  # Жулик → штраф (мягче чем -50, т.к. метод не идеален)

    # 5. Лесенка (+10)
    if signal.has_ladder:
        if signal.ladder_steps >= 5:
            score += 10
        elif signal.ladder_steps >= 4:
            score += 7
        else:
            score += 4

    # 6. Поведение стенки (0–5 бонус)
    wb = signal.wall_behavior
    if wb:
        if wb.behavior_type == "retreater":
            score += 5   # Убегает → хороший знак для трейдера
        elif wb.behavior_type == "aggressor":
            score -= 5   # Давит → опасно

    # 7. Количество данных (0–10)
    if signal.total_trades >= 50:
        score += 10
    elif signal.total_trades >= 20:
        score += 5

    # 8. Бонус за множественные кластеры (0–5)
    dominant_count = sum(1 for c in signal.clusters if c.is_dominant)
    if dominant_count >= 2:
        score += 5

    return round(max(min(score, 100), 0), 1)


def classify_algo(score: float, honesty: Optional[HonestyCheck] = None) -> str:
    if honesty and not honesty.is_honest:
        return "❌ Жулик"
    if score >= 65:
        return "🔥 Жирный"
    if score >= 40:
        return "⚠️ Робот"
    if score >= 20:
        return "🤖 Слабый"
    return "— Нет"


# ═══════════════════════════════════════════════════
# 6. ГЛАВНАЯ ФУНКЦИЯ АНАЛИЗА
# ═══════════════════════════════════════════════════

def analyze_algo(
    symbol: str,
    trades: list,
    spread_pct: float = 0.0,
    ladders: list = None,
    mover_events: list = None,
    current_mid_price: float = 0,
    tracked_walls: list = None,   # list[TrackedWall] из tracker.get_wall_behavior_raw()
) -> AlgoSignal:
    """
    Полный анализ пары на наличие алгоритма.

    Args:
        symbol: XYZUSDT
        trades: список Trade из буфера
        spread_pct: текущий спред пары
        ladders: список LadderPattern из analyzer.py
        mover_events: список MoverEvent из history.py (фолбэк)
        current_mid_price: текущая mid цена
        tracked_walls: список TrackedWall из tracker.get_wall_behavior_raw()
                       ПРЕДПОЧТИТЕЛЬНЫЙ источник для analyze_wall_behavior v2.
                       Если передан — используется новый метод (по dist_history).
                       Если нет — фолбэк на mover_events (старый метод).
    """
    signal = AlgoSignal(
        symbol=symbol,
        timestamp=time.time(),
        total_trades=len(trades),
        spread_pct=spread_pct,
    )

    if len(trades) < config.ALGO_MIN_TRADES:
        signal.verdict = "— Мало данных"
        return signal

    # 1. Кластеры
    clusters = find_size_clusters(trades)
    signal.clusters = clusters

    # 2. Доминантный кластер
    dominant = None
    for c in clusters:
        if c.is_dominant:
            dominant = c
            break
    signal.dominant_cluster = dominant

    # 3. Тайминг
    if dominant:
        signal.timing = analyze_timing(dominant)

    # 4. Честность v2 (без BBO!)
    if dominant:
        signal.honesty = check_honesty(dominant)

    # 5. Лесенки
    if ladders:
        best_ladder = max(ladders, key=lambda l: l.steps, default=None)
        if best_ladder and best_ladder.steps >= config.LADDER_MIN_STEPS:
            signal.has_ladder = True
            signal.ladder_steps = best_ladder.steps
            signal.ladder_side = best_ladder.side

    # 6. Поведение стенки v2
    # Приоритет: tracked_walls (новый метод по dist_history)
    # Фолбэк: mover_events (старый метод)
    signal.wall_behavior = analyze_wall_behavior(
        symbol=symbol,
        mover_events=mover_events,
        current_mid_price=current_mid_price,
        tracked_walls=tracked_walls,
    )

    # 7. Скоринг
    signal.algo_score = compute_algo_score(signal)
    signal.verdict = classify_algo(signal.algo_score, signal.honesty)

    return signal
