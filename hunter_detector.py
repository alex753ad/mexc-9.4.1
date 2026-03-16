"""
Hunter Pattern Detector v1.0 — Детекция следов трейдера-охотника

Ищет в ленте сделок циклический паттерн:
  1. Фаза squeeze: серия мелких SELL → цена падает к BID-стенке
  2. Покупка у дна: крупный BUY в зоне BID-стенки
  3. Продажа у верха: крупный SELL в зоне ASK-стенки
  4. Повторение = подтверждение что монета торгуема

Если кто-то уже торгует монету по squeeze-стратегии — значит она рабочая.
"""

import time
import statistics
from dataclasses import dataclass, field
from typing import Optional
from trades_buffer import Trade


@dataclass
class HunterCycle:
    """Один цикл squeeze→buy→sell"""
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    entry_usdt: float
    exit_usdt: float
    profit_pct: float
    duration_sec: float
    # Фаза squeeze перед покупкой
    squeeze_trades_count: int = 0
    squeeze_avg_size_usdt: float = 0.0


@dataclass
class HunterPattern:
    """Результат детекции hunter-паттерна"""
    found: bool
    cycle_count: int = 0
    avg_profit_pct: float = 0.0
    avg_duration_sec: float = 0.0
    total_volume_usdt: float = 0.0
    cycles: list = field(default_factory=list)
    # Активность
    is_active: bool = False         # Последний цикл < 1 часа назад
    last_cycle_ago_sec: float = 0.0
    # Аномалия объёма
    volume_anomaly: float = 0.0     # Насколько объём в squeeze > среднего (>1 = аномалия)

    @property
    def score(self) -> float:
        """0-30 баллов для скоринга"""
        if not self.found:
            return 0.0
        s = 0.0
        # Кол-во циклов (до 15 баллов)
        s += min(self.cycle_count * 5, 15)
        # Средняя прибыль (до 5 баллов)
        if self.avg_profit_pct > 0:
            s += min(self.avg_profit_pct, 5)
        # Активность (до 5 баллов)
        if self.is_active:
            s += 5
        elif self.last_cycle_ago_sec < 7200:  # < 2 часа
            s += 3
        # Аномалия объёма (до 5 баллов)
        if self.volume_anomaly > 1.5:
            s += 5
        elif self.volume_anomaly > 1.0:
            s += 2
        return round(min(s, 30), 1)

    @property
    def label(self) -> str:
        if not self.found:
            return "— нет следов"
        active = "🟢 активно" if self.is_active else "⚪ неактивно"
        return f"✅ {self.cycle_count} цикл(ов), ~{self.avg_profit_pct:.1f}%, {active}"


def detect_hunter_pattern(
    trades: list,
    lower_price: float,
    upper_price: float,
    range_width_pct: float = 0.0,
) -> HunterPattern:
    """
    Ищет паттерн squeeze-трейдера в истории сделок.

    Логика:
    - Покупка в нижних 30% ренджа с объёмом > 2x среднего = entry
    - Последующая продажа в верхних 30% ренджа = exit
    - Между entry и exit: движение цены вверх
    - Перед entry: аномальный объём мелких SELL (squeeze phase)

    Args:
        trades: список Trade, отсортированный по time_ms
        lower_price: нижняя граница ренджа (VWAP BID кластера)
        upper_price: верхняя граница ренджа (VWAP ASK кластера)
    """
    if not trades or len(trades) < 30:
        return HunterPattern(found=False)
    if lower_price <= 0 or upper_price <= lower_price:
        return HunterPattern(found=False)

    # Сортируем по времени
    sorted_t = sorted(trades, key=lambda t: t.time_ms)
    total = len(sorted_t)

    # Средний объём сделки
    usdt_values = [t.usdt for t in sorted_t if t.usdt > 0]
    if not usdt_values:
        return HunterPattern(found=False)
    avg_usdt = statistics.mean(usdt_values)
    median_usdt = statistics.median(usdt_values)

    # Зоны: нижние 30% и верхние 30% ренджа
    range_size = upper_price - lower_price
    buy_zone_upper = lower_price + range_size * 0.35   # нижние 35%
    sell_zone_lower = upper_price - range_size * 0.35   # верхние 35%

    # Порог "крупной сделки"
    large_threshold = max(avg_usdt * 1.8, median_usdt * 2.5)

    cycles = []
    i = 0
    while i < total:
        t = sorted_t[i]

        # Ищем крупную покупку в нижней зоне
        if (t.is_buy and
            t.price <= buy_zone_upper and
            t.usdt >= large_threshold):

            entry = t

            # Анализируем фазу squeeze перед покупкой (30 сделок до)
            squeeze_start = max(0, i - 30)
            pre_trades = sorted_t[squeeze_start:i]
            squeeze_sells = [pt for pt in pre_trades if not pt.is_buy]
            squeeze_count = len(squeeze_sells)
            squeeze_avg = statistics.mean([pt.usdt for pt in squeeze_sells]) if squeeze_sells else 0

            # Ищем выход: продажи в верхней зоне (в следующих 300 сделках).
            # B-17 fix: агрегируем продажи в скользящем окне 30 сек —
            # реальный выход часто разбит на несколько iceberg-ордеров.
            # Раньше: одна продажа < порога → цикл терялся.
            # Теперь: сумма продаж за 30 сек сравнивается с порогом.
            ICEBERG_WINDOW_MS = 30_000  # 30 секунд для агрегации
            search_end = min(i + 300, total)
            found_exit = False

            j = i + 1
            while j < search_end:
                tj = sorted_t[j]

                # Пропускаем сделки не в верхней зоне
                if tj.is_buy or tj.price < sell_zone_lower:
                    j += 1
                    continue

                # Нашли первую продажу в зоне — агрегируем окно 30 сек
                window_end_ms = tj.time_ms + ICEBERG_WINDOW_MS
                window_sells = [
                    t for t in sorted_t[j:search_end]
                    if not t.is_buy
                    and t.price >= sell_zone_lower
                    and t.time_ms <= window_end_ms
                ]
                total_exit_usdt = sum(t.usdt for t in window_sells)

                if total_exit_usdt >= large_threshold * 0.5:
                    # Средневзвешенная цена выхода
                    avg_exit_price = (
                        sum(t.price * t.usdt for t in window_sells) / total_exit_usdt
                    )
                    profit_pct = (avg_exit_price - entry.price) / entry.price * 100

                    if profit_pct > 0.3:  # Минимум 0.3% прибыли (после комиссий)
                        last_exit_trade = window_sells[-1]
                        duration = (last_exit_trade.time_ms - entry.time_ms) / 1000
                        if 10 < duration < 86400:  # 10 сек — 24 часа
                            cycles.append(HunterCycle(
                                entry_time_ms=entry.time_ms,
                                exit_time_ms=last_exit_trade.time_ms,
                                entry_price=entry.price,
                                exit_price=round(avg_exit_price, 8),
                                entry_usdt=entry.usdt,
                                exit_usdt=round(total_exit_usdt, 2),
                                profit_pct=round(profit_pct, 2),
                                duration_sec=round(duration, 0),
                                squeeze_trades_count=squeeze_count,
                                squeeze_avg_size_usdt=round(squeeze_avg, 2),
                            ))
                            # Продолжаем после последней сделки окна
                            i = sorted_t.index(last_exit_trade)
                            found_exit = True
                            break

                # Этот j не дал цикл — идём дальше
                j += 1

            if not found_exit:
                i += 1
        else:
            i += 1

    if not cycles:
        return HunterPattern(found=False)

    # Аномалия объёма: сравниваем средний размер сделок в фазе squeeze с общим средним.
    # B-18 fix: реализована задокументированная логика вместо неверного счётчика сделок.
    # Охотник выдавливает цену МЕЛКИМИ сделками → squeeze_avg < overall_avg.
    # Чем мельче squeeze-сделки относительно обычных — тем выше аномалия.
    # volume_anomaly > 1.5 означает что squeeze-сделки в 1.5x мельче среднего.
    overall_avg = avg_usdt
    squeeze_volumes = []
    for c in cycles:
        if c.squeeze_avg_size_usdt > 0:
            squeeze_volumes.append(c.squeeze_avg_size_usdt)
    volume_anomaly = 0.0
    if squeeze_volumes and overall_avg > 0:
        squeeze_avg_overall = statistics.mean(squeeze_volumes)
        if squeeze_avg_overall > 0:
            # overall_avg / squeeze_avg: > 1 = squeeze мельче среднего (аномалия есть)
            volume_anomaly = overall_avg / squeeze_avg_overall

    # Активность
    now_ms = time.time() * 1000
    last_exit = max(c.exit_time_ms for c in cycles)
    last_ago = (now_ms - last_exit) / 1000
    is_active = last_ago < 3600  # < 1 час

    avg_profit = statistics.mean([c.profit_pct for c in cycles])
    avg_duration = statistics.mean([c.duration_sec for c in cycles])
    total_vol = sum(c.entry_usdt + c.exit_usdt for c in cycles)

    return HunterPattern(
        found=True,
        cycle_count=len(cycles),
        avg_profit_pct=round(avg_profit, 2),
        avg_duration_sec=round(avg_duration, 0),
        total_volume_usdt=round(total_vol, 0),
        cycles=cycles,
        is_active=is_active,
        last_cycle_ago_sec=round(last_ago, 0),
        volume_anomaly=round(volume_anomaly, 2),
    )
