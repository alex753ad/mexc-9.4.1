"""
TradesBuffer v1.0 — Кольцевой буфер сделок по парам.
Используется и в WS-мониторе (реалтайм), и в Streamlit (REST-загрузка).
"""
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Trade:
    """Одна сделка (принт)"""
    price: float
    qty: float
    usdt: float              # price * qty
    time_ms: int             # timestamp в миллисекундах
    is_buyer_maker: bool     # True = тейкер продал (SELL), False = тейкер купил (BUY)
    second_of_minute: int = 0  # секунда внутри минуты (0-59) — для секундного анализа

    def __post_init__(self):
        if self.time_ms > 0:
            self.second_of_minute = int((self.time_ms % 60000) / 1000)

    @property
    def is_buy(self) -> bool:
        return not self.is_buyer_maker

    @property
    def side(self) -> str:
        return "BUY" if self.is_buy else "SELL"

    @property
    def time_sec(self) -> float:
        return self.time_ms / 1000.0


class TradesBuffer:
    """
    Кольцевой буфер сделок для каждой пары.
    Хранит последние max_trades сделок.
    Автоматически удаляет устаревшие (старше max_age_sec).
    """

    def __init__(self, max_trades: int = 500, max_age_sec: int = 600):
        self.max_trades = max_trades
        self.max_age_sec = max_age_sec
        self._buffers: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_trades))
        self._stats: dict[str, dict] = defaultdict(
            lambda: {"total_added": 0, "last_add": 0.0})
        # B-19 fix: время последней автоматической чистки буфера
        self._last_gc: float = 0.0

    def add_trade(self, symbol: str, trade: Trade):
        """Добавляет одну сделку в буфер"""
        self._buffers[symbol].append(trade)
        st = self._stats[symbol]
        st["total_added"] += 1
        st["last_add"] = time.time()

    def add_trades(self, symbol: str, trades: list[Trade]):
        """Добавляет пакет сделок (например из REST)"""
        buf = self._buffers[symbol]
        for t in trades:
            buf.append(t)
        st = self._stats[symbol]
        st["total_added"] += len(trades)
        st["last_add"] = time.time()

    def get_trades(self, symbol: str,
                   last_n: Optional[int] = None,
                   last_sec: Optional[int] = None) -> list[Trade]:
        """
        Получить сделки по паре.
        last_n — последние N сделок
        last_sec — сделки за последние N секунд
        Без параметров — все сделки из буфера.
        """
        # B-19 fix: периодическая авто-чистка устаревших сделок.
        # deque(maxlen=) ограничивает только количество, но не возраст.
        # Без cleanup устаревшие сделки загрязняют алго-анализ.
        now = time.time()
        if now - self._last_gc > 60:
            self.cleanup()
            self._last_gc = now

        buf = self._buffers.get(symbol)
        if not buf:
            return []

        trades = list(buf)

        # Фильтр по времени
        if last_sec is not None:
            cutoff_ms = (time.time() - last_sec) * 1000
            trades = [t for t in trades if t.time_ms >= cutoff_ms]

        # Фильтр по количеству
        if last_n is not None:
            trades = trades[-last_n:]

        return trades

    def get_symbols(self) -> list[str]:
        """Список пар с данными"""
        return list(self._buffers.keys())

    def count(self, symbol: str) -> int:
        """Количество сделок в буфере по паре"""
        return len(self._buffers.get(symbol, []))

    def get_stats(self, symbol: str) -> dict:
        """Статистика по паре"""
        buf = self._buffers.get(symbol)
        if not buf:
            return {"count": 0, "total_added": 0, "last_add": 0.0}
        return {
            "count": len(buf),
            **self._stats[symbol],
        }

    def get_summary(self, symbol: str) -> dict:
        """Краткая сводка по сделкам (объёмы, интервалы, стороны)"""
        trades = self.get_trades(symbol, last_sec=300)
        if not trades:
            return {}

        buy_count = sum(1 for t in trades if t.is_buy)
        sell_count = len(trades) - buy_count
        total_vol = sum(t.usdt for t in trades)
        buy_vol = sum(t.usdt for t in trades if t.is_buy)

        # Средний интервал между сделками
        avg_interval = 0.0
        if len(trades) > 1:
            sorted_t = sorted(trades, key=lambda t: t.time_ms)
            deltas = [(sorted_t[i+1].time_ms - sorted_t[i].time_ms) / 1000
                      for i in range(len(sorted_t) - 1)
                      if sorted_t[i+1].time_ms > sorted_t[i].time_ms]
            if deltas:
                avg_interval = sum(deltas) / len(deltas)

        return {
            "count_5m": len(trades),
            "buy_count": buy_count,
            "sell_count": sell_count,
            "total_vol_usdt": total_vol,
            "buy_vol_usdt": buy_vol,
            "sell_vol_usdt": total_vol - buy_vol,
            "buy_pct": round(buy_count / len(trades) * 100, 1) if trades else 0,
            "avg_interval_sec": round(avg_interval, 2),
            "avg_size_usdt": round(total_vol / len(trades), 2) if trades else 0,
        }

    def cleanup(self, max_age_sec: Optional[int] = None):
        """Удалить устаревшие сделки из всех буферов"""
        age = max_age_sec or self.max_age_sec
        cutoff_ms = (time.time() - age) * 1000
        for symbol in list(self._buffers.keys()):
            buf = self._buffers[symbol]
            while buf and buf[0].time_ms < cutoff_ms:
                buf.popleft()
            if not buf:
                del self._buffers[symbol]
                del self._stats[symbol]

    def clear(self, symbol: Optional[str] = None):
        """Очистить буфер"""
        if symbol:
            self._buffers.pop(symbol, None)
            self._stats.pop(symbol, None)
        else:
            self._buffers.clear()
            self._stats.clear()


# ═══════════════════════════════════════════════════
# Конвертация из сырых данных REST API
# ═══════════════════════════════════════════════════

def parse_rest_trades(raw_trades: list) -> list[Trade]:
    """
    Конвертирует сырой ответ MEXC /api/v3/trades в список Trade.
    Формат MEXC:
      {"id": ..., "price": "0.00123", "qty": "100",
       "quoteQty": "0.123", "time": 1700000000000,
       "isBuyerMaker": true}
    """
    if not raw_trades or not isinstance(raw_trades, list):
        return []

    trades = []
    for t in raw_trades:
        if not isinstance(t, dict):
            continue
        try:
            price = float(t.get("price", 0))
            qty = float(t.get("qty", 0))
            if price <= 0 or qty <= 0:
                continue
            time_ms = int(float(t.get("time", 0)))
            is_buyer_maker = bool(t.get("isBuyerMaker", True))

            trades.append(Trade(
                price=price,
                qty=qty,
                usdt=price * qty,
                time_ms=time_ms,
                is_buyer_maker=is_buyer_maker,
            ))
        except (ValueError, TypeError, KeyError):
            continue

    return trades


def parse_agg_trades(raw_trades: list) -> list[Trade]:
    """
    Конвертирует сырой ответ MEXC /api/v3/aggTrades в список Trade.
    Формат:
      {"a": id, "p": "0.00123", "q": "100",
       "f": firstId, "l": lastId, "T": timestamp, "m": isBuyerMaker}
    """
    if not raw_trades or not isinstance(raw_trades, list):
        return []

    trades = []
    for t in raw_trades:
        if not isinstance(t, dict):
            continue
        try:
            price = float(t.get("p", 0))
            qty = float(t.get("q", 0))
            if price <= 0 or qty <= 0:
                continue
            time_ms = int(float(t.get("T", t.get("time", 0))))
            is_buyer_maker = bool(t.get("m", t.get("isBuyerMaker", True)))

            trades.append(Trade(
                price=price,
                qty=qty,
                usdt=price * qty,
                time_ms=time_ms,
                is_buyer_maker=is_buyer_maker,
            ))
        except (ValueError, TypeError, KeyError):
            continue

    return trades


def parse_ws_deals(deals_data: list) -> list[Trade]:
    """
    Парсит поток deals из MEXC WebSocket.
    Формат MEXC WS deals (v3):
      {"deals": [{"p": "0.123", "v": "100", "S": 1, "t": 1700000000}]}
      S: 1=BUY, 2=SELL
      или: {"price": "...", "quantity": "...", ...}
    """
    if not deals_data or not isinstance(deals_data, list):
        return []

    trades = []
    for d in deals_data:
        if not isinstance(d, dict):
            continue
        try:
            # Формат 1: краткий (p/v/S/t)
            price = float(d.get("p", d.get("price", 0)))
            qty = float(d.get("v", d.get("quantity", d.get("qty", 0))))
            if price <= 0 or qty <= 0:
                continue

            # Время — может быть в секундах или миллисекундах
            raw_time = float(d.get("t", d.get("time", d.get("T", 0))))
            time_ms = int(raw_time * 1000) if raw_time < 1e12 else int(raw_time)

            # Сторона: S=1 → BUY (isBuyerMaker=False), S=2 → SELL (isBuyerMaker=True)
            side = d.get("S", d.get("s", d.get("side", 0)))
            if isinstance(side, str):
                is_buyer_maker = side.upper() == "SELL" or side == "2"
            else:
                is_buyer_maker = int(side) == 2

            trades.append(Trade(
                price=price,
                qty=qty,
                usdt=price * qty,
                time_ms=time_ms,
                is_buyer_maker=is_buyer_maker,
            ))
        except (ValueError, TypeError, KeyError):
            continue

    return trades
