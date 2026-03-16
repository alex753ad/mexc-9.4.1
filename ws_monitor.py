#!/usr/bin/env python3
"""
MEXC WebSocket Monitor v3.0
Реалтайм: стаканы + лента сделок (deals) + алго-детектор
"""

import asyncio
import json
import time
import argparse
import signal
from datetime import datetime
from collections import defaultdict

import websockets
import aiohttp

import config
from mexc_client import MexcClientAsync
from analyzer import (
    analyze_order_book, ScanResult, WallInfo, MoverEvent, detect_movers
)
from trades_buffer import TradesBuffer, Trade, parse_ws_deals, parse_rest_trades
from algo_detector import analyze_algo, AlgoSignal


class WsOrderBook:
    def __init__(self, symbol):
        self.symbol = symbol
        self.bids = {}
        self.asks = {}
        self.last_update = 0.0
        self.initialized = False

    def apply_snapshot(self, bids, asks):
        self.bids = {b[0]: b[1] for b in bids}
        self.asks = {a[0]: a[1] for a in asks}
        self.initialized = True
        self.last_update = time.time()

    def apply_update(self, bids, asks):
        for price, qty in bids:
            if float(qty) == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price, qty in asks:
            if float(qty) == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self.last_update = time.time()

    def to_depth_dict(self):
        sorted_bids = sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)[:config.ORDER_BOOK_DEPTH]
        sorted_asks = sorted(self.asks.items(), key=lambda x: float(x[0]))[:config.ORDER_BOOK_DEPTH]
        return {"bids": [[p, q] for p, q in sorted_bids], "asks": [[p, q] for p, q in sorted_asks]}

    def get_best_bid_ask(self):
        bb = max((float(p) for p in self.bids.keys()), default=0.0)
        ba = min((float(p) for p in self.asks.keys()), default=0.0)
        return bb, ba


class MexcWsMonitor:
    def __init__(self, on_event_callback=None):
        self.ws_url = config.MEXC_WS_URL
        self.order_books = {}
        self.prev_results = {}
        self.ticker_cache = {}
        self.on_event = on_event_callback or self._default_callback
        self._running = False
        self._ws = None
        self.trades_buffer = TradesBuffer(
            max_trades=config.ALGO_TRADES_BUFFER_SIZE,
            max_age_sec=config.ALGO_TRADES_MAX_AGE_SEC)
        self.algo_signals = {}
        self._last_algo_check = {}
        self.stats = {
            "messages": 0, "movers_detected": 0,
            "new_walls_detected": 0, "deals_received": 0,
            "algos_detected": 0, "start_time": 0,
        }

    async def start(self, symbols):
        self._running = True
        self.stats["start_time"] = time.time()
        for sym in symbols:
            self.order_books[sym] = WsOrderBook(sym)
        print(f"\n  Подключаюсь к WebSocket ({len(symbols)} пар)...")
        while self._running:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10, max_size=10*1024*1024) as ws:
                    self._ws = ws
                    print("  WebSocket подключён")
                    for sym in symbols:
                        await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": [f"spot@public.limit.depth.v3.api.pb@{sym}@20"]}))
                        await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": [f"spot@public.deals.v3.api@{sym}"]}))
                        await asyncio.sleep(0.1)
                    print(f"  Подписки: {len(symbols)} пар x 2 (depth + deals)")
                    await self._init_snapshots(symbols)
                    async for msg in ws:
                        if not self._running: break
                        await self._handle_message(msg)
            except websockets.ConnectionClosed:
                if self._running:
                    print("  Соединение потеряно, переподключаюсь через 5с...")
                    await asyncio.sleep(5)
            except Exception as e:
                if self._running:
                    print(f"  Ошибка WS: {e}, переподключаюсь через 10с...")
                    await asyncio.sleep(10)

    async def stop(self):
        self._running = False
        if self._ws: await self._ws.close()

    async def _init_snapshots(self, symbols):
        client = MexcClientAsync()
        try:
            # Тикеры — один запрос для всех
            try:
                tickers = await client.get_all_tickers_24h()
                if tickers:
                    self.ticker_cache = {t["symbol"]: t for t in tickers if "symbol" in t}
            except Exception:
                pass

            # Стаканы — батчами по 5 с паузой чтобы не словить rate limit
            for i, sym in enumerate(symbols):
                try:
                    book = await client.get_order_book(sym, config.ORDER_BOOK_DEPTH)
                    ob = self.order_books[sym]
                    if book and (book.get("bids") or book.get("asks")):
                        ob.apply_snapshot(book.get("bids", []), book.get("asks", []))
                        ticker = self.ticker_cache.get(sym, {"quoteVolume": "0"})
                        result = analyze_order_book(sym, book, ticker)
                        if result:
                            self.prev_results[sym] = result
                    else:
                        # Нет данных — помечаем как инициализированный с пустым стаканом
                        # WS-сообщения заполнят его при первом обновлении
                        ob.initialized = True
                except Exception:
                    # Даже при ошибке — разблокируем, WS заполнит
                    self.order_books[sym].initialized = True

                try:
                    raw_trades = await client.get_recent_trades(sym, 200)
                    if not raw_trades:
                        # Fallback: aggTrades
                        raw_trades = await client._request(
                            "/api/v3/aggTrades", {"symbol": sym, "limit": 200})
                    if raw_trades:
                        # aggTrades имеет другой формат — parse_agg_trades
                        if raw_trades and isinstance(raw_trades, list):
                            if raw_trades[0].get("a") is not None:
                                from trades_buffer import parse_agg_trades
                                parsed = parse_agg_trades(raw_trades)
                            else:
                                parsed = parse_rest_trades(raw_trades)
                            if parsed:
                                self.trades_buffer.add_trades(sym, parsed)
                except Exception:
                    pass

                # Пауза каждые 5 запросов
                if (i + 1) % 5 == 0:
                    await asyncio.sleep(0.5)
                else:
                    await asyncio.sleep(0.1)

            init_ob = sum(1 for ob in self.order_books.values() if ob.initialized)
            init_tb = len([s for s in symbols if self.trades_buffer.count(s) > 0])
            print(f"  Стаканы: {init_ob}/{len(symbols)} | Сделки: {init_tb}/{len(symbols)}")
        except Exception as e:
            print(f"  Ошибка инициализации снапшотов: {e}")
            # Разблокируем все символы чтобы WS мог работать
            for sym in symbols:
                self.order_books[sym].initialized = True
        finally:
            await client.close()

    async def _handle_message(self, raw_msg):
        try: data = json.loads(raw_msg)
        except: return
        self.stats["messages"] += 1
        channel = data.get("c", data.get("channel", ""))
        symbol = data.get("s", data.get("symbol", ""))
        if not symbol: return
        if "deals" in channel:
            await self._handle_deals(symbol, data)
            return
        if symbol not in self.order_books: return
        depth = data.get("d", data.get("publiclimitdepths", {}))
        if not depth or not isinstance(depth, dict): return
        bids_list = depth.get("bids", depth.get("bidsList", []))
        asks_list = depth.get("asks", depth.get("asksList", []))
        bids, asks = [], []
        for b in bids_list:
            if isinstance(b, dict): bids.append([b.get("price","0"), b.get("quantity","0")])
            elif isinstance(b, list): bids.append(b)
        for a in asks_list:
            if isinstance(a, dict): asks.append([a.get("price","0"), a.get("quantity","0")])
            elif isinstance(a, list): asks.append(a)
        if not bids and not asks: return
        ob = self.order_books[symbol]
        ob.apply_snapshot(bids, asks)
        ob.initialized = True  # WS прислал данные — стакан активен
        if time.time() - ob.last_update < 5.0: return
        await self._analyze_and_alert(symbol)

    async def _handle_deals(self, symbol, data):
        d = data.get("d", data)
        deals_raw = d.get("deals", d.get("dealsList", []))
        if not deals_raw and isinstance(d, list): deals_raw = d
        if not deals_raw: return
        parsed = parse_ws_deals(deals_raw)
        if not parsed: return
        self.trades_buffer.add_trades(symbol, parsed)
        self.stats["deals_received"] += len(parsed)
        now = time.time()
        if now - self._last_algo_check.get(symbol, 0) >= 30:
            self._last_algo_check[symbol] = now
            await self._check_algo(symbol)

    async def _check_algo(self, symbol):
        trades = self.trades_buffer.get_trades(symbol, last_sec=300)
        if len(trades) < config.ALGO_MIN_TRADES: return
        spread = 0.0
        ob = self.order_books.get(symbol)
        if ob and ob.initialized:
            bb, ba = ob.get_best_bid_ask()
            if bb > 0 and ba > 0: spread = (ba - bb) / bb * 100
        sig = analyze_algo(symbol, trades, spread)
        prev = self.algo_signals.get(symbol)
        self.algo_signals[symbol] = sig
        if sig.has_algo and sig.algo_score >= config.ALGO_SCORE_ROBOT:
            if prev is None or not prev.has_algo or prev.algo_score < config.ALGO_SCORE_ROBOT:
                self.stats["algos_detected"] += 1
                await self.on_event("ALGO_FOUND", sig, None)

    async def _analyze_and_alert(self, symbol):
        ob = self.order_books[symbol]
        # Разрешаем анализ если есть хоть что-то в стакане (WS уже прислал данные)
        if not ob.initialized and not ob.bids and not ob.asks:
            return
        depth = ob.to_depth_dict()
        ticker = self.ticker_cache.get(symbol, {"quoteVolume": "0"})
        result = analyze_order_book(symbol, depth, ticker)
        if not result: return
        prev = self.prev_results.get(symbol)
        if prev:
            movers = detect_movers(result, prev)
            if movers:
                result.mover_events = movers
                self.stats["movers_detected"] += len(movers)
                for event in movers: await self.on_event("MOVER", event, result)
        if prev:
            prev_prices = {w.price for w in prev.all_walls}
            for wall in result.all_walls:
                if wall.price not in prev_prices and wall.size_usdt >= 100:
                    self.stats["new_walls_detected"] += 1
                    await self.on_event("NEW_WALL", wall, result)
        self.prev_results[symbol] = result

    @staticmethod
    async def _default_callback(event_type, event, result):
        now = datetime.now().strftime("%H:%M:%S")
        if event_type == "MOVER":
            e = event
            arr = "UP" if e.direction == "UP" else "DN"
            print(f"  {now} {arr} ПЕРЕСТАВЛЯШ {e.symbol} {e.side}: ${e.size_usdt:,.0f} {e.old_price:.8g} -> {e.new_price:.8g} ({e.shift_pct:+.2f}%)")
        elif event_type == "NEW_WALL":
            w = event
            print(f"  {now} СТЕНКА {result.symbol} {w.side}: ${w.size_usdt:,.0f} @ {w.price:.8g} ({w.multiplier}x)")
        elif event_type == "ALGO_FOUND":
            sig = event
            dc = sig.dominant_cluster
            print(f"\n  {now} АЛГОРИТМ: {sig.symbol} | Скор={sig.algo_score} | {sig.verdict}")
            if dc: print(f"       Сайз: ${dc.center_usdt:.1f} ({dc.count}x, {dc.pct_of_total:.0f}%) | Тайминг: {sig.timing_str}\n")


async def scan_and_select(n_pairs=25):
    print(f"\n  REST-сканирование топ-{n_pairs}...\n")
    client = MexcClientAsync()
    try:
        info = await client.get_exchange_info()
        if not info: return []
        symbols = [s["symbol"] for s in info.get("symbols", [])
                   if s.get("quoteAsset") == config.QUOTE_ASSET
                   and s.get("isSpotTradingAllowed", True) and s.get("status") == "1"
                   and s["symbol"] not in config.ALGO_BLACKLIST]
        tickers = await client.get_all_tickers_24h()
        if not tickers: return []
        tm = {t["symbol"]: t for t in tickers if "symbol" in t}
        cands = [(sym, tm[sym]) for sym in symbols if sym in tm
                 and config.MIN_DAILY_VOLUME_USDT <= float(tm[sym].get("quoteVolume", 0)) <= config.MAX_DAILY_VOLUME_USDT]
        cands.sort(key=lambda x: float(x[1].get("quoteVolume", 0)), reverse=True)
        results = []
        for i in range(0, len(cands), config.MAX_CONCURRENT_REQUESTS):
            batch = cands[i:i+config.MAX_CONCURRENT_REQUESTS]
            br = await asyncio.gather(*[_scan_one(client, s, t) for s, t in batch])
            results.extend([r for r in br if r])
            if i + config.MAX_CONCURRENT_REQUESTS < len(cands): await asyncio.sleep(config.BATCH_DELAY)
        results.sort(key=lambda r: r.score, reverse=True)
        return results[:n_pairs]
    finally:
        await client.close()

async def _scan_one(client, symbol, ticker):
    try:
        book = await client.get_order_book(symbol, config.ORDER_BOOK_DEPTH)
        if not book: return None
        result = analyze_order_book(symbol, book, ticker)
        if result and result.spread_pct < config.MIN_SPREAD_PCT: return None
        return result
    except: return None

async def main():
    parser = argparse.ArgumentParser(description="MEXC WS Monitor v3.0")
    parser.add_argument("--pairs", type=int, default=25)
    parser.add_argument("--symbols", type=str, default=None)
    args = parser.parse_args()
    print("=" * 55)
    print("  MEXC WebSocket Monitor v3.0")
    print("  Реалтайм: плотности + переставляши + алгоритмы")
    print("=" * 55)
    monitor = MexcWsMonitor()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(monitor.stop()))
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
    else:
        top = await scan_and_select(min(args.pairs, 28))
        symbols = [r.symbol for r in top]
    if not symbols:
        print("  Не найдено подходящих пар")
        return
    await monitor.start(symbols)

if __name__ == "__main__":
    asyncio.run(main())
