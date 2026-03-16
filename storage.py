"""
Storage v1.0 — SQLite persistent storage для MEXC Scanner (Phase 3)

Сохраняет данные между перезапусками Streamlit:
- Результаты сканов (стенки, скоры)
- Алго-сигналы
- Историю границ ренджа (для долгосрочного drift)
- События recovery стенок
- Результаты скринера

Использование:
    db = ScannerDB()  # создаёт ~/.mexc_scanner/scanner.db
    db.save_scan_result(symbol, result_dict)
    history = db.load_range_history(symbol, last_hours=24)
    db.close()
"""

import os
import json
import time
import sqlite3
import threading
import atexit
from typing import Optional
from pathlib import Path


DB_DIR = os.path.expanduser("~/.mexc_scanner")
DB_FILE = os.path.join(DB_DIR, "scanner.db")
MAX_ROWS_PER_TABLE = 50_000      # Автоочистка старых записей
CLEANUP_INTERVAL_SEC = 3600       # Чистка раз в час
MAX_AGE_DAYS = 7                  # Хранить данные 7 дней


class ScannerDB:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_FILE
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        # B-11 fix: Lock защищает _conn от конкурентного доступа
        # из Streamlit UI-потока и WS background-потока одновременно.
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()
        self._last_cleanup = 0.0
        # B-13 fix: atexit надёжнее __del__ — гарантирует закрытие при выходе
        atexit.register(self.close)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ── Thread-safe обёртки (B-11 fix) ────────────────────────────────────────
    # Все обращения к SQLite идут через эти методы.
    # threading.Lock гарантирует что UI-поток и WS-поток не пишут одновременно.

    def _exec(self, sql: str, params: tuple = ()):
        """Выполнить один запрос без возврата результата."""
        with self._lock:
            return self._conn.execute(sql, params)

    def _execmany(self, sql: str, params_list):
        """Выполнить пакетный запрос."""
        with self._lock:
            return self._conn.executemany(sql, params_list)

    def _commit(self):
        """Thread-safe commit."""
        with self._lock:
            self._conn.commit()

    def _query(self, sql: str, params: tuple = ()):
        """Выполнить SELECT и вернуть все строки."""
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def _queryone(self, sql: str, params: tuple = ()):
        """Выполнить SELECT и вернуть одну строку."""
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    # ── Публичные методы для прямого доступа из app.py (B-11 fix) ──────────
    def find_open_recovery_event(self, symbol: str, wall_price: float):
        """Найти незакрытое событие recovery для стенки. Thread-safe."""
        return self._queryone(
            "SELECT id, disappeared_at FROM recovery_events "
            "WHERE symbol=? AND wall_price=? AND recovered=0 "
            "ORDER BY ts DESC LIMIT 1",
            (symbol, wall_price)
        )

    def update_recovery_event(self, event_id: int,
                               recovered_at: float, recovery_time_sec: float):
        """Закрыть событие recovery. Thread-safe."""
        with self._lock:
            self._conn.execute(
                "UPDATE recovery_events SET recovered=1, recovered_at=?, "
                "recovery_time_sec=? WHERE id=?",
                (recovered_at, recovery_time_sec, event_id)
            )
            self._conn.commit()

    def get_recovery_counts(self) -> tuple:
        """Вернуть (total, done) счётчики recovery. Thread-safe."""
        total = self._queryone("SELECT COUNT(*) as c FROM recovery_events")
        done  = self._queryone("SELECT COUNT(*) as c FROM recovery_events WHERE recovered=1")
        return (total["c"] if total else 0, done["c"] if done else 0)

    # ───────────────────────────────────────────────────────────────────────
    def _create_tables(self):
        with self._lock:
          self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                mid_price REAL,
                spread_pct REAL,
                volume_24h REAL,
                score REAL,
                bid_walls_json TEXT,
                ask_walls_json TEXT,
                ladders_count INTEGER DEFAULT 0,
                trade_count_24h INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS algo_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                algo_score REAL,
                verdict TEXT,
                has_algo INTEGER,
                cluster_size_usdt REAL,
                cluster_pct REAL,
                timing_type TEXT,
                timing_avg_sec REAL,
                honesty_score REAL,
                selftrade_pct REAL,
                alternating_score REAL DEFAULT 0,
                behavior_type TEXT,
                behavior_retreat_pct REAL,
                spread_pct REAL,
                total_trades INTEGER
            );

            CREATE TABLE IF NOT EXISTS range_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                lower_price REAL,
                upper_price REAL,
                lower_size_usdt REAL,
                upper_size_usdt REAL,
                mid_price REAL,
                bid_wall_count INTEGER,
                ask_wall_count INTEGER
            );

            CREATE TABLE IF NOT EXISTS recovery_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT,
                wall_price REAL,
                wall_size_usdt REAL,
                disappeared_at REAL,
                recovered_at REAL,
                recovery_time_sec REAL,
                recovered INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS screener_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                screener_score REAL,
                grade TEXT,
                is_workable INTEGER,
                range_width_pct REAL,
                behavior_type TEXT,
                squeeze_ratio REAL,
                recovery_avg_sec REAL,
                honesty_score REAL,
                exclude_flags TEXT,
                warning_flags TEXT,
                recommendation TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_scans_symbol_ts ON scans(symbol, ts);
            CREATE INDEX IF NOT EXISTS idx_algo_symbol_ts ON algo_signals(symbol, ts);
            CREATE INDEX IF NOT EXISTS idx_range_symbol_ts ON range_points(symbol, ts);
            CREATE INDEX IF NOT EXISTS idx_recovery_symbol_ts ON recovery_events(symbol, ts);
            CREATE INDEX IF NOT EXISTS idx_screener_symbol_ts ON screener_scores(symbol, ts);
        """)
        self._commit()

    # ═══════════════════════════════════════════════
    # SAVE
    # ═══════════════════════════════════════════════

    def save_scan_results(self, results: list):
        """Сохраняет batch результатов скана"""
        now = time.time()
        rows = []
        for r in results:
            bid_walls = [{"price": w.price, "size": w.size_usdt, "mult": w.multiplier,
                          "dist": w.distance_pct} for w in r.bid_walls] if r.bid_walls else []
            ask_walls = [{"price": w.price, "size": w.size_usdt, "mult": w.multiplier,
                          "dist": w.distance_pct} for w in r.ask_walls] if r.ask_walls else []
            rows.append((
                now, r.symbol, r.mid_price, r.spread_pct, r.volume_24h_usdt,
                r.score, json.dumps(bid_walls), json.dumps(ask_walls),
                len(r.ladders) if hasattr(r, 'ladders') else 0,
                getattr(r, 'trade_count_24h', 0),
            ))
        self._execmany(
            "INSERT INTO scans (ts, symbol, mid_price, spread_pct, volume_24h, score, "
            "bid_walls_json, ask_walls_json, ladders_count, trade_count_24h) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        self._commit()
        self._maybe_cleanup()

    def save_algo_signal(self, symbol: str, sig):
        """Вставляет один AlgoSignal без commit.
        Для одиночного сохранения используй save_algo_signal + commit вручную.
        Для массового — используй save_algo_signals_batch (один commit на весь батч).
        """
        dc = sig.dominant_cluster
        tp = sig.timing
        hc = sig.honesty
        wb = sig.wall_behavior

        self._exec(
            "INSERT INTO algo_signals (ts, symbol, algo_score, verdict, has_algo, "
            "cluster_size_usdt, cluster_pct, timing_type, timing_avg_sec, "
            "honesty_score, selftrade_pct, alternating_score, "
            "behavior_type, behavior_retreat_pct, spread_pct, total_trades) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (time.time(), symbol, sig.algo_score, sig.verdict,
             1 if sig.has_algo else 0,
             dc.center_usdt if dc else 0, dc.pct_of_total if dc else 0,
             tp.pattern_type if tp else "", tp.avg_interval_sec if tp else 0,
             hc.honesty_score if hc else 0, hc.selftrade_pct if hc else 0,
             getattr(hc, 'alternating_score', 0) if hc else 0,
             wb.behavior_type if wb else "", wb.retreat_pct if wb else 0,
             sig.spread_pct, sig.total_trades))
        # B-28 fix: commit намеренно убран — вызывающий код решает когда коммитить

    def save_algo_signals_batch(self, signals: dict):
        """Сохраняет dict {symbol: AlgoSignal}.
        B-28 fix: один commit на весь батч вместо N коммитов (~150-300мс экономии при 30 парах).
        """
        for sym, sig in signals.items():
            try:
                self.save_algo_signal(sym, sig)
            except Exception:
                continue
        self._commit()  # один коммит после всех INSERT

    def save_range_point(self, symbol: str, lower_price: float, upper_price: float,
                         lower_size: float, upper_size: float, mid_price: float,
                         bid_count: int, ask_count: int):
        self._exec(
            "INSERT INTO range_points (ts, symbol, lower_price, upper_price, "
            "lower_size_usdt, upper_size_usdt, mid_price, bid_wall_count, ask_wall_count) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (time.time(), symbol, lower_price, upper_price,
             lower_size, upper_size, mid_price, bid_count, ask_count))
        self._commit()

    def save_range_points_batch(self, results: list):
        """Сохраняет range points из batch scan results"""
        now = time.time()
        rows = []
        for r in results:
            # Кластерная граница
            bid_walls = r.bid_walls or []
            ask_walls = r.ask_walls or []
            if bid_walls:
                sorted_b = sorted(bid_walls, key=lambda w: w.size_usdt, reverse=True)[:3]
                total_bs = sum(w.size_usdt for w in sorted_b)
                lower_p = sum(w.price * w.size_usdt for w in sorted_b) / total_bs if total_bs > 0 else 0
                lower_s = total_bs
            else:
                lower_p = r.best_bid
                lower_s = 0
            if ask_walls:
                sorted_a = sorted(ask_walls, key=lambda w: w.size_usdt, reverse=True)[:3]
                total_as = sum(w.size_usdt for w in sorted_a)
                upper_p = sum(w.price * w.size_usdt for w in sorted_a) / total_as if total_as > 0 else 0
                upper_s = total_as
            else:
                upper_p = r.best_ask
                upper_s = 0
            rows.append((now, r.symbol, lower_p, upper_p, lower_s, upper_s,
                          r.mid_price, len(bid_walls), len(ask_walls)))
        if rows:
            self._execmany(
                "INSERT INTO range_points (ts, symbol, lower_price, upper_price, "
                "lower_size_usdt, upper_size_usdt, mid_price, bid_wall_count, ask_wall_count) "
                "VALUES (?,?,?,?,?,?,?,?,?)", rows)
            self._commit()

    def save_recovery_event(self, symbol: str, side: str, wall_price: float,
                            wall_size: float, disappeared_at: float,
                            recovered_at: float, recovery_time: float,
                            recovered: bool):
        self._exec(
            "INSERT INTO recovery_events (ts, symbol, side, wall_price, wall_size_usdt, "
            "disappeared_at, recovered_at, recovery_time_sec, recovered) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (time.time(), symbol, side, wall_price, wall_size,
             disappeared_at, recovered_at, recovery_time, 1 if recovered else 0))
        self._commit()

    def save_screener_result(self, r):
        """Сохраняет RobotScreenResult.
        БАГ-ФИКС: все обращения к вложенным объектам (squeeze_info,
        recovery_profile, range_info, robot_profile) обёрнуты в None-проверки.
        Ранее min(si.squeeze_ratio_lower, ...) падал с AttributeError когда
        squeeze_info = None (монеты без достаточного объёма для расчёта).
        """
        si  = r.squeeze_info
        rec = r.recovery_profile
        ri  = r.range_info
        rp  = r.robot_profile

        # Безопасное извлечение каждого поля
        range_width    = ri.width_pct if ri is not None else 0.0
        behavior_type  = rp.behavior_type if rp is not None else ""
        squeeze_ratio  = (min(si.squeeze_ratio_lower, si.squeeze_ratio_upper)
                          if si is not None else 0.0)
        recovery_sec   = (rec.avg_recovery_sec
                          if rec is not None and rec.has_data else 0.0)
        recommendation = (r.recommendation[:200]
                          if r.recommendation else "")
        exclude_json   = json.dumps(r.exclude_flags if r.exclude_flags else [])
        warning_json   = json.dumps(r.warning_flags if r.warning_flags else [])

        self._exec(
            "INSERT INTO screener_scores (ts, symbol, screener_score, grade, is_workable, "
            "range_width_pct, behavior_type, squeeze_ratio, recovery_avg_sec, "
            "honesty_score, exclude_flags, warning_flags, recommendation) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (time.time(), r.symbol, r.screener_score, r.grade,
             1 if r.is_workable else 0,
             range_width, behavior_type, squeeze_ratio, recovery_sec,
             r.honesty_score, exclude_json, warning_json, recommendation))
        self._commit()

    def save_screener_results_batch(self, results: list):
        for r in results:
            try:
                self.save_screener_result(r)
            except Exception:
                continue

    # ═══════════════════════════════════════════════
    # LOAD
    # ═══════════════════════════════════════════════

    def load_range_history(self, symbol: str, last_hours: float = 24) -> list:
        """Загружает историю границ ренджа для drift анализа"""
        cutoff = time.time() - last_hours * 3600
        rows = self._query(
            "SELECT ts, lower_price, upper_price, lower_size_usdt, upper_size_usdt, "
            "mid_price, bid_wall_count, ask_wall_count "
            "FROM range_points WHERE symbol = ? AND ts > ? ORDER BY ts",
            (symbol, cutoff))
        return [dict(r) for r in rows]

    def load_recovery_events(self, symbol: str, last_hours: float = 24) -> list:
        cutoff = time.time() - last_hours * 3600
        rows = self._query(
            "SELECT * FROM recovery_events WHERE symbol = ? AND ts > ? ORDER BY ts",
            (symbol, cutoff))
        return [dict(r) for r in rows]

    def load_recovery_stats(self, symbol: str, last_hours: float = 24) -> dict:
        """Агрегированная статистика recovery"""
        events = self.load_recovery_events(symbol, last_hours)
        if not events:
            return {"has_data": False}
        total = len(events)
        recovered = [e for e in events if e.get("recovered", 0)]
        times = [e["recovery_time_sec"] for e in recovered if e.get("recovery_time_sec", 0) > 0]
        return {
            "has_data": True,
            "total_disappearances": total,
            "total_recovered": len(recovered),
            "recovery_rate": round(len(recovered) / total, 2) if total > 0 else 0,
            "avg_recovery_sec": round(sum(times) / len(times), 1) if times else 0,
            "median_recovery_sec": round(sorted(times)[len(times)//2], 1) if times else 0,
            "min_recovery_sec": round(min(times), 1) if times else 0,
            "max_recovery_sec": round(max(times), 1) if times else 0,
        }

    def load_screener_history(self, symbol: str, last_hours: float = 24) -> list:
        cutoff = time.time() - last_hours * 3600
        rows = self._query(
            "SELECT * FROM screener_scores WHERE symbol = ? AND ts > ? ORDER BY ts",
            (symbol, cutoff))
        return [dict(r) for r in rows]

    def load_latest_screener_scores(self, limit: int = 50) -> list:
        """Последние оценки всех монет (по одной на symbol)"""
        rows = self._exec("""
            SELECT s.* FROM screener_scores s
            INNER JOIN (
                SELECT symbol, MAX(ts) as max_ts
                FROM screener_scores GROUP BY symbol
            ) latest ON s.symbol = latest.symbol AND s.ts = latest.max_ts
            ORDER BY s.screener_score DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def load_scan_count(self, symbol: str, last_hours: float = 1) -> int:
        cutoff = time.time() - last_hours * 3600
        row = self._queryone(
            "SELECT COUNT(*) as cnt FROM scans WHERE symbol = ? AND ts > ?",
            (symbol, cutoff))
        return row["cnt"] if row else 0

    def load_total_scan_count(self) -> int:
        # B-12 fix: ROUND(ts, 0) группирует записи одного батча по секунде.
        # COUNT(DISTINCT ts) давал ложный результат при микросекундных расхождениях
        # между строками одного и того же скана.
        row = self._exec(
            "SELECT COUNT(DISTINCT ROUND(ts, 0)) as cnt FROM scans"
        ).fetchone()
        return row["cnt"] if row else 0

    def get_tracked_symbols(self) -> list:
        """Все символы которые когда-либо сканировались"""
        rows = self._query(
            "SELECT DISTINCT symbol FROM scans ORDER BY symbol")
        return [r["symbol"] for r in rows]

    def get_db_stats(self) -> dict:
        stats = {}
        for table in ["scans", "algo_signals", "range_points", "recovery_events", "screener_scores"]:
            row = self._queryone(f"SELECT COUNT(*) as cnt FROM {table}")
            stats[table] = row["cnt"] if row else 0
        stats["db_size_mb"] = round(os.path.getsize(self.db_path) / 1024 / 1024, 2) if os.path.exists(self.db_path) else 0
        return stats

    # ═══════════════════════════════════════════════
    # CLEANUP
    # ═══════════════════════════════════════════════

    def _maybe_cleanup(self):
        now = time.time()
        if now - self._last_cleanup < CLEANUP_INTERVAL_SEC:
            return
        self._last_cleanup = now
        self.cleanup()

    def cleanup(self, max_age_days: int = None):
        """Удаляет старые записи и освобождает дисковое пространство"""
        age = max_age_days or MAX_AGE_DAYS
        cutoff = time.time() - age * 86400
        for table in ["scans", "algo_signals", "range_points", "recovery_events", "screener_scores"]:
            self._exec(f"DELETE FROM {table} WHERE ts < ?", (cutoff,))
        self._commit()
        self._exec("PRAGMA optimize")
        # B-14 fix: VACUUM возвращает освобождённые страницы на диск.
        # Без него файл БД растёт даже после DELETE.
        self._exec("VACUUM")

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    # B-13 fix: __del__ удалён — ненадёжен при завершении интерпретатора.
    # Закрытие соединения обеспечивается atexit.register(self.close) в __init__
    # и context manager (__enter__/__exit__).
