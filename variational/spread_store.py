from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


class SpreadStore:
    """SQLite-backed long-term spread history and window statistics."""

    _DAY_MS = 86_400_000
    _UTC8_OFFSET_MS = 8 * 3_600_000

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()
        self._read_lock = threading.RLock()
        in_memory = str(path) == ":memory:"
        database: str | Path = f"file:spread-store-{id(self)}?mode=memory&cache=shared" if in_memory else path
        self._writer = sqlite3.connect(database, check_same_thread=False, uri=in_memory)
        self._writer.row_factory = sqlite3.Row
        with self._write_lock:
            self._writer.execute("PRAGMA journal_mode=WAL")
            self._writer.execute("PRAGMA synchronous=NORMAL")
            self._writer.execute(
                """
                CREATE TABLE IF NOT EXISTS spread_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_ms INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    var_bid REAL,
                    var_ask REAL,
                    lighter_bid REAL,
                    lighter_ask REAL,
                    long_edge_pct REAL,
                    short_edge_pct REAL,
                    usdc_usdt_bid REAL,
                    usdc_usdt_ask REAL,
                    usdc_usdt_received_ms INTEGER
                )
                """
            )
            existing_columns = {
                str(row[1]) for row in self._writer.execute("PRAGMA table_info(spread_samples)").fetchall()
            }
            migration_columns = {
                "usdc_usdt_bid": "REAL",
                "usdc_usdt_ask": "REAL",
                "usdc_usdt_received_ms": "INTEGER",
            }
            for column, column_type in migration_columns.items():
                if column not in existing_columns:
                    self._writer.execute(
                        f"ALTER TABLE spread_samples ADD COLUMN {column} {column_type}"
                    )
            self._writer.execute(
                "CREATE INDEX IF NOT EXISTS idx_spread_samples_asset_time "
                "ON spread_samples(asset, timestamp_ms)"
            )
            self._writer.commit()
        self._reader = sqlite3.connect(database, check_same_thread=False, uri=in_memory)
        self._reader.row_factory = sqlite3.Row
        self._reader.execute("PRAGMA query_only=ON")

    def close(self) -> None:
        with self._read_lock:
            self._reader.close()
        with self._write_lock:
            self._writer.close()

    def record(
        self,
        *,
        asset: str,
        var_bid: Any,
        var_ask: Any,
        lighter_bid: Any,
        lighter_ask: Any,
        long_edge_pct: Any,
        short_edge_pct: Any,
        usdc_usdt_bid: Any = None,
        usdc_usdt_ask: Any = None,
        usdc_usdt_received_ms: int | None = None,
        timestamp_ms: int | None = None,
    ) -> None:
        if long_edge_pct is None and short_edge_pct is None:
            return
        values = (
            timestamp_ms or int(time.time() * 1000),
            str(asset).strip().upper() or "UNKNOWN",
            self._float_or_none(var_bid),
            self._float_or_none(var_ask),
            self._float_or_none(lighter_bid),
            self._float_or_none(lighter_ask),
            self._float_or_none(long_edge_pct),
            self._float_or_none(short_edge_pct),
            self._float_or_none(usdc_usdt_bid),
            self._float_or_none(usdc_usdt_ask),
            usdc_usdt_received_ms,
        )
        with self._write_lock:
            self._writer.execute(
                """
                INSERT INTO spread_samples (
                    timestamp_ms, asset, var_bid, var_ask, lighter_bid, lighter_ask,
                    long_edge_pct, short_edge_pct, usdc_usdt_bid, usdc_usdt_ask,
                    usdc_usdt_received_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            self._writer.commit()

    def window_stats(self, asset: str, window_seconds: float, side: str) -> tuple[float | None, float | None, float | None]:
        column = self._side_column(side)
        cutoff_ms = int((time.time() - window_seconds) * 1000)
        with self._read_lock:
            rows = self._reader.execute(
                f"SELECT {column} AS value FROM spread_samples "
                f"WHERE asset = ? AND timestamp_ms >= ? AND {column} IS NOT NULL ORDER BY value",
                (asset.upper(), cutoff_ms),
            ).fetchall()
        values = [float(row["value"]) for row in rows]
        if not values:
            return None, None, None
        return float(median(values)), self._percentile_sorted(values, 90), self._percentile_sorted(values, 10)

    def hourly_stats(self, asset: str, hours: int = 12) -> list[dict[str, Any]]:
        cutoff_ms = int((time.time() - hours * 3600) * 1000)
        with self._read_lock:
            rows = self._reader.execute(
                """
                SELECT timestamp_ms, long_edge_pct, short_edge_pct
                FROM spread_samples
                WHERE asset = ? AND timestamp_ms >= ?
                ORDER BY timestamp_ms
                """,
                (asset.upper(), cutoff_ms),
            ).fetchall()
        buckets: dict[int, dict[str, list[float]]] = {}
        for row in rows:
            hour_key = int(row["timestamp_ms"]) // 3_600_000
            bucket = buckets.setdefault(hour_key, {"long": [], "short": []})
            if row["long_edge_pct"] is not None:
                bucket["long"].append(float(row["long_edge_pct"]))
            if row["short_edge_pct"] is not None:
                bucket["short"].append(float(row["short_edge_pct"]))
        result: list[dict[str, Any]] = []
        for hour_key in sorted(buckets, reverse=True):
            result.append({
                "hour_key": hour_key,
                "long": self._stats_from_values(buckets[hour_key]["long"]),
                "short": self._stats_from_values(buckets[hour_key]["short"]),
            })
        return result

    def history(
        self,
        asset: str,
        window_seconds: float,
        max_points: int = 600,
        *,
        end_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        end_ms = int(time.time() * 1000) if end_ms is None else int(end_ms)
        start_ms = end_ms - int(window_seconds * 1000)
        bucket_ms = max(1000, int(window_seconds * 1000 / max_points))
        with self._read_lock:
            rows = self._reader.execute(
                """
                SELECT
                    (timestamp_ms / ?) * ? AS bucket_ms,
                    AVG(long_edge_pct) AS long_edge_pct,
                    AVG(short_edge_pct) AS short_edge_pct
                FROM spread_samples
                WHERE asset = ? AND timestamp_ms >= ? AND timestamp_ms <= ?
                GROUP BY bucket_ms
                ORDER BY bucket_ms
                """,
                (bucket_ms, bucket_ms, asset.upper(), start_ms, end_ms),
            ).fetchall()
        return [
            {
                "timestampMs": int(row["bucket_ms"]),
                "longEdge": row["long_edge_pct"],
                "shortEdge": row["short_edge_pct"],
            }
            for row in rows
        ]

    def extrema(self, asset: str, start_ms: int, end_ms: int) -> dict[str, Any]:
        """Return exact raw-sample extrema for a visible chart window."""
        result: dict[str, Any] = {}
        with self._read_lock:
            row = self._reader.execute(
                """
                SELECT MIN(long_edge_pct) AS long_min, MAX(long_edge_pct) AS long_max,
                       MIN(short_edge_pct) AS short_min, MAX(short_edge_pct) AS short_max
                FROM spread_samples
                WHERE asset = ? AND timestamp_ms >= ? AND timestamp_ms <= ?
                """,
                (asset.upper(), int(start_ms), int(end_ms)),
            ).fetchone()
            for side, column in (("long", "long_edge_pct"), ("short", "short_edge_pct")):
                side_result: dict[str, Any] = {}
                for kind in ("min", "max"):
                    value = row[f"{side}_{kind}"]
                    if value is None:
                        side_result[kind] = None
                        side_result[f"{kind}TimestampMs"] = None
                        continue
                    timestamp = self._reader.execute(
                        f"""
                        SELECT MIN(timestamp_ms) AS timestamp_ms
                        FROM spread_samples
                        WHERE asset = ? AND timestamp_ms >= ? AND timestamp_ms <= ?
                          AND {column} = ?
                        """,
                        (asset.upper(), int(start_ms), int(end_ms), value),
                    ).fetchone()["timestamp_ms"]
                    side_result[kind] = float(value)
                    side_result[f"{kind}TimestampMs"] = int(timestamp)
                result[side] = side_result
        return result

    def daily_extrema(self, asset: str, days: int = 7, *, end_ms: int | None = None) -> list[dict[str, Any]]:
        """Return exact extrema by UTC+8 calendar day, newest first."""
        end_ms = int(time.time() * 1000) if end_ms is None else int(end_ms)
        days = max(1, int(days))
        end_day = (end_ms + self._UTC8_OFFSET_MS) // self._DAY_MS
        start_day = end_day - days + 1
        start_ms = start_day * self._DAY_MS - self._UTC8_OFFSET_MS
        with self._read_lock:
            rows = self._reader.execute(
                """
                SELECT ((timestamp_ms + ?) / ?) AS day_key,
                       MIN(long_edge_pct) AS long_min, MAX(long_edge_pct) AS long_max,
                       MIN(short_edge_pct) AS short_min, MAX(short_edge_pct) AS short_max
                FROM spread_samples
                WHERE asset = ? AND timestamp_ms >= ? AND timestamp_ms <= ?
                GROUP BY day_key
                ORDER BY day_key DESC
                """,
                (self._UTC8_OFFSET_MS, self._DAY_MS, asset.upper(), start_ms, end_ms),
            ).fetchall()
        return [
            {
                "day": datetime.fromtimestamp(
                    int(row["day_key"]) * self._DAY_MS / 1000,
                    tz=timezone.utc,
                ).date().isoformat(),
                "long": {"min": row["long_min"], "max": row["long_max"]},
                "short": {"min": row["short_min"], "max": row["short_max"]},
            }
            for row in rows
        ]

    def sample_count(
        self,
        asset: str,
        window_seconds: float | None = None,
        *,
        end_ms: int | None = None,
    ) -> int:
        query = "SELECT COUNT(*) AS count FROM spread_samples WHERE asset = ?"
        params: tuple[Any, ...] = (asset.upper(),)
        if window_seconds is not None:
            window_end_ms = int(time.time() * 1000) if end_ms is None else int(end_ms)
            query += " AND timestamp_ms >= ? AND timestamp_ms <= ?"
            params = (asset.upper(), window_end_ms - int(window_seconds * 1000), window_end_ms)
        with self._read_lock:
            row = self._reader.execute(query, params).fetchone()
        return int(row["count"])

    def assets(self) -> list[str]:
        with self._read_lock:
            rows = self._reader.execute(
                "SELECT asset, MAX(timestamp_ms) AS latest FROM spread_samples GROUP BY asset ORDER BY latest DESC"
            ).fetchall()
        return [str(row["asset"]) for row in rows]

    def latest(self, asset: str) -> dict[str, Any] | None:
        with self._read_lock:
            row = self._reader.execute(
                """
                SELECT timestamp_ms, asset, var_bid, var_ask, lighter_bid, lighter_ask,
                       long_edge_pct, short_edge_pct, usdc_usdt_bid, usdc_usdt_ask,
                       usdc_usdt_received_ms
                FROM spread_samples WHERE asset = ? ORDER BY timestamp_ms DESC LIMIT 1
                """,
                (asset.upper(),),
            ).fetchone()
        if row is None:
            return None
        return {
            "timestampMs": int(row["timestamp_ms"]),
            "asset": row["asset"],
            "varBid": row["var_bid"],
            "varAsk": row["var_ask"],
            "lighterBid": row["lighter_bid"],
            "lighterAsk": row["lighter_ask"],
            "longEdge": row["long_edge_pct"],
            "shortEdge": row["short_edge_pct"],
            "usdcUsdtBid": row["usdc_usdt_bid"],
            "usdcUsdtAsk": row["usdc_usdt_ask"],
            "usdcUsdtReceivedMs": row["usdc_usdt_received_ms"],
        }

    @staticmethod
    def _float_or_none(value: Any) -> float | None:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def _side_column(side: str) -> str:
        if side == "long":
            return "long_edge_pct"
        if side == "short":
            return "short_edge_pct"
        raise ValueError(f"Unknown spread side: {side}")

    @staticmethod
    def _percentile_sorted(values: list[float], pct: float) -> float:
        if len(values) == 1:
            return values[0]
        rank = (len(values) - 1) * pct / 100.0
        lower = int(rank)
        upper = min(lower + 1, len(values) - 1)
        weight = rank - lower
        return values[lower] * (1.0 - weight) + values[upper] * weight

    @classmethod
    def _stats_from_values(cls, values: list[float]) -> tuple[float | None, float | None, float | None]:
        if not values:
            return None, None, None
        ordered = sorted(values)
        return float(median(ordered)), cls._percentile_sorted(ordered, 90), cls._percentile_sorted(ordered, 10)
