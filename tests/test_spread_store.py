import tempfile
import time
import unittest
import json
import sqlite3
import urllib.request
import threading
from pathlib import Path

from variational.spread_store import SpreadStore
from variational.spread_dashboard import SpreadDashboardServer


class SpreadStoreTest(unittest.TestCase):
    def test_old_samples_are_pruned_periodically(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SpreadStore(Path(directory) / "spreads.sqlite3")
            now_ms = int(time.time() * 1000)
            store.record(
                asset="BTC", var_bid=1, var_ask=2, lighter_bid=3, lighter_ask=4,
                long_edge_pct=5, short_edge_pct=6,
                timestamp_ms=now_ms - (store.RETENTION_SECONDS + 1) * 1000,
            )
            store.record(
                asset="BTC", var_bid=1, var_ask=2, lighter_bid=3, lighter_ask=4,
                long_edge_pct=7, short_edge_pct=8,
                timestamp_ms=now_ms,
            )
            store._last_prune_monotonic = 0
            store.record(
                asset="BTC", var_bid=1, var_ask=2, lighter_bid=3, lighter_ask=4,
                long_edge_pct=9, short_edge_pct=10,
                timestamp_ms=now_ms,
            )

            self.assertEqual(store.sample_count("BTC"), 2)
            store.close()

    def test_records_stablecoin_book_with_each_spread_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SpreadStore(Path(directory) / "spreads.sqlite3")
            store.record(
                asset="BTC", var_bid=1, var_ask=2, lighter_bid=3, lighter_ask=4,
                long_edge_pct=5, short_edge_pct=6,
                usdc_usdt_bid="0.9998", usdc_usdt_ask="0.9999",
                usdc_usdt_received_ms=123456,
            )

            latest = store.latest("BTC")

            self.assertEqual(latest["usdcUsdtBid"], 0.9998)
            self.assertEqual(latest["usdcUsdtAsk"], 0.9999)
            self.assertEqual(latest["usdcUsdtReceivedMs"], 123456)
            store.close()

    def test_existing_database_is_migrated_for_stablecoin_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spreads.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute(
                """CREATE TABLE spread_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp_ms INTEGER NOT NULL,
                    asset TEXT NOT NULL, var_bid REAL, var_ask REAL, lighter_bid REAL,
                    lighter_ask REAL, long_edge_pct REAL, short_edge_pct REAL
                )"""
            )
            connection.commit()
            connection.close()

            store = SpreadStore(path)
            store.record(
                asset="BTC", var_bid=1, var_ask=2, lighter_bid=3, lighter_ask=4,
                long_edge_pct=5, short_edge_pct=6,
                usdc_usdt_bid="0.9998", usdc_usdt_ask="0.9999",
                usdc_usdt_received_ms=123456,
            )

            self.assertEqual(store.latest("BTC")["usdcUsdtBid"], 0.9998)
            self.assertEqual(store.latest("BTC")["usdcUsdtReceivedMs"], 123456)
            store.close()
    def test_history_read_lock_does_not_block_writer(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SpreadStore(Path(directory) / "spreads.sqlite3")
            finished = threading.Event()

            def write_sample():
                store.record(asset="BTC", var_bid=1, var_ask=2, lighter_bid=3, lighter_ask=4, long_edge_pct=5, short_edge_pct=6)
                finished.set()

            with store._read_lock:
                thread = threading.Thread(target=write_sample)
                thread.start()
                self.assertTrue(finished.wait(1), "writer was blocked by a history read lock")
            thread.join()
            self.assertEqual(store.sample_count("BTC"), 1)
            store.close()

    def test_history_and_stats_survive_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spreads.sqlite3"
            now_ms = int(time.time() * 1000)
            store = SpreadStore(path)
            for index, value in enumerate((0.10, 0.20, 0.30)):
                store.record(
                    asset="BTC",
                    var_bid=65000 + index,
                    var_ask=65001 + index,
                    lighter_bid=65002 + index,
                    lighter_ask=65003 + index,
                    long_edge_pct=value,
                    short_edge_pct=-value,
                    timestamp_ms=now_ms - (2 - index) * 1000,
                )
            store.record(
                asset="XAU",
                var_bid=3000,
                var_ask=3001,
                lighter_bid=3002,
                lighter_ask=3003,
                long_edge_pct=9.9,
                short_edge_pct=-9.9,
                timestamp_ms=now_ms,
            )
            store.close()

            reopened = SpreadStore(path)
            history = reopened.history("BTC", 60)
            median_value, p90, p10 = reopened.window_stats("BTC", 60, "long")
            self.assertEqual(len(history), 3)
            self.assertAlmostEqual(median_value, 0.20)
            self.assertGreater(p90, median_value)
            self.assertLess(p10, median_value)
            self.assertTrue(all(point["longEdge"] != 9.9 for point in history))
            reopened.close()

    def test_history_can_read_a_window_ending_in_the_past(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SpreadStore(Path(directory) / "spreads.sqlite3")
            for timestamp_ms in (1_000, 2_000, 3_000, 4_000):
                store.record(asset="BTC", var_bid=1, var_ask=2, lighter_bid=3, lighter_ask=4, long_edge_pct=5, short_edge_pct=6, timestamp_ms=timestamp_ms)

            history = store.history("BTC", 2, end_ms=3_000)

            self.assertEqual([point["timestampMs"] for point in history], [1_000, 2_000, 3_000])
            self.assertEqual(store.sample_count("BTC", 2, end_ms=3_000), 3)
            store.close()

    def test_extrema_use_raw_samples_and_utc8_calendar_days(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SpreadStore(Path(directory) / "spreads.sqlite3")
            # 2026-07-22 23:00, 2026-07-23 00:00 and 01:00 in UTC+8.
            timestamps = (1784732400000, 1784736000000, 1784739600000)
            values = ((0.04, 0.07), (0.01, 0.09), (0.06, 0.03))
            for timestamp_ms, (long_edge, short_edge) in zip(timestamps, values):
                store.record(
                    asset="BTC", var_bid=1, var_ask=2, lighter_bid=3, lighter_ask=4,
                    long_edge_pct=long_edge, short_edge_pct=short_edge,
                    timestamp_ms=timestamp_ms,
                )

            visible = store.extrema("BTC", timestamps[0], timestamps[-1])
            daily = store.daily_extrema("BTC", 2, end_ms=timestamps[-1])

            self.assertEqual(visible["long"]["min"], 0.01)
            self.assertEqual(visible["long"]["minTimestampMs"], timestamps[1])
            self.assertEqual(visible["short"]["max"], 0.09)
            self.assertEqual([row["day"] for row in daily], ["2026-07-23", "2026-07-22"])
            self.assertEqual(daily[0]["long"], {"min": 0.01, "max": 0.06})
            store.close()


class SpreadDashboardTest(unittest.TestCase):
    def test_production_dashboard_has_chart_controls(self):
        html = (Path(__file__).resolve().parents[1] / "web" / "spread_dashboard.html").read_text(encoding="utf-8")
        self.assertIn('id="chart"', html)
        self.assertIn('id="assetSelect"', html)
        self.assertIn('data-range="604800"', html)
        self.assertIn("/api/history", html)
        self.assertIn("做空价差（Short Edge）", html)
        self.assertIn("quadraticCurveTo", html)
        self.assertIn('id="zoomIn"', html)
        self.assertIn('id="resetZoom"', html)
        self.assertIn('data-series="both"', html)
        self.assertIn('data-series="long"', html)
        self.assertIn('data-series="short"', html)
        self.assertIn('id="dailyExtrema"', html)
        self.assertIn("viewExtrema", html)
        self.assertIn("drawExtrema", html)
        self.assertIn('addEventListener("wheel"', html)
        self.assertIn('addEventListener("dblclick"', html)
        self.assertIn('addEventListener("mousedown"', html)
        self.assertIn('timeZone: TIME_ZONE', html)
        self.assertIn('UTC+8', html)
        self.assertIn("setTimeout(refreshLoop", html)

    def test_dashboard_crosshair_and_viewport_interactions_are_stable(self):
        html = (Path(__file__).resolve().parents[1] / "web" / "spread_dashboard.html").read_text(encoding="utf-8")

        self.assertIn("横线 Edge", html)
        self.assertIn("edgeAtY", html)
        self.assertIn("viewDurationMs = drag.duration", html)
        self.assertIn("MAX_VIEW_MS", html)
        self.assertNotIn("style.transform", html)
        self.assertIn("overflow-x: hidden", html)
        self.assertIn("@media (max-width: 1180px)", html)

    def test_dashboard_serves_html_assets_and_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            html = root / "index.html"
            html.write_text("<html>dashboard</html>", encoding="utf-8")
            store = SpreadStore(root / "spreads.sqlite3")
            store.record(asset="BTC", var_bid=100, var_ask=101, lighter_bid=102, lighter_ask=103, long_edge_pct=1, short_edge_pct=-1)
            server = SpreadDashboardServer(store, "127.0.0.1", 0, html)
            server.start()
            try:
                port = server._server.server_port
                page = urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2).read().decode()
                assets = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/assets", timeout=2))
                history = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/history?asset=BTC&range=3600", timeout=2))
                self.assertIn("dashboard", page)
                self.assertEqual(assets["assets"], ["BTC"])
                self.assertEqual(history["asset"], "BTC")
                self.assertEqual(history["sampleCount"], 1)
                self.assertEqual(history["latest"]["varAsk"], 101.0)
                self.assertEqual(history["viewExtrema"]["long"]["max"], 1.0)
                self.assertEqual(len(history["dailyExtrema"]), 1)
            finally:
                server.stop()
                store.close()
