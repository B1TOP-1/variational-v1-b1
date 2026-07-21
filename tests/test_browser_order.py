import asyncio
import json
import time
import unittest
from asyncio import Future
from decimal import Decimal
from pathlib import Path

from main import parse_args
from main import OrderLifecycle
from main import RunningStat
from main import SIGNAL_CONFIRM_MIN_QUOTES
from main import SIGNAL_CONFIRM_SECONDS
from main import SPREAD_TREND_WINDOW_SECONDS
from main import DASHBOARD_REFRESH_SECONDS
from main import VariationalToLighterRuntime
from variational.browser_order import BrowserOrderBroker, BrowserOrderCommand, BrowserOrderDispatchQueue
from variational.listener import VariationalMonitor


class BrowserOrderCommandTest(unittest.TestCase):
    def test_terminal_dashboard_refreshes_at_active_quote_frequency(self):
        self.assertEqual(DASHBOARD_REFRESH_SECONDS, 0.2)

    def test_browser_smoke_test_args_are_available(self):
        args = parse_args(["--browser-smoke-test", "--browser-smoke-qty", "0.001"])

        self.assertTrue(args.browser_smoke_test)
        self.assertEqual(args.browser_smoke_qty, "0.001")

    def test_browser_smoke_test_does_not_require_lighter_env(self):
        args = parse_args(["--browser-smoke-test"])

        runtime = VariationalToLighterRuntime(args)

        self.assertEqual(runtime.account_index, 0)
        self.assertEqual(runtime.api_key_index, 0)

    def test_builds_dry_run_browser_order_payload(self):
        command = BrowserOrderCommand(side="buy", qty=Decimal("0.001"))

        payload = command.to_payload()

        self.assertEqual(payload["side"], "buy")
        self.assertEqual(payload["qty"], "0.001")
        self.assertEqual(payload["dryRun"], True)
        self.assertEqual(payload["submitMethod"], "js_click")
        self.assertEqual(payload["waitBeforeInputMs"], 0)
        self.assertEqual(payload["waitAfterInputMs"], 3000)
        self.assertEqual(payload["disabledRetryWaitMs"], 3000)
        self.assertEqual(payload["skipInputWhenMatched"], True)
        json.dumps(payload)

    def test_builds_prepare_browser_order_payload(self):
        command = BrowserOrderCommand(side="sell", qty=Decimal("0.002"), prepare_only=True)

        self.assertEqual(command.action, "prepare_browser_order")
        payload = command.to_payload()
        self.assertEqual(payload["side"], "sell")
        self.assertEqual(payload["qty"], "0.002")
        self.assertEqual(payload["dryRun"], True)
        self.assertEqual(payload["prepareOnly"], True)

    def test_normalizes_unknown_side_to_buy(self):
        command = BrowserOrderCommand(side="invalid", qty=Decimal("0.002"))

        self.assertEqual(command.to_payload()["side"], "buy")

    def test_keeps_sell_side(self):
        command = BrowserOrderCommand(side="sell", qty=Decimal("0.003"))

        self.assertEqual(command.to_payload()["side"], "sell")

    def test_extension_allows_quote_and_order_responses(self):
        background = (Path(__file__).resolve().parents[1] / "chrome_extension" / "background.js").read_text()

        self.assertIn("https://omni.variational.io/api/quotes/indicative", background)
        self.assertIn("https://omni.variational.io/orders/new/market", background)

    def test_listener_ignores_quote_responses_rejected_by_extension_ordering(self):
        listener = (Path(__file__).resolve().parents[1] / "variational" / "listener.py").read_text()

        self.assertIn('payload.get("quoteAccepted") is False', listener)

    def test_extension_uses_submit_button_testid_selector(self):
        background = (Path(__file__).resolve().parents[1] / "chrome_extension" / "background.js").read_text()

        self.assertIn("button[data-testid='submit-button']", background)

    def test_extension_handles_read_position_action(self):
        background = (Path(__file__).resolve().parents[1] / "chrome_extension" / "background.js").read_text()

        self.assertIn('action === "read_position"', background)
        self.assertIn("当前仓位", background)

    def test_parse_dom_position_text(self):
        parse = VariationalToLighterRuntime._parse_dom_position_text

        self.assertEqual(parse("0.003 XAU"), Decimal("0.003"))
        self.assertEqual(parse("-0.01 XAU"), Decimal("-0.01"))
        self.assertEqual(parse("-"), Decimal("0"))
        self.assertEqual(parse(" - "), Decimal("0"))
        self.assertIsNone(parse(""))
        self.assertIsNone(parse(None))


class BrowserOrderBrokerTest(unittest.IsolatedAsyncioTestCase):
    async def test_listener_only_accepts_active_sequenced_quotes(self):
        monitor = VariationalMonitor()
        base_event = {
            "kind": "rest_response",
            "url": "https://omni.variational.io/api/quotes/indicative",
            "body": json.dumps({
                "instrument": {"underlying": "BTC"},
                "bid": "100",
                "ask": "101",
            }),
        }

        await monitor.process_rest_event(base_event)
        self.assertIsNone(monitor.current_quote_asset)

        await monitor.process_rest_event({
            **base_event,
            "activeQuote": {"sessionId": "session-1", "sequence": 7, "asset": "BTC"},
        })
        self.assertEqual(monitor.current_quote_asset, "BTC")
        self.assertEqual(monitor.quotes["BTC"]["quote_source"], "active_api")
        self.assertEqual(monitor.quotes["BTC"]["active_quote"]["sequence"], 7)

    async def test_place_order_cleans_pending_after_timeout(self):
        class HangingWebSocket:
            async def send(self, raw):
                self.raw = raw

        broker = BrowserOrderBroker()
        broker._websocket = HangingWebSocket()

        with self.assertRaises(asyncio.TimeoutError):
            await broker.place_order(BrowserOrderCommand(side="buy", qty=Decimal("0.001")), timeout=0.001)

        self.assertEqual(broker.pending_count(), 0)

    async def test_old_disconnect_does_not_clear_new_connection_pending(self):
        class WebSocket:
            def __init__(self, on_next=None):
                self.on_next = on_next

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.on_next is not None:
                    self.on_next()
                    self.on_next = None
                raise StopAsyncIteration

        broker = BrowserOrderBroker()
        new_ws = WebSocket()
        future: Future[dict[str, object]] = Future()
        broker._pending["new"] = future
        old_ws = WebSocket(lambda: setattr(broker, "_websocket", new_ws))

        await broker.on_connect(old_ws)

        self.assertIs(broker._websocket, new_ws)
        self.assertEqual(broker.pending_count(), 1)
        self.assertFalse(future.done())

    async def test_read_position_sends_action_and_returns_response(self):
        class CapturingWebSocket:
            def __init__(self, broker):
                self.broker = broker
                self.sent = None

            async def send(self, raw):
                self.sent = json.loads(raw)
                message_id = self.sent["id"]
                self.broker._pending[message_id].set_result(
                    {"id": message_id, "ok": True, "found": True, "valueText": "0.003 XAU"}
                )

        broker = BrowserOrderBroker()
        broker._websocket = CapturingWebSocket(broker)

        result = await broker.read_position(timeout=1.0)

        self.assertEqual(broker._websocket.sent["action"], "read_position")
        self.assertEqual(result["valueText"], "0.003 XAU")
        self.assertEqual(broker.pending_count(), 0)

    async def test_dispatch_queue_runs_submitted_items_in_order(self):
        handled: list[str] = []

        async def handler(item: str) -> None:
            handled.append(item)

        queue = BrowserOrderDispatchQueue(handler)
        queue.start()
        queue.submit("first")
        queue.submit("second")
        await queue.join()
        await queue.stop()

        self.assertEqual(handled, ["first", "second"])


class StrategyLoopTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _runtime():
        return VariationalToLighterRuntime(parse_args(["--browser-smoke-test"]))

    def test_binance_usdc_book_is_observation_only(self):
        rt = self._runtime()

        self.assertTrue(rt._update_binance_usdc_book({
            "s": "USDCUSDT", "b": "0.99980", "a": "0.99990",
        }))
        self.assertEqual(rt.binance_usdc_bid, Decimal("0.99980"))
        self.assertEqual(rt.binance_usdc_ask, Decimal("0.99990"))
        self.assertEqual(rt.binance_usdc_status, "connected")
        self.assertFalse(rt._update_binance_usdc_book({
            "s": "USDCUSDT", "b": "1.1", "a": "1.0",
        }))

        source = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
        self.assertIn("usdcusdt@bookTicker", source)
        self.assertIn("仅观察，不参与下单", source)

    @staticmethod
    def _confirm_signal(rt, long_edge=Decimal("0.2"), short_edge=Decimal("0"), position=Decimal("0")):
        start = 100.0
        result = None
        for index in range(SIGNAL_CONFIRM_MIN_QUOTES):
            result = rt._evaluate_gradient_signal(
                long_edge,
                short_edge,
                position,
                active_quote_key=("session-test", index + 1),
                now_monotonic=start + index * (SIGNAL_CONFIRM_SECONDS / (SIGNAL_CONFIRM_MIN_QUOTES - 1)),
            )
        return result

    async def test_eval_skips_and_reads_nothing_without_cached_position(self):
        rt = self._runtime()
        rt._cached_position_qty = None
        called = {"spreads": False}

        async def boom():
            called["spreads"] = True
            return (None, None)

        rt._compute_signal_spreads = boom
        await rt._run_strategy_evaluation()

        self.assertIsNone(rt._latest_gradient_signal)
        self.assertFalse(called["spreads"])  # 无缓存仓位：不评估、不读行情

    async def test_eval_uses_cached_position(self):
        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.1")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.01")

        async def spreads():
            return (Decimal("0.2"), Decimal("0"))

        rt._compute_signal_spreads = spreads
        await rt._run_strategy_evaluation()

        self.assertIsNotNone(rt._latest_gradient_signal)
        self.assertEqual(rt._latest_gradient_signal.action, "open")

    async def test_short_gradient_uses_short_edge_directly(self):
        rt = self._runtime()
        rt._cached_position_qty = Decimal("0.01")
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.close_rows[0].threshold_pct = Decimal("0.20")
        rt.gradient_strategy.close_rows[0].target_qty = Decimal("0")

        async def spreads():
            return (Decimal("0.10"), Decimal("0.18"))

        rt._compute_signal_spreads = spreads
        await rt._run_strategy_evaluation()

        self.assertIsNotNone(rt._latest_gradient_signal)
        self.assertEqual(rt._latest_gradient_signal.action, "close")
        self.assertEqual(rt._latest_gradient_signal.spread_pct, Decimal("0.18"))

    async def test_refresh_cache_uses_listener_without_dom(self):
        rt = self._runtime()

        async def reader():
            return Decimal("-0.05")

        async def dom_boom():
            raise AssertionError("DOM should not be hit when listener has data")

        rt._read_listener_position = reader
        rt._read_dom_position_qty = dom_boom
        await rt._refresh_position_cache(allow_dom=False)

        self.assertEqual(rt._cached_position_qty, Decimal("-0.05"))

    async def test_in_flight_blocks_duplicate_order_on_spread_flap(self):
        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")  # 两腿平衡，放行平衡闸
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.1")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.05")
        placed = []
        rt._handle_new_gradient_signal = lambda sig: placed.append(sig) or "rec"

        # 未连续满 1 秒时不下单。
        for index in range(SIGNAL_CONFIRM_MIN_QUOTES):
            rt._evaluate_gradient_signal(
                Decimal("0.2"),
                Decimal("0"),
                Decimal("0"),
                active_quote_key=("session-test", index + 1),
                now_monotonic=100.0 + index * 0.2,
            )
        self.assertEqual(len(placed), 0)
        # 连续满 1 秒且覆盖至少 4 个不同序号后下单。
        rt._evaluate_gradient_signal(
            Decimal("0.2"), Decimal("0"), Decimal("0"),
            active_quote_key=("session-test", SIGNAL_CONFIRM_MIN_QUOTES + 1),
            now_monotonic=101.0,
        )
        self.assertEqual(len(placed), 1)
        self.assertTrue(rt._strategy_order_in_flight)

        # 价差抖动：信号消失(指纹重置)再出现，在途期间不得再下单。
        rt._evaluate_gradient_signal(None, None, Decimal("0"))
        rt._evaluate_gradient_signal(Decimal("0.2"), Decimal("0"), Decimal("0"))
        rt._evaluate_gradient_signal(Decimal("0.2"), Decimal("0"), Decimal("0"))
        self.assertEqual(len(placed), 1)

    async def test_spike_far_above_recent_average_is_rejected(self):
        import main as main_mod

        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.1")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.05")
        placed = []
        rt._handle_new_gradient_signal = lambda sig: placed.append(sig) or "rec"

        rt.variational_ticker = "XAU"
        for _ in range(30):
            rt.spread_store.record(asset="XAU", var_bid=None, var_ask=None, lighter_bid=None, lighter_ask=None, long_edge_pct=0.07, short_edge_pct=0.0)
        original = main_mod.MAX_SPIKE_DEVIATION_PCT
        main_mod.MAX_SPIKE_DEVIATION_PCT = Decimal("0.02")
        try:
            # 触发 0.12 远超均值 0.07(偏离0.05>0.02) → 尖峰，多次也不下单
            for _ in range(3):
                rt._evaluate_gradient_signal(Decimal("0.12"), Decimal("0"), Decimal("0"))
        finally:
            main_mod.MAX_SPIKE_DEVIATION_PCT = original
        self.assertEqual(len(placed), 0)

    async def test_signal_near_recent_average_passes_spike_filter(self):
        import main as main_mod

        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.1")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.05")
        placed = []
        rt._handle_new_gradient_signal = lambda sig: placed.append(sig) or "rec"

        rt.variational_ticker = "XAU"
        for _ in range(30):
            rt.spread_store.record(asset="XAU", var_bid=None, var_ask=None, lighter_bid=None, lighter_ask=None, long_edge_pct=0.115, short_edge_pct=0.0)
        original = main_mod.MAX_SPIKE_DEVIATION_PCT
        main_mod.MAX_SPIKE_DEVIATION_PCT = Decimal("0.02")
        try:
            # 触发 0.12，偏离 0.005 <= 0.02 → 连续确认满 1 秒后下单。
            self._confirm_signal(rt, long_edge=Decimal("0.12"))
        finally:
            main_mod.MAX_SPIKE_DEVIATION_PCT = original
        self.assertEqual(len(placed), 1)

    async def test_single_tick_signal_is_treated_as_noise(self):
        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.1")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.05")
        placed = []
        rt._handle_new_gradient_signal = lambda sig: placed.append(sig) or "rec"

        # 信号出现→消失(噪音)→再出现：中断使连续计数清零
        rt._evaluate_gradient_signal(
            Decimal("0.2"), Decimal("0"), Decimal("0"),
            active_quote_key=("session-test", 1), now_monotonic=100.0,
        )
        rt._evaluate_gradient_signal(None, None, Decimal("0"), now_monotonic=100.4)
        rt._evaluate_gradient_signal(
            Decimal("0.2"), Decimal("0"), Decimal("0"),
            active_quote_key=("session-test", 2), now_monotonic=100.6,
        )
        self.assertEqual(len(placed), 0)
        # 旧的 100.0 命中不能累计；从 100.6 重新连续满 1 秒才下单。
        for index, now in enumerate((100.9, 101.2, 101.6), start=3):
            rt._evaluate_gradient_signal(
                Decimal("0.2"), Decimal("0"), Decimal("0"),
                active_quote_key=("session-test", index), now_monotonic=now,
            )
        self.assertEqual(len(placed), 1)

    async def test_repeated_active_quote_sequence_cannot_satisfy_confirmation(self):
        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.1")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.05")
        placed = []
        rt._handle_new_gradient_signal = lambda sig: placed.append(sig) or "rec"

        for now in (100.0, 100.4, 100.8, 101.2):
            rt._evaluate_gradient_signal(
                Decimal("0.2"), Decimal("0"), Decimal("0"),
                active_quote_key=("session-test", 1), now_monotonic=now,
            )

        self.assertEqual(len(placed), 0)

    async def test_pre_dispatch_recheck_cancels_reverted_signal(self):
        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.1")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.05")
        initial = rt.gradient_strategy.evaluate(Decimal("0.2"), Decimal("0"), Decimal("0"))
        self.assertIsNotNone(initial)
        rt._pending_signal_sig = initial.signature()
        rt._pending_signal_started_at = time.monotonic() - SIGNAL_CONFIRM_SECONDS
        rt._pending_signal_quote_keys = {("session-test", 1), ("session-test", 2), ("session-test", 3)}
        rt.runtime.monitor.current_quote_asset = "XAU"
        rt.runtime.monitor.quotes["XAU"] = {
            "active_quote": {"sessionId": "session-test", "sequence": 4},
        }
        spreads = [(Decimal("0.2"), Decimal("0")), (Decimal("0.05"), Decimal("0"))]

        async def compute_spreads():
            return spreads.pop(0)

        rt._compute_signal_spreads = compute_spreads
        placed = []
        rt._handle_new_gradient_signal = lambda sig: placed.append(sig) or "rec"

        await rt._run_strategy_evaluation()

        self.assertEqual(placed, [])
        self.assertIsNone(rt._pending_signal_sig)

    async def test_refresh_after_fill_waits_for_position_change(self):
        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")
        seq = [Decimal("0"), Decimal("0.01")]

        async def reader():
            return seq.pop(0) if seq else Decimal("0.01")

        rt._read_listener_position = reader
        await rt._refresh_position_cache_after_fill(Decimal("0"))

        self.assertEqual(rt._cached_position_qty, Decimal("0.01"))


class HedgeLegTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _runtime():
        return VariationalToLighterRuntime(parse_args(["--browser-smoke-test"]))

    def test_window_stats_single_pass(self):
        rt = self._runtime()
        rt.variational_ticker = "BTC"
        for v in (0.05, 0.06, 0.07, 0.08, 0.09):
            rt.spread_store.record(asset="BTC", var_bid=None, var_ask=None, lighter_bid=None, lighter_ask=None, long_edge_pct=v, short_edge_pct=None)
        med, p90, p10 = rt._window_stats(3600, long_side=True)
        self.assertEqual(med, 0.07)
        self.assertEqual(p90, rt._percentile([0.05, 0.06, 0.07, 0.08, 0.09], 90))
        self.assertEqual(p10, rt._percentile([0.05, 0.06, 0.07, 0.08, 0.09], 10))

    def test_refresh_spread_stats_populates_cache(self):
        rt = self._runtime()
        rt.variational_ticker = "BTC"
        rt.spread_store.record(asset="BTC", var_bid=None, var_ask=None, lighter_bid=None, lighter_ask=None, long_edge_pct=0.07, short_edge_pct=0.03)
        rt._refresh_spread_stats()
        self.assertEqual(rt._spread_stats_cache["long_median_5m"], 0.07)
        self.assertEqual(rt._spread_stats_cache["short_median_1h"], 0.03)

    def test_spread_trend_series_and_line_chart(self):
        rt = self._runtime()
        rt.variational_ticker = "BTC"
        w = SPREAD_TREND_WINDOW_SECONDS
        now_ms = int(time.time() * 1000)
        for age, long_edge, short_edge in ((w - 10, 0.05, -0.05), (w / 2, 0.07, -0.07), (10, 0.09, -0.09)):
            rt.spread_store.record(asset="BTC", var_bid=None, var_ask=None, lighter_bid=None, lighter_ask=None, long_edge_pct=long_edge, short_edge_pct=short_edge, timestamp_ms=now_ms - int(age * 1000))
        vals, lo, hi = rt._spread_trend_series(0, 10, time.monotonic())
        self.assertEqual(len(vals), 10)
        self.assertEqual((lo, hi), (0.05, 0.09))
        self.assertEqual(vals[0], 0.05)
        self.assertEqual(vals[-1], 0.09)
        self.assertIsNone(vals[1])       # 空桶
        chart = rt._ascii_line_chart(vals, 7)
        self.assertGreaterEqual(len(chart), 2)              # 行数随整齐刻度而定
        self.assertTrue(all("┤" in line for line in chart))  # 每行都带刻度标签

    def test_spread_samples_are_all_persisted_without_memory_throttle(self):
        rt = self._runtime()
        rt.variational_ticker = "BTC"
        rt._record_cross_spreads("BTC", None, None, None, None, Decimal("0.05"), Decimal("-0.05"))
        rt._record_cross_spreads("BTC", None, None, None, None, Decimal("0.06"), Decimal("-0.06"))
        self.assertEqual(rt.spread_store.sample_count("BTC", 60), 2)

    def test_running_stat_avg(self):
        s = RunningStat()
        self.assertIsNone(s.avg())
        s.add(2.0)
        s.add(4.0)
        self.assertEqual(s.avg(), 3.0)
        self.assertEqual(s.last, 4.0)
        self.assertEqual(s.n, 2)

    def test_slippage_stats_accumulate_once(self):
        rt = self._runtime()
        rec = OrderLifecycle(
            trade_key="strategy:x",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="XAU",
            auto_hedge_enabled=True,
            last_variational_status="filled",
            lighter_side="SELL",
            var_trigger_price=Decimal("100"),
            var_fill_price=Decimal("98"),
            dom_trigger_price=Decimal("99"),
            lighter_trigger_price=Decimal("100"),
            lighter_fill_price=Decimal("98"),
        )
        rt._maybe_record_slippage_stats(rec)
        rt._maybe_record_slippage_stats(rec)  # 去重：只记一次
        self.assertEqual(rt._stat_both_filled, 1)
        self.assertEqual(rt._stat_var_slip.n, 1)
        self.assertEqual(rt._stat_var_slip.last, 2.0)  # 做多 对api100→98 = +2%
        self.assertEqual(rt._stat_var_slip_dom.n, 1)   # dom 口径也记一次
        self.assertEqual(rt._stat_var_slip_dom.last, float((Decimal("99") - Decimal("98")) / Decimal("99") * 100))
        self.assertEqual(rt._stat_lighter_slip.last, -2.0)  # 做空 100→98 = -2%

    def test_quantize_to_lighter_lot_floors_to_step(self):
        rt = self._runtime()
        rt.base_amount_multiplier = 1000  # 最小步长 0.001
        self.assertEqual(rt._quantize_to_lighter_lot(Decimal("0.0015")), Decimal("0.001"))
        self.assertEqual(rt._quantize_to_lighter_lot(Decimal("0.012")), Decimal("0.012"))
        self.assertEqual(rt._quantize_to_lighter_lot(Decimal("0.0005")), Decimal("0"))

    def test_quantize_passthrough_without_multiplier(self):
        rt = self._runtime()
        rt.base_amount_multiplier = 0
        self.assertEqual(rt._quantize_to_lighter_lot(Decimal("0.0015")), Decimal("0.0015"))

    async def test_lighter_fill_pops_mapping(self):
        rt = self._runtime()
        key = "strategy:test"
        rt.records[key] = OrderLifecycle(
            trade_key=key,
            trade_id="",
            side="buy",
            qty=Decimal("0.01"),
            asset="XAU",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
        )
        rt.record_order.append(key)
        rt.lighter_client_order_to_trade_key[123] = key

        await rt.handle_lighter_fill_update(
            {
                "status": "filled",
                "client_order_id": 123,
                "filled_quote_amount": "40",
                "filled_base_amount": "0.01",
            }
        )

        self.assertNotIn(123, rt.lighter_client_order_to_trade_key)
        self.assertEqual(rt.records[key].lighter_fill_price, Decimal("4000"))

    async def test_lighter_latency_recorded_from_signal_trigger(self):
        rt = self._runtime()
        key = "strategy:lat"
        rt.records[key] = OrderLifecycle(
            trade_key=key,
            trade_id="",
            side="buy",
            qty=Decimal("0.01"),
            asset="XAU",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
            signal_trigger_monotonic=time.monotonic() - 0.05,  # 50ms 前触发
        )
        rt.record_order.append(key)
        rt.lighter_client_order_to_trade_key[7] = key

        await rt.handle_lighter_fill_update(
            {"status": "filled", "client_order_id": 7, "filled_quote_amount": "40", "filled_base_amount": "0.01"}
        )

        self.assertEqual(rt._stat_lighter_latency.n, 1)
        self.assertGreaterEqual(rt._stat_lighter_latency.last, 40.0)  # ~50ms 端到端

    def test_epoch_ms_parses_seconds_and_millis(self):
        self.assertIsNone(VariationalToLighterRuntime._epoch_ms(None))
        self.assertEqual(VariationalToLighterRuntime._epoch_ms(1700000000000), 1700000000000.0)
        self.assertEqual(VariationalToLighterRuntime._epoch_ms(1700000000), 1700000000000.0)

    def test_runtime_feeds_quote_comparator(self):
        rt = self._runtime()
        rt.variational_ticker = "XAU"
        rt._on_api_quote("XAU", "100", "101", None)          # api 先出
        rt._on_dom_quote({"bid": "100", "ask": "101", "ts": None})  # dom 后出 → 匹配
        snap = rt.quote_comparator.snapshot()
        self.assertEqual(snap["transitions"]["api"], 1)
        self.assertEqual(snap["transitions"]["dom"], 1)
        self.assertEqual(snap["matched"], 1)

    def test_active_api_quote_wakes_strategy_immediately(self):
        rt = self._runtime()
        rt._strategy_wake.clear()

        rt._on_api_quote("XAU", "100", "101", None)

        self.assertTrue(rt._strategy_wake.is_set())

    def test_var_quote_disconnect_gate(self):
        rt = self._runtime()
        self.assertFalse(rt._var_quote_disconnected())  # 从未收到 → 不算断线
        rt.quote_comparator._last_acquire_ms["api"] = time.monotonic() * 1000.0 - 5000.0
        self.assertTrue(rt._var_quote_disconnected())   # api 5s 未刷新 → 断线
        rt.quote_comparator._last_acquire_ms["api"] = time.monotonic() * 1000.0
        self.assertFalse(rt._var_quote_disconnected())  # 刚收到 → 恢复

    def test_disconnect_blocks_strategy_orders(self):
        rt = self._runtime()
        rt.args.auto_hedge = False  # 隔离平衡闸
        rt.quote_comparator._last_acquire_ms["dom"] = time.monotonic() * 1000.0 - 4000.0
        self.assertTrue(rt._strategy_order_allowed())  # DOM 仅用于对比，不阻断主动 API
        rt.quote_comparator._last_acquire_ms["api"] = time.monotonic() * 1000.0 - 4000.0
        self.assertFalse(rt._strategy_order_allowed())

    def test_dom_transport_delay_recorded(self):
        rt = self._runtime()
        ts_ms = time.time() * 1000.0 - 30.0  # 浏览器事件 30ms 前
        rt._on_dom_quote({"bid": "100", "ask": "101", "ts": ts_ms})
        delay = rt._last_quote_delay["dom"]
        self.assertIsNotNone(delay)
        self.assertGreaterEqual(delay, 25.0)
        self.assertLess(delay, 300.0)

    def test_api_quote_ignored_for_other_asset(self):
        rt = self._runtime()
        rt.variational_ticker = "XAU"
        rt._on_api_quote("BTC", "100", "101", None)  # 非活跃标的，忽略
        self.assertEqual(rt.quote_comparator.snapshot()["transitions"]["api"], 0)

    def test_parse_lighter_positions_applies_sign(self):
        parsed = VariationalToLighterRuntime._parse_lighter_positions(
            {"positions": {"1": {"symbol": "BTC", "sign": -1, "position": "0.004"}}}
        )
        self.assertEqual(parsed["BTC"], Decimal("-0.004"))

    def test_positions_balanced_requires_opposite_legs(self):
        rt = self._runtime()
        rt.base_amount_multiplier = 1000  # 容差 0.001
        rt.ticker = "BTC"
        rt._cached_position_qty = Decimal("0.01")  # Var 多
        rt._lighter_positions = {"BTC": Decimal("-0.01")}  # Lighter 空
        self.assertTrue(rt._positions_balanced())
        rt._lighter_positions = {"BTC": Decimal("0")}  # 裸腿
        self.assertFalse(rt._positions_balanced())

    def test_strategy_order_allowed_gate(self):
        rt = self._runtime()
        rt.base_amount_multiplier = 1000
        rt.ticker = "BTC"
        rt.args.auto_hedge = True
        rt._cached_position_qty = Decimal("0.01")
        rt._lighter_positions = {"BTC": Decimal("-0.01")}
        self.assertTrue(rt._strategy_order_allowed())  # 平衡
        rt._strategy_halted = True
        self.assertFalse(rt._strategy_order_allowed())  # 已停止
        rt._strategy_halted = False
        rt._lighter_positions = {"BTC": Decimal("0")}
        self.assertFalse(rt._strategy_order_allowed())  # 不平衡

    async def test_confirm_hedge_halts_on_timeout(self):
        import main as main_mod

        rt = self._runtime()
        rt.base_amount_multiplier = 1000
        rt.ticker = "BTC"
        rt.args.auto_hedge = True
        rt._lighter_position_ready.set()  # 跳过 REST 兜底
        rt._cached_position_qty = Decimal("0.01")
        rt._lighter_positions = {"BTC": Decimal("0")}  # 裸腿，永不平衡

        async def reader():
            return Decimal("0.01")

        rt._read_listener_position = reader
        original = main_mod.POSITION_BALANCE_TIMEOUT_SECONDS
        main_mod.POSITION_BALANCE_TIMEOUT_SECONDS = 0.2
        try:
            await rt._confirm_hedge_or_halt(Decimal("0"))
        finally:
            main_mod.POSITION_BALANCE_TIMEOUT_SECONDS = original

        self.assertTrue(rt._strategy_halted)

    async def test_warm_lighter_skips_without_hedge(self):
        rt = self._runtime()
        rt.args.auto_hedge = False
        await rt.warm_lighter()
        self.assertFalse(rt._lighter_ready)

    async def test_warm_lighter_sets_ready_on_success(self):
        rt = self._runtime()
        rt.args.auto_hedge = True
        rt.initialize_lighter_client = lambda: None
        rt._rest_get_lighter_account = lambda: {}
        await rt.warm_lighter()
        self.assertTrue(rt._lighter_ready)

    async def test_confirm_hedge_ok_when_balanced(self):
        rt = self._runtime()
        rt.base_amount_multiplier = 1000
        rt.ticker = "BTC"
        rt.args.auto_hedge = True
        rt._lighter_position_ready.set()
        rt._cached_position_qty = Decimal("0.01")
        rt._lighter_positions = {"BTC": Decimal("-0.01")}  # 已平衡

        async def reader():
            return Decimal("0.01")

        rt._read_listener_position = reader
        await rt._confirm_hedge_or_halt(Decimal("0"))

        self.assertFalse(rt._strategy_halted)


if __name__ == "__main__":
    unittest.main()
