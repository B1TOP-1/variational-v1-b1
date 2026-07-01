import asyncio
import json
import unittest
from asyncio import Future
from decimal import Decimal
from pathlib import Path

from main import parse_args
from main import OrderLifecycle
from main import VariationalToLighterRuntime
from variational.browser_order import BrowserOrderBroker, BrowserOrderCommand, BrowserOrderDispatchQueue


class BrowserOrderCommandTest(unittest.TestCase):
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

        rt._evaluate_gradient_signal(Decimal("0.2"), Decimal("0"), Decimal("0"))
        self.assertEqual(len(placed), 1)
        self.assertTrue(rt._strategy_order_in_flight)

        # 价差抖动：信号消失(指纹重置)再出现，在途期间不得再下单。
        rt._evaluate_gradient_signal(None, None, Decimal("0"))
        rt._evaluate_gradient_signal(Decimal("0.2"), Decimal("0"), Decimal("0"))
        self.assertEqual(len(placed), 1)

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
