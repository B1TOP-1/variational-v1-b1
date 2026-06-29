import unittest
import time
import asyncio
import argparse
from decimal import Decimal

from main import (
    BrowserOrderCommand,
    OrderLifecycle,
    PENDING_TRIGGER_SPREAD_TTL_SECONDS,
    PendingTriggerSpread,
    VariationalToLighterRuntime,
    cross_spread_percentages,
)
from variational.gradient_strategy import GradientSignal, GradientStrategyState
from variational.gradient_strategy import StrategySection


class SpreadMathTest(unittest.TestCase):
    def test_cross_spread_uses_raw_lighter_price_without_stablecoin_normalization(self):
        var_ask = Decimal("100")
        var_bid = Decimal("99")
        lighter_bid = Decimal("101")
        lighter_ask = Decimal("102")

        long_pct, short_pct = cross_spread_percentages(var_bid, var_ask, lighter_bid, lighter_ask)

        self.assertEqual(long_pct, Decimal("1.00"))
        self.assertEqual(short_pct, Decimal("-2.941176470588235294117647059"))

    def test_fill_diff_uses_raw_lighter_price_without_stablecoin_normalization(self):
        diff, pct = VariationalToLighterRuntime._fill_diff_by_direction(
            "buy",
            Decimal("100"),
            Decimal("101"),
            Decimal("1.10"),
        )

        self.assertEqual(diff, Decimal("1"))
        self.assertEqual(pct, Decimal("1.00"))

    def test_unit_spread_uses_raw_lighter_price_without_stablecoin_normalization(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.usdc_usdt_rate = Decimal("1.10")
        record = OrderLifecycle(
            trade_key="trade",
            trade_id="trade",
            side="buy",
            qty=Decimal("1"),
            asset="BTC",
            auto_hedge_enabled=False,
            last_variational_status="filled",
            var_fill_price=Decimal("100"),
            lighter_fill_price=Decimal("101"),
            fill_usdc_usdt_rate=Decimal("1.10"),
        )

        self.assertEqual(runtime._record_unit_spread(record), Decimal("1"))

    def test_spread_slippage_is_actual_fill_pct_minus_trigger_pct(self):
        record = OrderLifecycle(
            trade_key="trade",
            trade_id="trade",
            side="buy",
            qty=Decimal("1"),
            asset="BTC",
            auto_hedge_enabled=False,
            last_variational_status="filled",
            var_fill_price=Decimal("100"),
            lighter_fill_price=Decimal("100.011"),
            trigger_spread_pct=Decimal("0.0100"),
        )

        self.assertEqual(record.spread_slippage_pct(), Decimal("0.00100"))

    def test_pending_trigger_spread_binds_by_side_fifo(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        now = time.monotonic()
        runtime._pending_trigger_spreads = [
            PendingTriggerSpread(side="buy", spread_pct=Decimal("0.0125"), created_at_monotonic=now),
            PendingTriggerSpread(side="sell", spread_pct=Decimal("0.0300"), created_at_monotonic=now),
        ]

        self.assertEqual(runtime._consume_pending_trigger_spread("buy"), Decimal("0.0125"))
        self.assertEqual(len(runtime._pending_trigger_spreads), 1)
        self.assertEqual(runtime._pending_trigger_spreads[0].side, "sell")

    def test_expired_pending_trigger_spread_is_not_bound(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime._pending_trigger_spreads = [
            PendingTriggerSpread(
                side="buy",
                spread_pct=Decimal("0.0125"),
                created_at_monotonic=time.monotonic() - PENDING_TRIGGER_SPREAD_TTL_SECONDS - 1,
            ),
        ]

        self.assertIsNone(runtime._consume_pending_trigger_spread("buy"))
        self.assertEqual(runtime._pending_trigger_spreads, [])

    def test_dry_run_gradient_signal_does_not_create_bindable_pending_spread(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime._pending_trigger_spreads = []

        runtime._record_dry_run_trigger_spread(side="buy", spread_pct=Decimal("0.0125"))

        self.assertEqual(runtime._pending_trigger_spreads, [])

    def test_live_trigger_spread_can_be_bound_later(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime._pending_trigger_spreads = []

        runtime._record_live_trigger_spread(side="buy", spread_pct=Decimal("0.0125"))

        self.assertEqual(runtime._consume_pending_trigger_spread("buy"), Decimal("0.0125"))

    def test_prepare_browser_order_uses_single_order_qty(self):
        class Queue:
            def __init__(self):
                self.items = []

            def submit(self, item):
                self.items.append(item)

        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.gradient_strategy = GradientStrategyState.default()
        runtime._prepared_order_side = "buy"
        runtime._last_prepared_order_sig = None
        runtime._browser_order_queue = Queue()

        runtime._schedule_prepare_browser_order()

        self.assertEqual(len(runtime._browser_order_queue.items), 1)
        command = runtime._browser_order_queue.items[0]
        self.assertIsInstance(command, BrowserOrderCommand)
        self.assertEqual(command.side, "buy")
        self.assertEqual(command.qty, Decimal("0.001"))
        self.assertTrue(command.prepare_only)

    def test_prepare_browser_order_keeps_current_panel_side_when_cursor_moves_to_close_section(self):
        class Queue:
            def __init__(self):
                self.items = []

            def submit(self, item):
                self.items.append(item)

        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.gradient_strategy = GradientStrategyState.default()
        runtime.gradient_strategy.cursor_section = StrategySection.CLOSE
        runtime._prepared_order_side = "buy"
        runtime._last_prepared_order_sig = None
        runtime._browser_order_queue = Queue()

        runtime._schedule_prepare_browser_order()

        command = runtime._browser_order_queue.items[0]
        self.assertEqual(command.side, "buy")

    def test_prepare_browser_order_dedupes_only_after_success(self):
        class Queue:
            def __init__(self):
                self.items = []

            def submit(self, item):
                self.items.append(item)

        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.gradient_strategy = GradientStrategyState.default()
        runtime._prepared_order_side = "buy"
        runtime._last_prepared_order_sig = None
        runtime._browser_order_queue = Queue()

        runtime._schedule_prepare_browser_order()
        runtime._schedule_prepare_browser_order()

        self.assertEqual(len(runtime._browser_order_queue.items), 2)

        runtime._last_prepared_order_sig = ("buy", "0.001")
        runtime._schedule_prepare_browser_order()

        self.assertEqual(len(runtime._browser_order_queue.items), 2)

    def test_gradient_signal_creates_strategy_record_and_live_browser_order(self):
        class Queue:
            def __init__(self):
                self.items = []

            def submit(self, item):
                self.items.append(item)

        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.args = argparse.Namespace(auto_hedge=False)
        runtime.gradient_strategy = GradientStrategyState.default()
        runtime.gradient_strategy.single_order_qty = Decimal("0.001")
        runtime.variational_ticker = "BTC"
        runtime.records = {}
        runtime.record_order = []
        runtime._pending_variational_strategy_order_keys = []
        runtime._browser_order_queue = Queue()
        runtime.logger = None

        signal = GradientSignal(
            action="open",
            section=StrategySection.OPEN,
            spread_pct=Decimal("0.0200"),
            threshold_pct=Decimal("0.0100"),
            target_qty=Decimal("0.003"),
            current_qty=Decimal("0.001"),
            delta_qty=Decimal("0.002"),
        )

        record = runtime._handle_new_gradient_signal(signal)

        self.assertTrue(record.trade_key.startswith("strategy:"))
        self.assertEqual(record.side, "buy")
        self.assertEqual(record.lighter_side, "SELL")
        self.assertEqual(record.qty, Decimal("0.001"))
        self.assertEqual(record.trigger_spread_pct, Decimal("0.0200"))
        self.assertEqual(record.strategy_action, "open")
        self.assertEqual(record.strategy_target_qty, Decimal("0.003"))
        self.assertEqual(record.strategy_current_qty, Decimal("0.001"))
        self.assertEqual(runtime.records[record.trade_key], record)
        self.assertEqual(runtime.record_order, [record.trade_key])
        self.assertEqual(runtime._pending_variational_strategy_order_keys, [record.trade_key])

        self.assertEqual(len(runtime._browser_order_queue.items), 1)
        command = runtime._browser_order_queue.items[0]
        self.assertIsInstance(command, BrowserOrderCommand)
        self.assertEqual(command.side, "buy")
        self.assertEqual(command.qty, Decimal("0.001"))
        self.assertFalse(command.dry_run)

    def test_close_signal_sets_lighter_buy_side(self):
        class Queue:
            def __init__(self):
                self.items = []

            def submit(self, item):
                self.items.append(item)

        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.args = argparse.Namespace(auto_hedge=False)
        runtime.gradient_strategy = GradientStrategyState.default()
        runtime.gradient_strategy.single_order_qty = Decimal("0.005")
        runtime.variational_ticker = "BTC"
        runtime.records = {}
        runtime.record_order = []
        runtime._pending_variational_strategy_order_keys = []
        runtime._browser_order_queue = Queue()
        runtime.logger = None

        signal = GradientSignal(
            action="close",
            section=StrategySection.CLOSE,
            spread_pct=Decimal("0.0000"),
            threshold_pct=Decimal("0.0100"),
            target_qty=Decimal("0"),
            current_qty=Decimal("0.003"),
            delta_qty=Decimal("0.003"),
        )

        record = runtime._handle_new_gradient_signal(signal)

        self.assertEqual(record.side, "sell")
        self.assertEqual(record.lighter_side, "BUY")
        self.assertEqual(record.qty, Decimal("0.003"))

    def test_no_hedge_lighter_ws_url_is_readonly(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.args = argparse.Namespace(auto_hedge=False)

        self.assertEqual(runtime.build_lighter_ws_url(), "wss://mainnet.zklighter.elliot.ai/stream?readonly=true")


class StrategyOrderAsyncTest(unittest.IsolatedAsyncioTestCase):
    async def test_browser_submit_result_updates_strategy_record(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.records = {}
        runtime.logged = []

        record = OrderLifecycle(
            trade_key="strategy:submit",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="XAU",
            auto_hedge_enabled=False,
            last_variational_status="strategy_submitted",
        )
        runtime.records[record.trade_key] = record
        runtime._record_lock = asyncio.Lock()

        async def append_order_log(event_type, payload):
            runtime.logged.append((event_type, payload))

        runtime.append_order_log = append_order_log

        result = {
            "ok": True,
            "clickStartedAt": "2026-06-29T01:02:03.000Z",
            "clickStartedAtMs": 1782694923000,
            "timing": {"totalDuration": 88.5},
            "lastQuote": {"bid": 4058.45, "ask": 4058.98, "quoteId": "q1"},
            "orderResponse": {
                "status": 200,
                "json": {"id": "var-order-1"},
                "capturedAt": "2026-06-29T01:02:03.090Z",
            },
        }

        await runtime._record_var_submit_result(record.trade_key, result)

        self.assertEqual(record.var_submit_ok, True)
        self.assertEqual(record.var_submit_order_id, "var-order-1")
        self.assertEqual(record.var_submit_status, 200)
        self.assertEqual(record.var_submit_quote_snapshot["quoteId"], "q1")
        self.assertEqual(record.var_submit_timing["totalDuration"], 88.5)
        self.assertEqual(runtime.logged[0][0], "variational_order_submitted")

    async def test_no_hedge_activate_asset_uses_market_config_and_readonly_ws(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.args = argparse.Namespace(auto_hedge=False)
        runtime.variational_ticker = None
        runtime.ticker = None
        runtime._asset_switch_lock = asyncio.Lock()
        runtime.lighter_ws_task = None
        runtime.calls = []

        def get_lighter_market_config():
            runtime.calls.append("get_lighter_market_config")
            return 92, 1000, 100

        async def handle_lighter_ws():
            runtime.calls.append("handle_lighter_ws")

        async def wait_for_lighter_order_book_ready():
            await asyncio.sleep(0)
            runtime.calls.append("wait_for_lighter_order_book_ready")

        async def reset_lighter_order_book():
            runtime.calls.append("reset_lighter_order_book")

        async def reset_state_for_asset_switch():
            runtime.calls.append("reset_state_for_asset_switch")

        runtime.get_lighter_market_config = get_lighter_market_config
        runtime.handle_lighter_ws = handle_lighter_ws
        runtime.wait_for_lighter_order_book_ready = wait_for_lighter_order_book_ready
        runtime.reset_lighter_order_book = reset_lighter_order_book
        runtime._reset_state_for_asset_switch = reset_state_for_asset_switch
        runtime.logger = type("Logger", (), {"info": lambda *args, **kwargs: None})()

        await runtime.activate_asset("BTC", reason="test")

        self.assertEqual(runtime.ticker, "BTC")
        self.assertEqual(runtime.variational_ticker, "BTC")
        self.assertEqual(runtime.lighter_market_index, 92)
        self.assertEqual(runtime.base_amount_multiplier, 1000)
        self.assertEqual(runtime.price_multiplier, 100)
        self.assertEqual(
            runtime.calls,
            [
                "get_lighter_market_config",
                "reset_lighter_order_book",
                "reset_state_for_asset_switch",
                "handle_lighter_ws",
                "wait_for_lighter_order_book_ready",
            ],
        )

    async def test_variational_fill_binds_to_pending_strategy_record(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.args = argparse.Namespace(auto_hedge=True)
        runtime.accepted_assets = {"BTC"}
        runtime.variational_ticker = "BTC"
        runtime.records = {}
        runtime.record_order = []
        runtime.lighter_client_order_to_trade_key = {}
        runtime._pending_variational_strategy_order_keys = []
        runtime._pending_trigger_spreads = []
        runtime._record_lock = asyncio.Lock()
        runtime.appended = []

        async def append_order_log(event_type, payload):
            runtime.appended.append((event_type, payload))

        runtime.append_order_log = append_order_log

        record = OrderLifecycle(
            trade_key="strategy:1",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="created",
        )
        runtime.records[record.trade_key] = record
        runtime.record_order.append(record.trade_key)
        runtime._pending_variational_strategy_order_keys.append(record.trade_key)

        await runtime.process_variational_trade_event(
            {
                "trade_id": "var-123",
                "event_seq": 10,
                "side": "buy",
                "qty": "0.001",
                "asset": "BTC",
                "status": "confirmed",
                "price": "100",
                "timestamp": "2026-06-28T00:00:00Z",
            }
        )

        self.assertEqual(list(runtime.records), ["strategy:1"])
        self.assertEqual(runtime.record_order, ["strategy:1"])
        self.assertEqual(runtime._pending_variational_strategy_order_keys, [])
        self.assertEqual(record.trade_id, "var-123")
        self.assertEqual(record.last_variational_status, "filled")
        self.assertEqual(record.var_fill_price, Decimal("100"))
        self.assertEqual(record.var_fill_ts_iso, "2026-06-28T00:00:00Z")
        self.assertEqual(runtime.appended[0][0], "variational_fill")

    async def test_variational_created_record_no_longer_triggers_lighter_hedge(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.args = argparse.Namespace(auto_hedge=True)
        runtime.accepted_assets = {"BTC"}
        runtime.variational_ticker = "BTC"
        runtime.records = {}
        runtime.record_order = []
        runtime.lighter_client_order_to_trade_key = {}
        runtime._pending_variational_strategy_order_keys = []
        runtime._pending_trigger_spreads = []
        runtime._record_lock = asyncio.Lock()
        runtime.hedge_calls = 0

        async def append_order_log(event_type, payload):
            return None

        async def place_lighter_order(record):
            runtime.hedge_calls += 1

        runtime.append_order_log = append_order_log
        runtime.place_lighter_order = place_lighter_order

        await runtime.process_variational_trade_event(
            {
                "trade_id": "manual-1",
                "event_seq": 11,
                "side": "buy",
                "qty": "0.001",
                "asset": "BTC",
                "status": "confirmed",
                "price": "100",
            }
        )

        self.assertEqual(runtime.hedge_calls, 0)
        self.assertEqual(len(runtime.records), 1)

    async def test_variational_fill_binds_with_equivalent_asset_symbol(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.args = argparse.Namespace(auto_hedge=True)
        runtime.accepted_assets = {"LIGHTER", "LIT"}
        runtime.variational_ticker = "LIGHTER"
        runtime.records = {}
        runtime.record_order = []
        runtime.lighter_client_order_to_trade_key = {}
        runtime._pending_variational_strategy_order_keys = []
        runtime._pending_trigger_spreads = []
        runtime._record_lock = asyncio.Lock()

        async def append_order_log(event_type, payload):
            return None

        runtime.append_order_log = append_order_log

        record = OrderLifecycle(
            trade_key="strategy:lit",
            trade_id="",
            side="buy",
            qty=Decimal("1"),
            asset="LIGHTER",
            auto_hedge_enabled=True,
            last_variational_status="created",
        )
        runtime.records[record.trade_key] = record
        runtime.record_order.append(record.trade_key)
        runtime._pending_variational_strategy_order_keys.append(record.trade_key)

        await runtime.process_variational_trade_event(
            {
                "trade_id": "lit-var-1",
                "event_seq": 12,
                "side": "buy",
                "qty": "1",
                "asset": "LIT",
                "status": "confirmed",
                "price": "2",
            }
        )

        self.assertEqual(record.trade_id, "lit-var-1")
        self.assertEqual(record.var_fill_price, Decimal("2"))
        self.assertEqual(runtime._pending_variational_strategy_order_keys, [])


if __name__ == "__main__":
    unittest.main()
