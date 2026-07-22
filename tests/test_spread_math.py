import unittest
import time
import asyncio
import argparse
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from main import (
    BrowserOrderCommand,
    OrderLifecycle,
    PENDING_TRIGGER_SPREAD_TTL_SECONDS,
    PendingTriggerSpread,
    VariationalToLighterRuntime,
    cross_spread_percentages,
    edge_pnl_percent,
    fill_diff_by_direction,
    parse_args,
    PRICE_MAPPING_RATIO,
    resolve_lighter_ticker,
    resolve_variational_ticker,
)
from variational.gradient_strategy import GradientSignal, GradientStrategyState
from variational.gradient_strategy import StrategySection


class SpreadMathTest(unittest.TestCase):
    def test_cl_uses_lighter_wti_market(self):
        self.assertEqual(resolve_lighter_ticker("CL"), "WTI")
        self.assertEqual(resolve_variational_ticker("WTI"), "CL")

    def test_production_price_mapping_ratio_defaults_to_one(self):
        self.assertEqual(PRICE_MAPPING_RATIO, Decimal("1"))
        source = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
        self.assertGreaterEqual(source.count("PRICE_MAPPING_RATIO"), 3)

    def test_edge_pnl_uses_executable_opposite_close_edge(self):
        self.assertEqual(
            edge_pnl_percent(Decimal("0.30"), Decimal("0.10"), Decimal("0.1")),
            Decimal("0.20"),
        )
        self.assertEqual(
            edge_pnl_percent(Decimal("0.10"), Decimal("0.30"), Decimal("-0.1")),
            Decimal("0.20"),
        )

    def test_cross_spread_uses_raw_lighter_price_without_stablecoin_normalization(self):
        var_ask = Decimal("100")
        var_bid = Decimal("99")
        lighter_bid = Decimal("101")
        lighter_ask = Decimal("102")

        long_pct, short_pct = cross_spread_percentages(var_bid, var_ask, lighter_bid, lighter_ask)

        self.assertEqual(long_pct, Decimal("200") / Decimal("201"))
        self.assertEqual(short_pct, Decimal("600") / Decimal("201"))

    def test_fill_diff_uses_raw_lighter_price_without_stablecoin_normalization(self):
        diff, pct = fill_diff_by_direction(
            "buy",
            Decimal("100"),
            Decimal("101"),
        )

        self.assertEqual(diff, Decimal("1"))
        self.assertEqual(pct, Decimal("200") / Decimal("201"))

    def test_unit_spread_uses_raw_lighter_price_without_stablecoin_normalization(self):
        runtime = object.__new__(VariationalToLighterRuntime)
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

        expected_fill = Decimal("200") * Decimal("0.011") / Decimal("200.011")
        self.assertEqual(record.spread_slippage_pct(), expected_fill - Decimal("0.0100"))

    def test_short_fill_edge_and_slippage_use_current_symmetric_formula(self):
        record = OrderLifecycle(
            trade_key="short",
            trade_id="short",
            side="sell",
            qty=Decimal("1"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="filled",
            var_fill_price=Decimal("100"),
            lighter_fill_price=Decimal("100.08"),
            trigger_spread_pct=Decimal("0.10"),
        )

        _diff, fill_edge = fill_diff_by_direction("sell", Decimal("100"), Decimal("100.08"))
        expected_fill = Decimal("200") * Decimal("0.08") / Decimal("200.08")
        self.assertEqual(fill_edge, expected_fill)
        self.assertEqual(record.spread_slippage_pct(), Decimal("0.10") - expected_fill)
        self.assertGreater(record.spread_slippage_pct(), 0)

    def test_leg_slippage_signs(self):
        # 做多 Var：触发100 成交98 → +2%(买得便宜=有利)；做空 Lighter 成交98 → -2%(卖得便宜=不利)
        rec = OrderLifecycle(
            trade_key="t",
            trade_id="",
            side="buy",
            qty=Decimal("1"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="filled",
            lighter_side="SELL",
            var_trigger_price=Decimal("100"),
            var_fill_price=Decimal("98"),
            lighter_trigger_price=Decimal("100"),
            lighter_fill_price=Decimal("98"),
        )
        self.assertEqual(rec.var_slippage_pct(), Decimal("2"))
        self.assertEqual(rec.lighter_slippage_pct(), Decimal("-2"))

        # 做空 Lighter 成交101 → +1%(卖得更高=有利)
        rec2 = OrderLifecycle(
            trade_key="t2",
            trade_id="",
            side="sell",
            qty=Decimal("1"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="filled",
            lighter_side="BUY",
            var_trigger_price=Decimal("100"),
            var_fill_price=Decimal("101"),
            lighter_trigger_price=Decimal("100"),
            lighter_fill_price=Decimal("99"),
        )
        # 做空 Var：触发100 成交101 → +1%(卖得更高)
        self.assertEqual(rec2.var_slippage_pct(), Decimal("1"))
        # 做多 Lighter：触发100 成交99 → +1%(买得便宜)
        self.assertEqual(rec2.lighter_slippage_pct(), Decimal("1"))

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
        runtime._pending_prepare_sigs = set()
        runtime._browser_order_queue = Queue()

        runtime._schedule_prepare_browser_order()

        self.assertEqual(len(runtime._browser_order_queue.items), 1)
        command = runtime._browser_order_queue.items[0]
        self.assertIsInstance(command, BrowserOrderCommand)
        self.assertEqual(command.side, "buy")
        self.assertEqual(command.qty, Decimal("0.001"))
        self.assertTrue(command.prepare_only)
        self.assertEqual(command.wait_after_input_ms, 500)
        self.assertEqual(command.disabled_retry_wait_ms, 1000)

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
        runtime._pending_prepare_sigs = set()
        runtime._browser_order_queue = Queue()

        runtime._schedule_prepare_browser_order()

        command = runtime._browser_order_queue.items[0]
        self.assertEqual(command.side, "buy")

    def test_prepare_browser_order_dedupes_while_request_is_pending(self):
        class Queue:
            def __init__(self):
                self.items = []

            def submit(self, item):
                self.items.append(item)

        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.gradient_strategy = GradientStrategyState.default()
        runtime._prepared_order_side = "buy"
        runtime._last_prepared_order_sig = None
        runtime._pending_prepare_sigs = set()
        runtime._browser_order_queue = Queue()

        runtime._schedule_prepare_browser_order()
        runtime._schedule_prepare_browser_order()

        self.assertEqual(len(runtime._browser_order_queue.items), 1)

        runtime._last_prepared_order_sig = ("buy", "0.001")
        runtime._schedule_prepare_browser_order()

        self.assertEqual(len(runtime._browser_order_queue.items), 1)

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
        runtime.lighter_gateway = SimpleNamespace(size_multiplier=None)
        runtime._last_leg_prices = {}
        runtime._last_dom_bid = None
        runtime._last_dom_ask = None
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
        self.assertEqual(record.strategy_signal_source, "gradient")
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
        runtime.lighter_gateway = SimpleNamespace(size_multiplier=None)
        runtime._last_leg_prices = {}
        runtime._last_dom_bid = None
        runtime._last_dom_ask = None
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

    def test_no_hedge_uses_readonly_rust_gateway(self):
        runtime = VariationalToLighterRuntime(parse_args(["--browser-smoke-test", "--no-hedge"]))

        self.assertFalse(runtime.lighter_gateway.execution_enabled)


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

    async def test_no_hedge_activate_asset_uses_rust_gateway(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.args = argparse.Namespace(auto_hedge=False, depth_notional=2000.0)
        runtime.variational_ticker = None
        runtime.ticker = None
        runtime._asset_switch_lock = asyncio.Lock()
        runtime.calls = []

        class Gateway:
            async def set_market(self, symbol, depth_notional, timeout):
                runtime.calls.append(("set_market", symbol, depth_notional))
                return 92

        async def reset_lighter_order_book():
            runtime.calls.append("reset_lighter_order_book")

        async def reset_state_for_asset_switch():
            runtime.calls.append("reset_state_for_asset_switch")

        runtime.lighter_gateway = Gateway()
        runtime.reset_lighter_order_book = reset_lighter_order_book
        runtime._reset_state_for_asset_switch = reset_state_for_asset_switch
        runtime.logger = type("Logger", (), {"info": lambda *args, **kwargs: None})()

        await runtime.activate_asset("BTC", reason="test")

        self.assertEqual(runtime.ticker, "BTC")
        self.assertEqual(runtime.variational_ticker, "BTC")
        self.assertEqual(runtime.lighter_market_index, 92)
        self.assertEqual(
            runtime.calls,
            [
                "reset_lighter_order_book",
                "reset_state_for_asset_switch",
                ("set_market", "BTC", Decimal("2000.0")),
            ],
        )

    async def test_activate_variational_cl_uses_lighter_wti(self):
        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.args = argparse.Namespace(auto_hedge=False, depth_notional=2000.0)
        runtime.variational_ticker = None
        runtime.ticker = None
        runtime._asset_switch_lock = asyncio.Lock()

        class Gateway:
            async def set_market(_self, symbol, depth_notional, timeout):
                self.assertEqual(symbol, "WTI")
                return 93

        async def noop():
            return None

        runtime.lighter_gateway = Gateway()
        runtime.reset_lighter_order_book = noop
        runtime._reset_state_for_asset_switch = noop
        runtime.logger = type("Logger", (), {"info": lambda *args, **kwargs: None})()

        await runtime.activate_asset("CL", reason="test")

        self.assertEqual(runtime.variational_ticker, "CL")
        self.assertEqual(runtime.ticker, "WTI")
        self.assertEqual(runtime.accepted_assets, {"CL", "WTI"})

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

    async def _run_manual_var_fill(self, *, strategy_enabled: bool) -> int:
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
        runtime.gradient_strategy = GradientStrategyState.default()
        runtime.gradient_strategy.enabled = strategy_enabled
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
        await asyncio.sleep(0)  # 让调度的对冲任务跑一次
        self.assertEqual(len(runtime.records), 1)
        return runtime.hedge_calls

    async def test_manual_var_fill_hedges_when_strategy_disabled(self):
        self.assertEqual(await self._run_manual_var_fill(strategy_enabled=False), 1)

    async def test_manual_var_fill_not_hedged_when_strategy_enabled(self):
        self.assertEqual(await self._run_manual_var_fill(strategy_enabled=True), 0)

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
