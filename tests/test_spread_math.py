import unittest
import time
from decimal import Decimal

from main import (
    BrowserOrderCommand,
    OrderLifecycle,
    PENDING_TRIGGER_SPREAD_TTL_SECONDS,
    PendingTriggerSpread,
    VariationalToLighterRuntime,
    cross_spread_percentages,
)
from variational.gradient_strategy import GradientStrategyState


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
        runtime._last_prepared_order_sig = None
        runtime._browser_order_queue = Queue()

        runtime._schedule_prepare_browser_order()

        self.assertEqual(len(runtime._browser_order_queue.items), 1)
        command = runtime._browser_order_queue.items[0]
        self.assertIsInstance(command, BrowserOrderCommand)
        self.assertEqual(command.side, "buy")
        self.assertEqual(command.qty, Decimal("0.001"))
        self.assertTrue(command.prepare_only)

    def test_prepare_browser_order_dedupes_only_after_success(self):
        class Queue:
            def __init__(self):
                self.items = []

            def submit(self, item):
                self.items.append(item)

        runtime = object.__new__(VariationalToLighterRuntime)
        runtime.gradient_strategy = GradientStrategyState.default()
        runtime._last_prepared_order_sig = None
        runtime._browser_order_queue = Queue()

        runtime._schedule_prepare_browser_order()
        runtime._schedule_prepare_browser_order()

        self.assertEqual(len(runtime._browser_order_queue.items), 2)

        runtime._last_prepared_order_sig = ("buy", "0.001")
        runtime._schedule_prepare_browser_order()

        self.assertEqual(len(runtime._browser_order_queue.items), 2)


if __name__ == "__main__":
    unittest.main()
