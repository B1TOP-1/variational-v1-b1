import unittest
from decimal import Decimal

from main import OrderLifecycle, VariationalToLighterRuntime, cross_spread_percentages


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


if __name__ == "__main__":
    unittest.main()
