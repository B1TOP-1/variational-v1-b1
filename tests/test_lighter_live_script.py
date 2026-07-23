import importlib.util
import unittest
from decimal import Decimal
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "test_lighter_live.py"
SPEC = importlib.util.spec_from_file_location("test_lighter_live_script", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class LighterLiveScriptTest(unittest.TestCase):
    def test_quantity_is_floored_to_lighter_step(self):
        self.assertEqual(MODULE.quantize_quantity(Decimal("0.0019"), 1000), Decimal("0.001"))

    def test_aggressive_limits_cross_the_visible_book(self):
        buy = MODULE.aggressive_limit("buy", Decimal("100"), Decimal("101"), Decimal("10"))
        sell = MODULE.aggressive_limit("sell", Decimal("100"), Decimal("101"), Decimal("10"))

        self.assertGreater(buy, Decimal("101"))
        self.assertLess(sell, Decimal("100"))

    def test_live_mode_requires_explicit_confirmation_phrase(self):
        self.assertEqual(MODULE.LIVE_CONFIRMATION, "I_UNDERSTAND_THIS_SENDS_REAL_ORDERS")


if __name__ == "__main__":
    unittest.main()
