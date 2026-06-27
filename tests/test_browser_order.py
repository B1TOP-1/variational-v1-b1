import json
import unittest
from decimal import Decimal

from variational.browser_order import BrowserOrderCommand


class BrowserOrderCommandTest(unittest.TestCase):
    def test_builds_dry_run_browser_order_payload(self):
        command = BrowserOrderCommand(side="buy", qty=Decimal("0.001"))

        payload = command.to_payload()

        self.assertEqual(payload["side"], "buy")
        self.assertEqual(payload["qty"], "0.001")
        self.assertEqual(payload["dryRun"], True)
        self.assertEqual(payload["submitMethod"], "js_dispatch_mouse")
        self.assertEqual(payload["skipInputWhenMatched"], True)
        json.dumps(payload)

    def test_normalizes_unknown_side_to_buy(self):
        command = BrowserOrderCommand(side="invalid", qty=Decimal("0.002"))

        self.assertEqual(command.to_payload()["side"], "buy")

    def test_keeps_sell_side(self):
        command = BrowserOrderCommand(side="sell", qty=Decimal("0.003"))

        self.assertEqual(command.to_payload()["side"], "sell")


if __name__ == "__main__":
    unittest.main()
