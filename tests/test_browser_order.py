import asyncio
import json
import unittest
from asyncio import Future
from decimal import Decimal
from pathlib import Path

from main import parse_args
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


if __name__ == "__main__":
    unittest.main()
