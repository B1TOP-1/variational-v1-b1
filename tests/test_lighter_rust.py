import asyncio
import shutil
import unittest
from decimal import Decimal
from pathlib import Path

from variational.lighter_rust import RustLighterGateway


class RustLighterGatewayTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.events = []

        async def record(event):
            self.events.append(event)

        self.gateway = RustLighterGateway(
            execution_enabled=True,
            event_handler=record,
            binary=Path("/tmp/not-started-gateway"),
        )

    async def test_book_event_updates_rust_owned_quotes(self):
        await self.gateway._handle_event(
            {
                "type": "book",
                "ready": True,
                "symbol": "BTC",
                "market_id": 1,
                "bid": "65000.1",
                "ask": "65000.2",
                "vwap_bid": "64999.8",
                "vwap_ask": "65000.5",
            }
        )

        self.assertTrue(self.gateway.book_ready.is_set())
        self.assertEqual(self.gateway.best_bid, Decimal("65000.1"))
        self.assertEqual(self.gateway.vwap_ask, Decimal("65000.5"))
        self.assertEqual(self.events[-1]["type"], "book")

    async def test_stale_book_clears_readiness(self):
        self.gateway.book_ready.set()
        self.gateway.best_bid = Decimal("1")
        await self.gateway._handle_event({"type": "book", "ready": False, "symbol": "BTC"})
        self.assertFalse(self.gateway.book_ready.is_set())
        self.assertIsNone(self.gateway.best_bid)

    async def test_position_event_is_authoritative(self):
        await self.gateway._handle_event(
            {"type": "position", "symbol": "BTC", "market_id": 1, "quantity": "-0.015"}
        )
        self.assertEqual(self.gateway.positions["BTC"], Decimal("-0.015"))
        self.assertTrue(self.gateway.position_ready.is_set())

    async def test_position_from_an_inactive_symbol_does_not_mark_market_ready(self):
        self.gateway.symbol = "BTC"

        await self.gateway._handle_event(
            {"type": "position", "symbol": "ETH", "market_id": 0, "quantity": "1.25"}
        )

        self.assertEqual(self.gateway.positions["ETH"], Decimal("1.25"))
        self.assertFalse(self.gateway.position_ready.is_set())

    async def test_set_market_requires_and_seeds_authoritative_position(self):
        async def command(command_type, **fields):
            self.assertEqual(command_type, "set_market")
            self.assertEqual(fields["symbol"], "BTC")
            self.gateway.book_ready.set()
            return {
                "data": {
                    "market_id": 1,
                    "min_base_amount": "0.001",
                    "size_multiplier": 1000,
                    "price_multiplier": 10,
                    "position_quantity": "0.011",
                }
            }

        self.gateway.command = command

        market_id = await self.gateway.set_market("BTC", Decimal("1000"))

        self.assertEqual(market_id, 1)
        self.assertEqual(self.gateway.positions["BTC"], Decimal("0.011"))
        self.assertTrue(self.gateway.position_ready.is_set())

    async def test_command_result_resolves_matching_request_only(self):
        pending = asyncio.get_running_loop().create_future()
        self.gateway._pending["py-7"] = pending
        await self.gateway._handle_event(
            {"type": "command_result", "id": "py-7", "ok": True, "data": {"market_id": 1}}
        )
        self.assertEqual((await pending)["data"]["market_id"], 1)

    async def test_missing_binary_fails_with_build_command(self):
        with self.assertRaisesRegex(RuntimeError, "cargo build --release"):
            await self.gateway.start()

    async def test_start_reports_process_exit_without_waiting_full_timeout(self):
        self.gateway.binary = Path(shutil.which("false") or "/usr/bin/false")
        with self.assertRaisesRegex(RuntimeError, "exited during startup"):
            await self.gateway.start(timeout=10)
