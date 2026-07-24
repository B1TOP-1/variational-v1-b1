import asyncio
import json
import logging
import tempfile
import time
import unittest
from unittest.mock import AsyncMock
from asyncio import Future
from decimal import Decimal
from pathlib import Path

from rich.console import Console

from main import parse_args
from main import OrderLifecycle
from main import RunningStat
from main import SIGNAL_CONFIRM_MIN_QUOTES
from main import SIGNAL_CONFIRM_SECONDS
from main import SIGNAL_FAST_CONFIRM_MIN_QUOTES
from main import SIGNAL_FAST_CONFIRM_SECONDS
from main import SPREAD_TREND_WINDOW_SECONDS
from main import DASHBOARD_REFRESH_SECONDS, SPREAD_SAMPLE_INTERVAL_SECONDS
from main import CstLogFormatter
from main import VariationalToLighterRuntime
from variational.browser_order import BrowserOrderBroker, BrowserOrderCommand, BrowserOrderDispatchQueue
from variational.listener import VariationalMonitor


class BrowserOrderCommandTest(unittest.TestCase):
    def test_order_payload_keeps_details_removed_from_compact_runtime_log(self):
        record = OrderLifecycle(
            trade_key="strategy:test",
            trade_id="order-1",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="filled",
            lighter_limit_price=Decimal("65000.5"),
            lighter_tx_hash="abc123",
            strategy_section="open",
            strategy_threshold_pct=Decimal("0.055"),
            signal_long_edge_pct=Decimal("0.056"),
            signal_short_edge_pct=Decimal("0.044"),
        )

        payload = record.to_payload()

        self.assertEqual(payload["lighter_limit_price"], "65000.5")
        self.assertEqual(payload["lighter_tx_hash"], "abc123")
        self.assertEqual(payload["strategy_section"], "open")
        self.assertEqual(payload["strategy_threshold_pct"], "0.055")
        self.assertEqual(payload["signal_long_edge_pct"], "0.056")
        self.assertEqual(payload["signal_short_edge_pct"], "0.044")

    def test_order_table_uses_compact_chinese_direction_and_cst_time(self):
        record = OrderLifecycle(
            trade_key="strategy:test",
            trade_id="order-1",
            side="sell",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="filled",
            var_submit_click_started_at="2026-07-21T07:42:30.000Z",
        )

        self.assertEqual(VariationalToLighterRuntime._compact_direction_label("sell"), "做空V/做多L")
        self.assertEqual(VariationalToLighterRuntime._compact_direction_label("buy"), "做多V/做空L")
        self.assertEqual(VariationalToLighterRuntime._order_time_label(record), "7/21 15.42")

    def test_sell_direction_highlights_fill_spread_but_keeps_leg_colors(self):
        runtime = VariationalToLighterRuntime(parse_args(["--browser-smoke-test"]))

        self.assertEqual(
            runtime._style_fill_value_by_direction("32.26", "sell"),
            "[yellow]32.26[/yellow]",
        )
        self.assertEqual(runtime._style_fill_value_by_direction("32.26", "buy"), "32.26")

        sell_text = runtime._fmt_fill_pct_with_leg_slippage(
            Decimal("0.0491"), Decimal("0.001"), Decimal("-0.006"), "sell"
        )
        self.assertIn("[yellow]0.0491%[/yellow]", sell_text)
        self.assertIn("V[green]+0.001%[/green]", sell_text)
        self.assertIn("L[red]-0.006%[/red]", sell_text)

        buy_text = runtime._fmt_fill_pct_with_leg_slippage(
            Decimal("0.0491"), None, None, "buy"
        )
        self.assertEqual(buy_text, "0.0491%")

    def test_terminal_dashboard_refreshes_at_active_quote_frequency(self):
        self.assertEqual(DASHBOARD_REFRESH_SECONDS, 0.2)
        self.assertEqual(SPREAD_SAMPLE_INTERVAL_SECONDS, 1.0)

    def test_browser_smoke_test_args_are_available(self):
        args = parse_args(["--browser-smoke-test", "--browser-smoke-qty", "0.001"])

        self.assertTrue(args.browser_smoke_test)
        self.assertEqual(args.browser_smoke_qty, "0.001")

    def test_browser_smoke_test_does_not_require_lighter_env(self):
        args = parse_args(["--browser-smoke-test"])

        runtime = VariationalToLighterRuntime(args)

        self.assertIsNone(runtime.lighter_gateway.process)

    def test_session_log_paths_are_isolated_and_runtime_time_is_utc_plus_8(self):
        runtime = VariationalToLighterRuntime(parse_args(["--browser-smoke-test"]))
        self.assertEqual(runtime.run_dir.parent.name, "runs")
        self.assertTrue(runtime.run_dir.name.endswith("_UTC+8"))
        self.assertEqual(runtime.orders_file.name, "order_metrics.jsonl")
        self.assertNotEqual(runtime.spread_store.path, runtime.run_dir / "spread_history.sqlite3")

        record = logging.LogRecord("test", logging.INFO, __file__, 1, "message", (), None)
        record.created = 0
        record.msecs = 0
        self.assertEqual(CstLogFormatter().formatTime(record), "1970-01-01 08:00:00,000")

    def test_builds_dry_run_browser_order_payload(self):
        command = BrowserOrderCommand(side="buy", qty=Decimal("0.001"))

        payload = command.to_payload()

        self.assertEqual(payload["side"], "buy")
        self.assertEqual(payload["qty"], "0.001")
        self.assertEqual(payload["dryRun"], True)
        self.assertEqual(payload["submitMethod"], "js_click")
        self.assertEqual(payload["waitBeforeInputMs"], 0)
        self.assertEqual(payload["waitAfterInputMs"], 500)
        self.assertEqual(payload["disabledRetryWaitMs"], 1000)
        self.assertEqual(payload["orderResponseTimeoutMs"], 1000)
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

    def test_builds_activity_click_without_submit_action(self):
        command = BrowserOrderCommand(
            side="sell",
            qty=Decimal("0.001"),
            activity_only=True,
        )

        self.assertEqual(command.action, "keepalive_click")
        self.assertTrue(command.to_payload()["activityOnly"])

    def test_normalizes_unknown_side_to_buy(self):
        command = BrowserOrderCommand(side="invalid", qty=Decimal("0.002"))

        self.assertEqual(command.to_payload()["side"], "buy")

    def test_keeps_sell_side(self):
        command = BrowserOrderCommand(side="sell", qty=Decimal("0.003"))

        self.assertEqual(command.to_payload()["side"], "sell")

    def test_extension_allows_quote_and_order_responses(self):
        background = (Path(__file__).resolve().parents[1] / "chrome_extension" / "background.js").read_text()

        self.assertIn("https://omni.variational.io/api/quotes/indicative", background)
        self.assertIn("https://omni.variational.io/api/orders/new/market", background)
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

    def test_extension_keepalive_click_only_targets_side_control(self):
        background = (Path(__file__).resolve().parents[1] / "chrome_extension" / "background.js").read_text()
        start = background.index('if (action === "keepalive_click")')
        end = background.index('if (action === "read_position")', start)
        keepalive = background[start:end]

        self.assertIn("locateOrderElementsInPage", keepalive)
        self.assertIn("dispatchTrustedClick", keepalive)
        self.assertNotIn("submit-button", keepalive)

    def test_parse_dom_position_text(self):
        parse = VariationalToLighterRuntime._parse_dom_position_text

        self.assertEqual(parse("0.003 XAU"), Decimal("0.003"))
        self.assertEqual(parse("-0.01 XAU"), Decimal("-0.01"))
        self.assertEqual(parse("-"), Decimal("0"))
        self.assertEqual(parse(" - "), Decimal("0"))
        self.assertIsNone(parse(""))
        self.assertIsNone(parse(None))

class PositionRecoveryTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _runtime():
        runtime = VariationalToLighterRuntime(parse_args(["--browser-smoke-test"]))
        runtime.ticker = "BTC"
        runtime._lighter_positions["BTC"] = Decimal("-0.004")
        runtime._lighter_position_ready.set()
        runtime.lighter_gateway.size_multiplier = 1000
        return runtime

    async def test_manual_missing_var_fill_uses_price_and_quantity_and_marks_source(self):
        rt = self._runtime()
        rt._round_ledger_synced = True
        entry = OrderLifecycle(
            trade_key="strategy:entry",
            trade_id="",
            side="buy",
            qty=Decimal("0.005"),
            asset="BTC",
            auto_hedge_enabled=True,
            var_fill_price=Decimal("65000"),
            lighter_fill_price=Decimal("65010"),
            lighter_filled_qty=Decimal("0.005"),
            last_variational_status="filled",
        )
        rt.records[entry.trade_key] = entry
        rt.record_order.append(entry.trade_key)
        rt._account_round_fill(entry, Decimal("0.05"))
        missing = OrderLifecycle(
            trade_key="strategy:manual",
            trade_id="",
            side="sell",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            lighter_fill_price=Decimal("65510"),
            lighter_filled_qty=Decimal("0.001"),
            last_variational_status="strategy_submitted",
        )
        rt.records[missing.trade_key] = missing
        rt.record_order.append(missing.trade_key)
        rt._pending_variational_strategy_order_keys.append(missing.trade_key)
        rt._cached_position_qty = Decimal("0.004")

        draft = rt._prepare_manual_fill_recovery()
        self.assertIsNotNone(draft)
        self.assertEqual(draft.legs, ("var",))
        draft.values[("var", "price")] = Decimal("65500")
        draft.values[("var", "qty")] = Decimal("0.001")
        draft.step_index = 2

        await rt._apply_manual_fill_recovery(draft)

        self.assertEqual(missing.var_fill_price, Decimal("65500"))
        self.assertEqual(missing.var_fill_price_source, "manual_input")
        self.assertTrue(missing.var_fill_price_estimated)
        self.assertIn("手动补记成功", rt._manual_fill_recovery_status)
        self.assertNotIn(missing.trade_key, rt._pending_variational_strategy_order_keys)

    async def test_manual_missing_lighter_fill_uses_price_and_quantity_and_marks_source(self):
        rt = self._runtime()
        rt._round_ledger_synced = True
        entry = OrderLifecycle(
            trade_key="strategy:entry",
            trade_id="",
            side="buy",
            qty=Decimal("0.005"),
            asset="BTC",
            auto_hedge_enabled=True,
            var_fill_price=Decimal("65000"),
            lighter_fill_price=Decimal("65010"),
            lighter_filled_qty=Decimal("0.005"),
            last_variational_status="filled",
        )
        rt.records[entry.trade_key] = entry
        rt.record_order.append(entry.trade_key)
        rt._account_round_fill(entry, Decimal("0.05"))
        missing = OrderLifecycle(
            trade_key="strategy:missing-lighter",
            trade_id="fill-var",
            side="sell",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            var_fill_price=Decimal("65500"),
            var_fill_price_source="trade_ws",
            last_variational_status="filled",
        )
        rt.records[missing.trade_key] = missing
        rt.record_order.append(missing.trade_key)
        rt._cached_position_qty = Decimal("0.004")

        draft = rt._prepare_manual_fill_recovery()
        self.assertIsNotNone(draft)
        self.assertEqual(draft.legs, ("lighter",))
        draft.values[("lighter", "price")] = Decimal("65510")
        draft.values[("lighter", "qty")] = Decimal("0.001")
        draft.step_index = 2

        await rt._apply_manual_fill_recovery(draft)

        self.assertEqual(missing.lighter_fill_price, Decimal("65510"))
        self.assertEqual(missing.lighter_filled_qty, Decimal("0.001"))
        self.assertEqual(missing.lighter_fill_price_source, "manual_input")
        self.assertTrue(missing.round_accounted)

    async def test_manual_external_flat_close_prompts_for_both_legs_and_completes_round(self):
        rt = self._runtime()
        rt._round_ledger_synced = True
        rt.round_exit_ledger.apply_fill(
            "buy",
            Decimal("0.005"),
            Decimal("0.05"),
            unit_spread=Decimal("10"),
        )
        rt._cached_position_qty = Decimal("0")
        rt._lighter_positions["BTC"] = Decimal("0")

        draft = rt._prepare_manual_fill_recovery()
        self.assertIsNotNone(draft)
        self.assertTrue(draft.external_close)
        self.assertEqual(draft.legs, ("var", "lighter"))
        self.assertEqual(draft.side, "sell")
        self.assertEqual(draft.expected_qty, Decimal("0.005"))
        draft.values.update({
            ("var", "price"): Decimal("65500"),
            ("var", "qty"): Decimal("0.005"),
            ("lighter", "price"): Decimal("65510"),
            ("lighter", "qty"): Decimal("0.005"),
        })
        draft.step_index = 4

        await rt._apply_manual_fill_recovery(draft)

        self.assertEqual(rt.round_exit_ledger.position_qty, Decimal("0"))
        self.assertEqual(rt.round_exit_ledger.completed_round_count, 1)
        manual = next(record for record in rt.records.values() if record.strategy_action == "manual_close")
        self.assertEqual(manual.var_fill_price_source, "manual_input")
        self.assertEqual(manual.lighter_fill_price_source, "manual_input")
        self.assertIn("持仓周期已归零", rt._manual_fill_recovery_status)

    def test_manual_recovery_rejects_wrong_quantity_before_apply(self):
        rt = self._runtime()
        rt._round_ledger_synced = True
        rt._strategy_halted = True
        rt.round_exit_ledger.apply_fill("buy", Decimal("0.005"), Decimal("0.05"))
        rt._cached_position_qty = Decimal("0")
        rt._lighter_positions["BTC"] = Decimal("0")

        self.assertTrue(rt._handle_manual_fill_recovery_key("r"))
        rt._manual_fill_price_buffer = "65500"
        self.assertTrue(rt._handle_manual_fill_recovery_key("\r"))
        rt._manual_fill_price_buffer = "0.004"
        self.assertTrue(rt._handle_manual_fill_recovery_key("\r"))

        self.assertIn("必须输入 0.005", rt._manual_fill_recovery_status)
        self.assertEqual(rt._manual_fill_recovery_draft.current_field, "qty")

    def test_restart_with_external_flat_preserves_live_round_for_manual_recovery(self):
        rt = self._runtime()
        rt.round_exit_ledger.apply_fill("buy", Decimal("0.005"), Decimal("0.05"))
        rt._lighter_positions["BTC"] = Decimal("0")
        rt._round_ledger_synced = False

        rt._sync_round_ledger_with_live_position(Decimal("0"))

        self.assertTrue(rt._round_ledger_synced)
        self.assertTrue(rt._strategy_halted)
        self.assertEqual(rt.round_exit_ledger.position_qty, Decimal("0.005"))
        self.assertIn("程序外两腿平仓", rt._halt_reason)

    def test_manual_external_close_rejects_even_one_lot_of_live_position(self):
        rt = self._runtime()
        rt._round_ledger_synced = True
        rt.round_exit_ledger.apply_fill("buy", Decimal("0.005"), Decimal("0.05"))
        rt._cached_position_qty = Decimal("0.001")
        rt._lighter_positions["BTC"] = Decimal("0")

        draft = rt._prepare_manual_fill_recovery()

        self.assertIsNone(draft)
        self.assertIn("没有检测到两腿已手动归零", rt._manual_fill_recovery_status)


    def test_enter_resumes_strategy_from_dedicated_halted_row(self):
        rt = self._runtime()
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.05")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.005")
        rt._round_ledger_synced = True
        rt.round_exit_ledger.apply_fill("buy", Decimal("0.004"), Decimal("0.04"))
        rt._cached_position_qty = Decimal("0.004")
        rt._strategy_halted = True
        rt._halt_reason = "manual check"

        handled = rt._handle_manual_fill_recovery_key("\r")

        self.assertTrue(handled)
        self.assertFalse(rt._strategy_halted)
        self.assertTrue(rt.gradient_strategy.enabled)
        self.assertEqual(rt._manual_fill_recovery_status, "策略已手动启动")

    def test_enter_rejects_resume_while_ledger_still_mismatches(self):
        rt = self._runtime()
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.05")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.005")
        rt._round_ledger_synced = True
        rt.round_exit_ledger.apply_fill("buy", Decimal("0.005"), Decimal("0.04"))
        rt._cached_position_qty = Decimal("0.003")
        rt._strategy_halted = True

        handled = rt._handle_manual_fill_recovery_key("\r")

        self.assertTrue(handled)
        self.assertTrue(rt._strategy_halted)
        self.assertIn("账本与真实仓位不一致", rt._manual_fill_recovery_status)

    def test_pending_fill_match_uses_fifo(self):
        rt = self._runtime()
        old = OrderLifecycle(
            trade_key="strategy:old",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            strategy_created_at_ms=1_000_000,
            last_variational_status="strategy_submitted",
        )
        current = OrderLifecycle(
            trade_key="strategy:current",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            strategy_created_at_ms=1_300_000,
            last_variational_status="strategy_submitted",
        )
        for record in (old, current):
            rt.records[record.trade_key] = record
            rt._pending_variational_strategy_order_keys.append(record.trade_key)

        selected = rt._match_pending_variational_strategy_order(
            "buy",
            Decimal("0.001"),
            "BTC",
        )

        self.assertIs(selected, old)
        self.assertNotIn(old.trade_key, rt._pending_variational_strategy_order_keys)
        self.assertIn(current.trade_key, rt._pending_variational_strategy_order_keys)

    def test_filled_pending_record_cannot_consume_next_trade(self):
        rt = self._runtime()
        stale = OrderLifecycle(
            trade_key="strategy:stale",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            var_fill_price=Decimal("65000"),
            var_fill_price_source="manual_input",
            last_variational_status="filled",
        )
        current = OrderLifecycle(
            trade_key="strategy:current",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
        )
        for record in (stale, current):
            rt.records[record.trade_key] = record
            rt._pending_variational_strategy_order_keys.append(record.trade_key)

        selected = rt._match_pending_variational_strategy_order("buy", Decimal("0.001"), "BTC")

        self.assertIs(selected, current)
        self.assertNotIn(stale.trade_key, rt._pending_variational_strategy_order_keys)

    def test_order_ids_do_not_override_fifo_order(self):
        rt = self._runtime()
        old = OrderLifecycle(
            trade_key="strategy:old",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            var_submit_order_id="order-old",
            last_variational_status="strategy_submitted",
        )
        current = OrderLifecycle(
            trade_key="strategy:current",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            var_submit_order_id="order-current",
            last_variational_status="strategy_submitted",
        )
        for record in (old, current):
            rt.records[record.trade_key] = record
            rt._pending_variational_strategy_order_keys.append(record.trade_key)

        selected = rt._match_pending_variational_strategy_order(
            "buy",
            Decimal("0.001"),
            "BTC",
        )

        self.assertIs(selected, old)
        self.assertNotIn(old.trade_key, rt._pending_variational_strategy_order_keys)
        self.assertIn(current.trade_key, rt._pending_variational_strategy_order_keys)

    def test_unknown_trade_id_falls_back_to_fifo(self):
        rt = self._runtime()
        pending = OrderLifecycle(
            trade_key="strategy:pending",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            var_submit_order_id="known-order",
            last_variational_status="strategy_submitted",
        )
        rt.records[pending.trade_key] = pending
        rt._pending_variational_strategy_order_keys.append(pending.trade_key)

        selected = rt._match_pending_variational_strategy_order("buy", Decimal("0.001"), "BTC")

        self.assertIs(selected, pending)
        self.assertNotIn(pending.trade_key, rt._pending_variational_strategy_order_keys)

    async def test_same_size_trade_events_consume_fifo_once_without_overwrite(self):
        rt = self._runtime()
        rt.accepted_assets = {"BTC"}
        rt.variational_ticker = "BTC"
        rt.logged_fills = []

        async def append_order_log(event_type, payload):
            rt.logged_fills.append((event_type, payload))

        rt.append_order_log = append_order_log
        first = OrderLifecycle(
            trade_key="strategy:first",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
        )
        second = OrderLifecycle(
            trade_key="strategy:second",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
        )
        for record in (first, second):
            rt.records[record.trade_key] = record
            rt.record_order.append(record.trade_key)
            rt._pending_variational_strategy_order_keys.append(record.trade_key)

        await rt.process_variational_trade_event({
            "trade_id": "fill-1",
            "side": "buy",
            "qty": "0.001",
            "asset": "BTC",
            "status": "filled",
            "price": "65000",
            "timestamp": "2026-07-24T00:00:01Z",
        })
        await rt.process_variational_trade_event({
            "trade_id": "fill-2",
            "side": "buy",
            "qty": "0.001",
            "asset": "BTC",
            "status": "filled",
            "price": "65001",
            "timestamp": "2026-07-24T00:00:02Z",
        })
        # 重复推送只命中原 trade_id，不得覆盖价格或消费新的在途单。
        await rt.process_variational_trade_event({
            "trade_id": "fill-1",
            "side": "buy",
            "qty": "0.001",
            "asset": "BTC",
            "status": "filled",
            "price": "99999",
            "timestamp": "2026-07-24T00:00:03Z",
        })

        self.assertEqual(first.trade_id, "fill-1")
        self.assertEqual(first.var_fill_price, Decimal("65000"))
        self.assertEqual(second.trade_id, "fill-2")
        self.assertEqual(second.var_fill_price, Decimal("65001"))
        self.assertEqual(list(rt._pending_variational_strategy_order_keys), [])
        self.assertEqual([event for event, _ in rt.logged_fills], ["variational_fill", "variational_fill"])

    async def test_fill_arriving_before_order_response_is_merged_by_id(self):
        rt = self._runtime()
        strategy = OrderLifecycle(
            trade_key="strategy:race",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
        )
        orphan = OrderLifecycle(
            trade_key="id:order-race",
            trade_id="order-race",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            var_fill_price=Decimal("65001"),
            var_fill_ts_iso="2026-07-24T00:00:00Z",
            last_variational_status="filled",
        )
        for record in (strategy, orphan):
            rt.records[record.trade_key] = record
            rt.record_order.append(record.trade_key)
        rt._pending_variational_strategy_order_keys.append(strategy.trade_key)

        await rt._record_var_submit_result(strategy.trade_key, {
            "ok": True,
            "orderResponse": {"status": 200, "json": {"id": "order-race"}},
        })

        self.assertEqual(strategy.var_submit_order_id, "order-race")
        self.assertEqual(strategy.trade_id, "order-race")
        self.assertEqual(strategy.var_fill_price, Decimal("65001"))
        self.assertNotIn(orphan.trade_key, rt.records)
        self.assertNotIn(strategy.trade_key, rt._pending_variational_strategy_order_keys)


class TelegramFillMessageTest(unittest.TestCase):
    @staticmethod
    def _runtime():
        runtime = VariationalToLighterRuntime(parse_args(["--browser-smoke-test"]))
        runtime.ticker = "BTC"
        runtime.variational_ticker = "BTC"
        runtime._round_ledger_synced = True
        runtime.telegram_messages = []
        runtime.telegram_notifier = type(
            "CaptureNotifier",
            (),
            {"enqueue": lambda _self, message: runtime.telegram_messages.append(message) or True},
        )()
        return runtime

    @staticmethod
    def _filled_record(key, side, var_price, lighter_price):
        return OrderLifecycle(
            trade_key=key,
            trade_id=key,
            side=side,
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="filled",
            var_fill_price=Decimal(var_price),
            var_fill_price_source="trade_ws",
            lighter_side="SELL" if side == "buy" else "BUY",
            lighter_fill_price=Decimal(lighter_price),
            lighter_filled_qty=Decimal("0.001"),
            lighter_filled_quote=Decimal(lighter_price) * Decimal("0.001"),
            lighter_fill_price_source="trade_ws",
        )

    def test_fill_and_round_notifications_use_compact_approved_templates(self):
        rt = self._runtime()
        entry = self._filled_record("strategy:entry", "buy", "65000", "65010")

        rt._maybe_record_slippage_stats(entry)

        self.assertEqual(len(rt.telegram_messages), 1)
        self.assertIn("🔔 BTC｜买V/卖L 0.001｜第1单", rt.telegram_messages[0])
        self.assertIn("平仓Edge 0%", rt.telegram_messages[0])
        self.assertNotIn("本轮已配对", rt.telegram_messages[0])
        self.assertNotIn("累计", rt.telegram_messages[0])

        close = self._filled_record("strategy:close", "sell", "65050", "65020")
        rt._maybe_record_slippage_stats(close)

        self.assertEqual(len(rt.telegram_messages), 3)
        self.assertIn("🔔 BTC｜卖V/买L 0.001｜第2单", rt.telegram_messages[1])
        self.assertIn("本轮已配对", rt.telegram_messages[1])
        self.assertIn("累计", rt.telegram_messages[1])
        self.assertIn("✅ BTC｜第1轮清仓归零", rt.telegram_messages[2])
        self.assertIn("入场 做多V/做空L｜Edge", rt.telegram_messages[2])
        self.assertIn("平仓 做空V/做多L｜Edge", rt.telegram_messages[2])
        self.assertIn("累计收益", rt.telegram_messages[2])

        rt._maybe_record_slippage_stats(close)
        self.assertEqual(len(rt.telegram_messages), 3)


class BrowserOrderBrokerTest(unittest.IsolatedAsyncioTestCase):
    def test_trade_event_preserves_order_id(self):
        monitor = VariationalMonitor()

        monitor._update_trade_event({
            "type": "trade_filled",
            "data": {
                "id": "trade-1",
                "order_id": "order-1",
                "side": "buy",
                "qty": "0.001",
                "price": "65000",
                "status": "filled",
                "instrument": {"underlying": "BTC"},
            },
        })

        self.assertEqual(monitor.trade_events[-1]["trade_id"], "trade-1")
        self.assertEqual(monitor.trade_events[-1]["order_id"], "order-1")

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

    async def test_dom_notice_is_forwarded_to_observer(self):
        broker = BrowserOrderBroker()
        notices = []
        broker.on_dom_notice = notices.append

        await broker.handle_raw_message(json.dumps({
            "event": "dom_notice",
            "title": "连接已断开",
            "message": "正在尝试重新连接。",
            "ts": 123,
        }))

        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0]["title"], "连接已断开")

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
        runtime = VariationalToLighterRuntime(parse_args(["--browser-smoke-test"]))
        runtime.ticker = "BTC"
        runtime._lighter_positions["BTC"] = Decimal("0")
        runtime._lighter_position_ready.set()
        return runtime

    def test_binance_usdc_book_is_observation_only(self):
        rt = self._runtime()

        self.assertTrue(rt._update_binance_usdc_book({
            "s": "USDCUSDT", "b": "0.99980", "a": "0.99990",
        }))
        self.assertEqual(rt.binance_usdc_bid, Decimal("0.99980"))
        self.assertEqual(rt.binance_usdc_ask, Decimal("0.99990"))
        self.assertIsNotNone(rt.binance_usdc_received_ms)
        self.assertEqual(rt.binance_usdc_status, "connected")
        self.assertFalse(rt._update_binance_usdc_book({
            "s": "USDCUSDT", "b": "1.1", "a": "1.0",
        }))

        source = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
        self.assertIn("usdcusdt@bookTicker", source)
        self.assertIn("仅观察，不参与下单", source)

    @staticmethod
    def _configure_round_exit_gradients(rt):
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.06")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.005")
        rt.gradient_strategy.close_rows[0].threshold_pct = Decimal("0.05")
        rt.gradient_strategy.close_rows[0].target_qty = Decimal("-0.02")

    @staticmethod
    def _account_round_fill(rt, side, edge, qty="0.001"):
        record = OrderLifecycle(
            trade_key=f"strategy:{side}:{len(rt.records)}",
            trade_id="",
            side=side,
            qty=Decimal(qty),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="filled",
        )
        rt.records[record.trade_key] = record
        rt.record_order.append(record.trade_key)
        rt._account_round_fill(record, Decimal(edge))
        return record

    def test_actual_fills_create_round_and_order_metadata(self):
        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt._round_ledger_synced = True

        record = self._account_round_fill(rt, "sell", "0.042", qty="0.003")

        self.assertEqual(rt.round_exit_ledger.position_qty, Decimal("-0.003"))
        self.assertEqual(rt.round_exit_ledger.entry_edge_actual, Decimal("0.042"))
        self.assertEqual(record.round_id, 1)
        self.assertEqual(record.round_fill_role, "entry")

    def test_no_round_exit_before_actual_close_fill(self):
        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt._round_ledger_synced = True
        self._account_round_fill(rt, "sell", "0.042", qty="0.003")

        signal = rt._select_strategy_signal(Decimal("0.055"), Decimal("0.03"), Decimal("-0.003"))

        self.assertEqual(signal.source, "gradient")
        self.assertEqual(signal.target_qty, Decimal("-0.02"))

    def test_profitable_actual_close_enables_cost_line_exit_without_normal_threshold(self):
        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt.gradient_strategy.close_rows[0].target_qty = Decimal("-0.005")
        rt._round_ledger_synced = True
        self._account_round_fill(rt, "sell", "0.042", qty="0.010")
        close_record = self._account_round_fill(rt, "buy", "0.052")

        signal = rt._select_strategy_signal(Decimal("0.052"), Decimal("0.048"), Decimal("-0.009"))

        self.assertTrue(rt.round_exit_ledger.guard_started)
        self.assertEqual(close_record.round_fill_role, "close")
        self.assertEqual(signal.source, "round_exit_guard")
        self.assertEqual(signal.target_qty, Decimal("-0.005"))
        self.assertEqual(signal.threshold_pct, Decimal("0.042"))

    def test_one_basis_point_can_trigger_first_exit_without_actual_close_history(self):
        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt.gradient_strategy.close_rows[0].target_qty = Decimal("-0.005")
        rt._round_ledger_synced = True
        self._account_round_fill(rt, "sell", "0.0448", qty="0.010")

        signal = rt._select_strategy_signal(
            Decimal("0.0548"),
            Decimal("0.0489"),
            Decimal("-0.010"),
        )

        self.assertIsNone(rt.round_exit_ledger.close_edge_actual)
        self.assertEqual(signal.source, "round_exit_guard")
        self.assertEqual(signal.target_qty, Decimal("-0.005"))
        self.assertEqual(signal.spread_pct, Decimal("0.0548"))

    def test_normal_gradient_can_refill_after_actual_close(self):
        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt._round_ledger_synced = True
        self._account_round_fill(rt, "sell", "0.042", qty="0.003")
        self._account_round_fill(rt, "buy", "0.040")

        signal = rt._select_strategy_signal(Decimal("0.05"), Decimal("0.03"), Decimal("-0.002"))

        self.assertEqual(signal.source, "gradient")
        self.assertEqual(signal.target_qty, Decimal("-0.02"))

    def test_normal_gradient_cross_zero_keeps_configured_target(self):
        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt._round_ledger_synced = True
        self._account_round_fill(rt, "sell", "0.042", qty="0.003")
        self._account_round_fill(rt, "buy", "0.040")

        signal = rt._select_strategy_signal(Decimal("0.07"), Decimal("0.10"), Decimal("-0.002"))

        self.assertEqual(signal.source, "gradient")
        self.assertEqual(signal.target_qty, Decimal("0.005"))
        self.assertEqual(signal.delta_qty, Decimal("0.007"))

    def test_actual_refill_after_close_updates_entry_cost(self):
        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt._round_ledger_synced = True
        self._account_round_fill(rt, "sell", "0.042", qty="0.003")
        self._account_round_fill(rt, "buy", "0.052")

        refill = self._account_round_fill(rt, "sell", "0.04")

        self.assertFalse(rt._strategy_halted)
        self.assertEqual(refill.round_fill_role, "entry")
        self.assertEqual(rt.round_exit_ledger.position_qty, Decimal("-0.003"))

    def test_runtime_round_state_persists_and_restores(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "round_exit_state.json"
            rt = self._runtime()
            self._configure_round_exit_gradients(rt)
            rt.round_exit_state_file = path
            rt._round_ledger_synced = True
            self._account_round_fill(rt, "sell", "0.042", qty="0.003")
            self._account_round_fill(rt, "buy", "0.052")

            restored_runtime = self._runtime()
            restored_runtime.round_exit_state_file = path
            restored_runtime.round_exit_ledger = restored_runtime._load_round_exit_ledger()
            restored_runtime._sync_round_ledger_with_live_position(Decimal("-0.002"))

            self.assertTrue(restored_runtime._round_ledger_synced)
            self.assertEqual(restored_runtime.round_exit_ledger.entry_edge_actual, Decimal("0.042"))
            self.assertEqual(restored_runtime.round_exit_ledger.close_edge_actual, Decimal("0.052"))

    def test_restored_round_position_mismatch_halts(self):
        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt._round_ledger_synced = True
        self._account_round_fill(rt, "sell", "0.042", qty="0.003")
        rt._round_ledger_synced = False

        rt._sync_round_ledger_with_live_position(Decimal("-0.002"))

        self.assertTrue(rt._strategy_halted)
        self.assertIn("恢复本轮仓位不一致", rt._halt_reason)

    def test_runtime_round_position_mismatch_requires_continuous_confirmation(self):
        import main as main_mod

        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt._round_ledger_synced = True
        self._account_round_fill(rt, "sell", "0.044", qty="0.002")

        first = rt._select_strategy_signal(Decimal("0.03"), Decimal("0.06"), Decimal("-0.001"))
        self.assertIsNone(first)
        self.assertFalse(rt._strategy_halted)

        rt._select_strategy_signal(Decimal("0.03"), Decimal("0.06"), Decimal("-0.002"))
        self.assertIsNone(rt._round_position_mismatch_since)

        rt._select_strategy_signal(Decimal("0.03"), Decimal("0.06"), Decimal("-0.001"))
        rt._round_position_mismatch_since = (
            time.monotonic() - main_mod.ROUND_POSITION_MISMATCH_CONFIRM_SECONDS - 0.1
        )
        rt._select_strategy_signal(Decimal("0.03"), Decimal("0.06"), Decimal("-0.001"))

        self.assertTrue(rt._strategy_halted)
        self.assertIn("持续不一致", rt._halt_reason)

    def test_round_position_mismatch_keeps_existing_halt_reason(self):
        import main as main_mod

        rt = self._runtime()
        self._configure_round_exit_gradients(rt)
        rt._round_ledger_synced = True
        self._account_round_fill(rt, "sell", "0.044", qty="0.002")
        rt._strategy_halted = True
        rt._halt_reason = "两腿已平衡但本轮成交记账超时: strategy:missing-fill"

        rt._select_strategy_signal(Decimal("0.03"), Decimal("0.06"), Decimal("-0.001"))
        rt._round_position_mismatch_since = (
            time.monotonic() - main_mod.ROUND_POSITION_MISMATCH_CONFIRM_SECONDS - 0.1
        )
        rt._select_strategy_signal(Decimal("0.03"), Decimal("0.06"), Decimal("-0.001"))

        self.assertEqual(
            rt._halt_reason,
            "两腿已平衡但本轮成交记账超时: strategy:missing-fill",
        )

    async def test_round_accounting_waits_for_actual_fill_record(self):
        rt = self._runtime()
        rt.args.auto_hedge = True
        rt._round_ledger_synced = True
        record = OrderLifecycle(
            trade_key="strategy:wait",
            trade_id="",
            side="sell",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
        )
        rt.records[record.trade_key] = record

        async def complete_accounting():
            await asyncio.sleep(0.01)
            async with rt._record_lock:
                record.round_accounted = True

        task = asyncio.create_task(complete_accounting())
        await rt._wait_for_round_accounting(record.trade_key, timeout_seconds=0.2)
        await task

        self.assertFalse(rt._strategy_halted)

    async def test_round_accounting_timeout_halts(self):
        rt = self._runtime()
        rt.args.auto_hedge = True
        rt._round_ledger_synced = True
        record = OrderLifecycle(
            trade_key="strategy:timeout",
            trade_id="",
            side="sell",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
        )
        rt.records[record.trade_key] = record

        await rt._wait_for_round_accounting(record.trade_key, timeout_seconds=0.01)

        self.assertTrue(rt._strategy_halted)
        self.assertIn("本轮成交记账超时", rt._halt_reason)

    def test_cross_spread_sample_includes_binance_usdcusdt_book(self):
        rt = self._runtime()
        rt.binance_usdc_bid = Decimal("0.99980")
        rt.binance_usdc_ask = Decimal("0.99990")
        rt.binance_usdc_received_ms = 123456

        rt._record_cross_spreads(
            "BTC", Decimal("65000"), Decimal("65001"),
            Decimal("65002"), Decimal("65003"), Decimal("0.1"), Decimal("0.2"),
        )

        latest = rt.spread_store.latest("BTC")
        self.assertEqual(latest["usdcUsdtBid"], 0.9998)
        self.assertEqual(latest["usdcUsdtAsk"], 0.9999)
        self.assertEqual(latest["usdcUsdtReceivedMs"], 123456)

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
                Decimal("0.2") if index % 2 == 0 else Decimal("0.25"),
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

    async def test_stable_edge_uses_fast_confirmation_path(self):
        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.1")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.05")
        placed = []
        rt._handle_new_gradient_signal = lambda sig: placed.append(sig) or "rec"

        edges = (Decimal("0.200"), Decimal("0.205"), Decimal("0.203"))
        for index in range(SIGNAL_FAST_CONFIRM_MIN_QUOTES):
            rt._evaluate_gradient_signal(
                edges[index], Decimal("0"), Decimal("0"),
                active_quote_key=("session-fast", index + 1),
                now_monotonic=100.0 + index * (SIGNAL_FAST_CONFIRM_SECONDS / (SIGNAL_FAST_CONFIRM_MIN_QUOTES - 1)),
            )

        self.assertEqual(len(placed), 1)

    async def test_volatile_edge_cannot_use_fast_confirmation_path(self):
        rt = self._runtime()
        rt._cached_position_qty = Decimal("0")
        rt.gradient_strategy.enabled = True
        rt.gradient_strategy.open_rows[0].threshold_pct = Decimal("0.1")
        rt.gradient_strategy.open_rows[0].target_qty = Decimal("0.05")
        placed = []
        rt._handle_new_gradient_signal = lambda sig: placed.append(sig) or "rec"

        for index, edge in enumerate((Decimal("0.20"), Decimal("0.28"), Decimal("0.21"))):
            rt._evaluate_gradient_signal(
                edge, Decimal("0"), Decimal("0"),
                active_quote_key=("session-volatile", index + 1),
                now_monotonic=100.0 + index * 0.2,
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
        runtime = VariationalToLighterRuntime(parse_args(["--browser-smoke-test"]))
        runtime.ticker = "BTC"
        runtime._lighter_positions["BTC"] = Decimal("0")
        runtime._lighter_position_ready.set()
        return runtime

    def test_window_stats_single_pass(self):
        rt = self._runtime()
        rt.variational_ticker = "BTC"
        for v in (0.05, 0.06, 0.07, 0.08, 0.09):
            rt.spread_store.record(asset="BTC", var_bid=None, var_ask=None, lighter_bid=None, lighter_ask=None, long_edge_pct=v, short_edge_pct=None)
        med, p90, p10 = rt._window_stats(3600, long_side=True)
        self.assertEqual(med, 0.07)
        self.assertEqual(p90, rt._percentile([0.05, 0.06, 0.07, 0.08, 0.09], 90))
        self.assertEqual(p10, rt._percentile([0.05, 0.06, 0.07, 0.08, 0.09], 10))

    async def test_header_places_both_positions_on_binance_line_only(self):
        rt = self._runtime()
        rt.current_page = 2
        rt.variational_ticker = "BTC"
        rt.ticker = "BTC"
        rt._cached_position_qty = Decimal("-0.011")
        rt._lighter_positions = {"BTC": Decimal("0.011")}
        rt._lighter_position_ready.set()
        rt.binance_usdc_bid = Decimal("1.00050")
        rt.binance_usdc_ask = Decimal("1.00051")
        rt.binance_usdc_updated_at = time.monotonic()
        console = Console(record=True, width=220, height=50)

        console.print(await rt.render_dashboard())
        lines = console.export_text().splitlines()
        binance_line = next(line for line in lines if "Binance USDC/USDT" in line)
        var_line = next(line for line in lines if "Var↔Lit" in line)

        self.assertIn("持仓-0.011BTC", binance_line)
        self.assertIn("Lit仓+0.011", binance_line)
        self.assertIn("累计已配对毛收益", var_line)
        self.assertNotIn("含未平单", var_line)
        self.assertNotIn("持仓", var_line)
        self.assertNotIn("Lit仓", var_line)

    def test_refresh_spread_stats_populates_cache(self):
        rt = self._runtime()
        rt.variational_ticker = "BTC"
        rt.spread_store.record(asset="BTC", var_bid=None, var_ask=None, lighter_bid=None, lighter_ask=None, long_edge_pct=0.07, short_edge_pct=0.03)
        rt._refresh_spread_stats()
        self.assertEqual(rt._spread_stats_cache["long_median_5m"], 0.07)
        self.assertEqual(rt._spread_stats_cache["short_median_1h"], 0.03)

    async def test_spread_stats_refresh_is_throttled_even_when_cache_is_empty(self):
        rt = self._runtime()
        calls = []

        def calculate(asset):
            calls.append(asset)
            return {}

        rt._calculate_spread_stats = calculate
        await rt._refresh_spread_stats_if_due("BTC")
        await rt._refresh_spread_stats_if_due("BTC")

        self.assertEqual(calls, ["BTC"])

    async def test_spread_stats_refreshes_immediately_after_market_switch(self):
        rt = self._runtime()
        calls = []

        def calculate(asset):
            calls.append(asset)
            return {"asset": asset}

        rt._calculate_spread_stats = calculate
        await rt._refresh_spread_stats_if_due("BTC")
        await rt._refresh_spread_stats_if_due("CL")

        self.assertEqual(calls, ["BTC", "CL"])
        self.assertEqual(rt._spread_stats_cache, {"asset": "CL"})

    async def test_slow_spread_stats_query_does_not_block_event_loop(self):
        rt = self._runtime()
        heartbeat_ran = asyncio.Event()

        def calculate(_asset):
            time.sleep(0.05)
            return {}

        async def heartbeat():
            await asyncio.sleep(0.005)
            heartbeat_ran.set()

        rt._calculate_spread_stats = calculate
        heartbeat_task = asyncio.create_task(heartbeat())
        await rt._refresh_spread_stats_if_due("BTC")
        await heartbeat_task

        self.assertTrue(heartbeat_ran.is_set())

    async def test_history_snapshot_is_cached_for_same_asset_and_width(self):
        rt = self._runtime()
        calls = []

        def load(asset, width):
            calls.append((asset, width))
            return [], ([None] * width, None, None)

        rt._load_history_snapshot = load
        await rt._refresh_history_cache_if_due("BTC", 80)
        await rt._refresh_history_cache_if_due("BTC", 80)

        self.assertEqual(calls, [("BTC", 80)])

    async def test_spread_sampling_persists_without_dashboard_render(self):
        rt = self._runtime()
        rt.variational_ticker = "BTC"
        rt.get_variational_best_bid_ask = AsyncMock(
            return_value=(Decimal("100"), Decimal("101"), "BTC")
        )
        rt.get_lighter_best_bid_ask = AsyncMock(
            return_value=(Decimal("102"), Decimal("103"))
        )
        rt.get_lighter_depth_quote = AsyncMock(
            return_value=(Decimal("102"), Decimal("103"))
        )
        rt._refresh_spread_stats_if_due = AsyncMock()
        recorded = []
        rt._record_cross_spreads = lambda *args: recorded.append(args)

        await rt._sample_current_spread()

        self.assertEqual(recorded[0][0], "BTC")
        self.assertIsNotNone(recorded[0][5])
        self.assertIsNotNone(recorded[0][6])
        rt._refresh_spread_stats_if_due.assert_awaited_once_with("BTC")

    async def test_strategy_and_history_use_separate_terminal_pages(self):
        rt = self._runtime()
        rt.variational_ticker = "BTC"
        rt.ticker = "BTC"
        rt._cached_position_qty = Decimal("0")
        rt.current_page = 2
        strategy_console = Console(record=True, width=160, height=50)
        strategy_console.print(await rt.render_dashboard())
        strategy_text = strategy_console.export_text()

        rt.current_page = 3
        history_console = Console(record=True, width=160, height=50)
        history_console.print(await rt.render_dashboard())
        history_text = history_console.export_text()

        self.assertIn("触发策略", strategy_text)
        self.assertNotIn("历史价差", strategy_text)
        self.assertIn("历史价差", history_text)

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
            lighter_filled_qty=Decimal("0.001"),
        )
        rt._maybe_record_slippage_stats(rec)
        rt._maybe_record_slippage_stats(rec)  # 去重：只记一次
        self.assertEqual(rt._stat_both_filled, 1)
        self.assertEqual(rt._stat_var_slip.n, 1)
        self.assertEqual(rt._stat_var_slip.last, 2.0)  # 做多 对api100→98 = +2%
        self.assertEqual(rt._stat_var_slip_dom.n, 1)
        self.assertEqual(rt._stat_var_slip_dom.last, float((Decimal("99") - Decimal("98")) / Decimal("99") * 100))
        self.assertEqual(rt._stat_lighter_slip.last, -2.0)  # 做空 100→98 = -2%
        self.assertEqual(rt._stat_long_fill_edge.n, 1)
        self.assertEqual(rt._stat_long_fill_edge.last, 0.0)
        self.assertEqual(rt._stat_short_fill_edge.n, 0)

        short = OrderLifecycle(
            trade_key="strategy:y",
            trade_id="",
            side="sell",
            qty=Decimal("0.001"),
            asset="XAU",
            auto_hedge_enabled=True,
            last_variational_status="filled",
            var_fill_price=Decimal("100"),
            lighter_fill_price=Decimal("99"),
            lighter_filled_qty=Decimal("0.001"),
        )
        rt._maybe_record_slippage_stats(short)
        self.assertEqual(rt._stat_short_fill_edge.n, 1)
        self.assertAlmostEqual(
            rt._stat_short_fill_edge.last,
            float(Decimal("200") * Decimal("-1") / Decimal("199")),
        )

    def test_stats_panel_omits_quote_transport_comparison(self):
        source = (Path(__file__).resolve().parents[1] / "main.py").read_text()
        self.assertNotIn("最近获取(+传输延迟)", source)
        self.assertNotIn("获取领先:", source)
        self.assertIn("V 滑点·200ms主动报价", source)
        self.assertIn("V 滑点·DOM约1s报价", source)
        self.assertIn("Long 实际成交Edge", source)
        self.assertIn("Short 实际成交Edge", source)

    def test_quantize_to_lighter_lot_floors_to_step(self):
        rt = self._runtime()
        rt.lighter_gateway.size_multiplier = 1000  # 最小步长 0.001
        self.assertEqual(rt._quantize_to_lighter_lot(Decimal("0.0015")), Decimal("0.001"))
        self.assertEqual(rt._quantize_to_lighter_lot(Decimal("0.012")), Decimal("0.012"))
        self.assertEqual(rt._quantize_to_lighter_lot(Decimal("0.0005")), Decimal("0"))

    def test_quantize_passthrough_without_multiplier(self):
        rt = self._runtime()
        rt.lighter_gateway.size_multiplier = None
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
                "kind": "fill",
                "client_order_index": 123,
                "price": "4000",
                "quantity": "0.01",
            }
        )

        self.assertNotIn(123, rt.lighter_client_order_to_trade_key)
        self.assertEqual(rt.records[key].lighter_fill_price, Decimal("4000"))

    async def test_rust_partial_fills_accumulate_vwap_until_complete(self):
        rt = self._runtime()
        key = "strategy:partial"
        rt.records[key] = OrderLifecycle(
            trade_key=key,
            trade_id="",
            side="buy",
            qty=Decimal("0.01"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
            var_fill_price=Decimal("99"),
            lighter_side="SELL",
        )
        rt._round_ledger_synced = True
        rt.lighter_client_order_to_trade_key[88] = key

        await rt.handle_lighter_fill_update(
            {"kind": "fill", "client_order_index": 88, "price": "100", "quantity": "0.004"}
        )
        self.assertIn(88, rt.lighter_client_order_to_trade_key)
        self.assertIsNone(rt.records[key].lighter_fill_ts_iso)
        self.assertEqual(rt._stat_both_filled, 0)
        self.assertFalse(rt.records[key].slippage_recorded)
        self.assertEqual(rt.round_exit_ledger.position_qty, Decimal("0"))

        await rt.handle_lighter_fill_update(
            {"kind": "fill", "client_order_index": 88, "price": "110", "quantity": "0.006"}
        )
        self.assertNotIn(88, rt.lighter_client_order_to_trade_key)
        self.assertEqual(rt.records[key].lighter_fill_price, Decimal("106"))
        self.assertIsNotNone(rt.records[key].lighter_fill_ts_iso)
        self.assertEqual(rt._stat_both_filled, 1)
        self.assertTrue(rt.records[key].slippage_recorded)
        self.assertEqual(rt.round_exit_ledger.position_qty, Decimal("0.01"))

    def test_dashboard_cumulative_profit_includes_only_realtime_paired_quantity(self):
        rt = self._runtime()
        rt.ticker = "BTC"
        rt.round_exit_ledger.apply_fill(
            "buy",
            Decimal("0.002"),
            Decimal("1"),
            unit_spread=Decimal("1"),
        )

        console = Console(record=True, width=120)
        console.print(rt._render_stats_panel(is_zh=True))
        open_text = console.export_text()

        self.assertIn("累计已配对毛收益: 0 | 已配对数量 0 BTC", open_text)
        self.assertNotIn("+0.0020u", open_text)

        rt.round_exit_ledger.apply_fill(
            "sell",
            Decimal("0.001"),
            Decimal("0.5"),
            unit_spread=Decimal("-0.5"),
        )
        console = Console(record=True, width=120)
        console.print(rt._render_stats_panel(is_zh=True))
        partially_paired_text = console.export_text()

        self.assertEqual(rt.round_exit_ledger.position_qty, Decimal("0.001"))
        self.assertIn("累计已配对毛收益: +0.0005u", partially_paired_text)
        self.assertIn("已配对数量 0.001 BTC", partially_paired_text)

        rt.round_exit_ledger.apply_fill(
            "sell",
            Decimal("0.001"),
            Decimal("0.5"),
            unit_spread=Decimal("-0.5"),
        )
        console = Console(record=True, width=120)
        console.print(rt._render_stats_panel(is_zh=True))
        fully_paired_text = console.export_text()

        self.assertIn("累计已配对毛收益: +0.0010u", fully_paired_text)
        self.assertIn("已配对数量 0.002 BTC", fully_paired_text)

    def test_record_cache_releases_old_completed_lifecycle_objects(self):
        rt = self._runtime()
        for index in range(550):
            key = f"strategy:{index}"
            record = OrderLifecycle(
                trade_key=key,
                trade_id="",
                side="buy",
                qty=Decimal("0.001"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
            )
            rt._remember_record(key, record)

        self.assertEqual(len(rt.record_order), 500)
        self.assertEqual(len(rt.records), 500)
        self.assertNotIn("strategy:0", rt.records)

    async def test_immediate_lighter_fill_is_mapped_before_create_order_returns(self):
        rt = self._runtime()
        rt.args.auto_hedge = True
        rt.lighter_gateway.size_multiplier = 1000
        rt.lighter_market_index = 1
        rt.ticker = "BTC"

        async def best_prices():
            return Decimal("66000"), Decimal("66001")

        rt.get_lighter_best_bid_ask = best_prices
        record = OrderLifecycle(
            trade_key="strategy:immediate-fill",
            trade_id="",
            side="buy",
            qty=Decimal("0.001"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
        )
        rt.records[record.trade_key] = record
        rt.record_order.append(record.trade_key)
        logged_events = []

        async def append_order_log(event_type, payload):
            logged_events.append((event_type, payload))

        rt.append_order_log = append_order_log

        class ImmediateFillGateway:
            async def place_order(self, **kwargs):
                await rt.handle_lighter_fill_update({
                    "kind": "fill",
                    "client_order_index": kwargs["client_order_index"],
                    "price": "66200",
                    "quantity": "0.001",
                })
                return "tx-test"

        rt.lighter_gateway = ImmediateFillGateway()

        await rt.place_lighter_order(record)

        self.assertEqual(record.lighter_fill_price, Decimal("66200"))
        self.assertIsNotNone(record.lighter_fill_ts_iso)
        self.assertNotIn(record.lighter_client_order_id, rt.lighter_client_order_to_trade_key)
        submitted = next(payload for event, payload in logged_events if event == "lighter_submitted")
        self.assertEqual(submitted["lighter_limit_price"], "65340.00")
        self.assertEqual(submitted["lighter_tx_hash"], "tx-test")

    async def test_lighter_submit_failure_hard_stops_strategy(self):
        rt = self._runtime()
        rt.args.auto_hedge = True

        async def best_prices():
            return Decimal("66000"), Decimal("66001")

        class FailingGateway:
            async def place_order(self, **_kwargs):
                raise RuntimeError("sendtx rejected")

        rt.get_lighter_best_bid_ask = best_prices
        rt.lighter_gateway = FailingGateway()
        record = OrderLifecycle(
            trade_key="strategy:lighter-failed", trade_id="", side="buy",
            qty=Decimal("0.001"), asset="BTC", auto_hedge_enabled=True,
            last_variational_status="strategy_submitted",
        )

        await rt.place_lighter_order(record)

        self.assertTrue(rt._strategy_halted)
        self.assertIn("Lighter 对冲提交失败", rt._halt_reason)

    async def test_variational_submit_failure_hard_stops_strategy(self):
        rt = self._runtime()
        rt.browser_order_broker.place_order = AsyncMock(return_value={"ok": False, "error": "rejected"})
        rt._strategy_order_in_flight = True

        await rt._send_browser_order(BrowserOrderCommand(
            side="buy", qty=Decimal("0.001"), dry_run=False,
            trade_key="strategy:var-failed",
        ))

        self.assertTrue(rt._strategy_halted)
        self.assertIn("Variational 提交失败", rt._halt_reason)
        self.assertFalse(rt._strategy_order_in_flight)

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
            {"kind": "fill", "client_order_index": 7, "price": "4000", "quantity": "0.01"}
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

    async def test_rust_position_event_updates_authoritative_position(self):
        rt = self._runtime()
        await rt._handle_lighter_rust_event(
            {"type": "position", "symbol": "BTC", "market_id": 1, "quantity": "-0.004"}
        )
        self.assertEqual(rt._lighter_positions["BTC"], Decimal("-0.004"))

    async def test_rust_gateway_disconnect_halts_auto_hedge_and_clears_book(self):
        rt = self._runtime()
        rt.args.auto_hedge = True
        rt.stop_flag = False
        rt.lighter_order_book_ready = True
        rt.lighter_best_bid = Decimal("100")
        rt.lighter_best_ask = Decimal("101")

        await rt._handle_lighter_rust_event(
            {"type": "health", "ready": False, "error": "gateway exited with code 1"}
        )

        self.assertTrue(rt._strategy_halted)
        self.assertIn("Lighter Rust断开", rt._halt_reason)
        self.assertFalse(rt.lighter_order_book_ready)

    def test_positions_balanced_requires_opposite_legs(self):
        rt = self._runtime()
        rt.lighter_gateway.size_multiplier = 1000  # 容差 0.001
        rt.ticker = "BTC"
        rt._cached_position_qty = Decimal("0.01")  # Var 多
        rt._lighter_positions = {"BTC": Decimal("-0.01")}  # Lighter 空
        rt._lighter_position_ready.set()
        self.assertTrue(rt._positions_balanced())
        rt._lighter_positions = {"BTC": Decimal("0")}  # 裸腿
        self.assertFalse(rt._positions_balanced())

    def test_unknown_lighter_position_is_never_treated_as_zero(self):
        rt = self._runtime()
        rt.ticker = "BTC"
        rt._cached_position_qty = Decimal("0")
        rt._lighter_positions.clear()
        rt._lighter_position_ready.clear()

        self.assertFalse(rt._current_lighter_position_known())
        self.assertFalse(rt._positions_balanced())

    def test_strategy_order_allowed_gate(self):
        rt = self._runtime()
        rt.lighter_gateway.size_multiplier = 1000
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
        rt.lighter_gateway.size_multiplier = 1000
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

    async def test_warm_lighter_starts_readonly_gateway_without_hedge(self):
        rt = self._runtime()
        rt.args.auto_hedge = False
        called = []

        async def start():
            called.append(True)

        rt.lighter_gateway.start = start
        await rt.warm_lighter()
        self.assertTrue(rt._lighter_ready)
        self.assertEqual(called, [True])

    async def test_warm_lighter_sets_ready_on_success(self):
        rt = self._runtime()
        rt.args.auto_hedge = True
        rt.lighter_gateway.start = AsyncMock()
        await rt.warm_lighter()
        self.assertTrue(rt._lighter_ready)

    async def test_confirm_hedge_ok_when_balanced(self):
        rt = self._runtime()
        rt.lighter_gateway.size_multiplier = 1000
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
