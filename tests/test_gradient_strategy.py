import unittest
from decimal import Decimal

from variational.gradient_strategy import (
    CursorTarget,
    EditableField,
    GradientRow,
    GradientStrategyState,
    StrategySection,
)


class GradientStrategyStateTest(unittest.TestCase):
    @staticmethod
    def _complete_minimum_config(state):
        state.open_rows[0].threshold_pct = Decimal("0.06")
        state.open_rows[0].target_qty = Decimal("0.005")

    def test_strategy_cannot_enable_without_a_complete_gradient(self):
        state = GradientStrategyState.default()

        state.handle_key("\r")

        self.assertFalse(state.enabled)
        self.assertIn("至少配置一条", state.validation_error)

    def test_duplicate_threshold_and_position_are_rejected(self):
        state = GradientStrategyState.default()
        self._complete_minimum_config(state)
        state.add_row(StrategySection.OPEN)
        state.open_rows[1].threshold_pct = Decimal("0.06")
        state.open_rows[1].target_qty = Decimal("0.005")

        errors = state.validation_errors()

        self.assertTrue(any("重复阈值" in error for error in errors))
        self.assertTrue(any("重复目标仓位" in error for error in errors))

    def test_long_targets_must_strictly_increase_with_threshold(self):
        state = GradientStrategyState.default()
        self._complete_minimum_config(state)
        state.add_row(StrategySection.OPEN)
        state.open_rows[1].threshold_pct = Decimal("0.07")
        state.open_rows[1].target_qty = Decimal("0.004")

        self.assertTrue(any("Long" in error and "严格增大" in error for error in state.validation_errors()))

    def test_short_targets_must_strictly_decrease_as_threshold_falls(self):
        state = GradientStrategyState.default()
        self._complete_minimum_config(state)
        state.close_rows[0].threshold_pct = Decimal("0.05")
        state.close_rows[0].target_qty = Decimal("-0.005")
        state.add_row(StrategySection.CLOSE)
        state.close_rows[1].threshold_pct = Decimal("0.04")
        state.close_rows[1].target_qty = Decimal("-0.004")

        state.cursor_target = CursorTarget.ENABLED
        state.handle_key("\r")

        self.assertFalse(state.enabled)
        self.assertIn("严格减小", state.validation_error)

    def test_valid_monotonic_gradients_can_enable(self):
        state = GradientStrategyState.default()
        self._complete_minimum_config(state)
        state.add_row(StrategySection.OPEN)
        state.open_rows[1].threshold_pct = Decimal("0.07")
        state.open_rows[1].target_qty = Decimal("0.01")
        state.close_rows[0].threshold_pct = Decimal("0.05")
        state.close_rows[0].target_qty = Decimal("-0.005")
        state.add_row(StrategySection.CLOSE)
        state.close_rows[1].threshold_pct = Decimal("0.04")
        state.close_rows[1].target_qty = Decimal("-0.01")

        state.cursor_target = CursorTarget.ENABLED
        state.handle_key("\r")

        self.assertTrue(state.enabled)
        self.assertEqual(state.validation_error, "")

    def test_long_and_short_edges_resolve_one_signed_target_position(self):
        state = GradientStrategyState.default()
        state.enabled = True
        state.open_rows[0].threshold_pct = Decimal("0.6")
        state.open_rows[0].target_qty = Decimal("0.1")
        state.add_row(StrategySection.OPEN)
        state.open_rows[1].threshold_pct = Decimal("0.7")
        state.open_rows[1].target_qty = Decimal("0.2")
        state.close_rows[0].threshold_pct = Decimal("0.4")
        state.close_rows[0].target_qty = Decimal("0")
        state.add_row(StrategySection.CLOSE)
        state.close_rows[1].threshold_pct = Decimal("0.3")
        state.close_rows[1].target_qty = Decimal("-0.1")

        long_signal = state.evaluate(Decimal("0.65"), Decimal("0.5"), Decimal("0"))
        self.assertIsNotNone(long_signal)
        self.assertEqual(long_signal.section, StrategySection.OPEN)
        self.assertEqual(long_signal.target_qty, Decimal("0.1"))
        self.assertEqual(long_signal.action, "open")

        flat_signal = state.evaluate(Decimal("0.5"), Decimal("0.35"), Decimal("0.1"))
        self.assertIsNotNone(flat_signal)
        self.assertEqual(flat_signal.section, StrategySection.CLOSE)
        self.assertEqual(flat_signal.target_qty, Decimal("0"))
        self.assertEqual(flat_signal.action, "close")

        short_signal = state.evaluate(Decimal("0.2"), Decimal("0.25"), Decimal("0"))
        self.assertIsNotNone(short_signal)
        self.assertEqual(short_signal.section, StrategySection.CLOSE)
        self.assertEqual(short_signal.target_qty, Decimal("-0.1"))
        self.assertEqual(short_signal.action, "close")

        conflict = state.evaluate(Decimal("0.65"), Decimal("0.2"), Decimal("0"))
        self.assertIsNone(conflict)

    def test_short_edge_cannot_increase_position_to_close_existing_short(self):
        state = GradientStrategyState.default()
        state.enabled = True
        state.close_rows[0].threshold_pct = Decimal("0.4")
        state.close_rows[0].target_qty = Decimal("0")

        signal = state.evaluate(Decimal("0.1"), Decimal("0.35"), Decimal("-0.1"))

        self.assertIsNone(signal)

    def test_long_edge_cannot_reduce_an_existing_long_position(self):
        state = GradientStrategyState.default()
        state.enabled = True
        state.open_rows[0].threshold_pct = Decimal("0.7")
        state.open_rows[0].target_qty = Decimal("0.1")
        state.close_rows[0].threshold_pct = Decimal("0.6")
        state.close_rows[0].target_qty = Decimal("0")

        signal = state.evaluate(Decimal("0.8"), Decimal("0.8"), Decimal("0.2"))

        self.assertIsNone(signal)

    def test_short_edge_can_sell_across_zero_to_negative_target(self):
        state = GradientStrategyState.default()
        state.enabled = True
        state.close_rows[0].threshold_pct = Decimal("0.6")
        state.close_rows[0].target_qty = Decimal("-0.2")

        signal = state.evaluate(Decimal("0.1"), Decimal("0.5"), Decimal("0.2"))

        self.assertIsNotNone(signal)
        self.assertEqual(signal.section, StrategySection.CLOSE)
        self.assertEqual(signal.target_qty, Decimal("-0.2"))
        self.assertEqual(signal.delta_qty, Decimal("0.4"))

    def test_long_edge_can_buy_across_zero_to_positive_target(self):
        state = GradientStrategyState.default()
        state.enabled = True
        state.open_rows[0].threshold_pct = Decimal("0.7")
        state.open_rows[0].target_qty = Decimal("0.2")

        signal = state.evaluate(Decimal("0.8"), Decimal("0.9"), Decimal("-0.2"))

        self.assertIsNotNone(signal)
        self.assertEqual(signal.section, StrategySection.OPEN)
        self.assertEqual(signal.target_qty, Decimal("0.2"))
        self.assertEqual(signal.delta_qty, Decimal("0.4"))

    def test_default_rows_and_cursor_editing(self):
        state = GradientStrategyState.default()

        self.assertEqual(len(state.open_rows), 1)
        self.assertEqual(len(state.close_rows), 1)
        self.assertEqual(state.single_order_qty, Decimal("0.001"))
        self.assertEqual(state.cursor_target, CursorTarget.ENABLED)
        self.assertFalse(state.enabled)

        self._complete_minimum_config(state)
        state.handle_key("\r")
        self.assertTrue(state.enabled)
        state.handle_key("\r")
        self.assertFalse(state.enabled)
        state.handle_key("\x1b[B")
        state.handle_key("\x1b[B")

        self.assertEqual(state.cursor_section, StrategySection.OPEN)
        self.assertEqual(state.cursor_index, 0)
        self.assertEqual(state.cursor_field, EditableField.THRESHOLD)

        state.handle_key("1")
        state.handle_key(".")
        state.handle_key("1")
        state.handle_key("1")
        state.handle_key("\r")
        state.handle_key("\x1b[C")
        state.handle_key("0")
        state.handle_key(".")
        state.handle_key("0")
        state.handle_key("0")
        state.handle_key("1")
        state.handle_key("\r")

        self.assertEqual(state.open_rows[0].threshold_pct, Decimal("1.11"))
        self.assertEqual(state.open_rows[0].target_qty, Decimal("0.001"))

    def test_single_order_qty_is_editable_before_gradient_rows(self):
        state = GradientStrategyState.default()

        state.handle_key("\x1b[B")
        self.assertEqual(state.cursor_target, CursorTarget.ORDER_SIZE)

        state.handle_key("0")
        state.handle_key(".")
        state.handle_key("0")
        state.handle_key("0")
        state.handle_key("2")
        state.handle_key("\r")

        self.assertEqual(state.single_order_qty, Decimal("0.002"))

        state.handle_key("\x1b[B")
        self.assertEqual(state.cursor_target, CursorTarget.ROW)
        self.assertEqual(state.cursor_section, StrategySection.OPEN)
        self.assertEqual(state.cursor_index, 0)

    def test_threshold_slash_enters_negative_value(self):
        state = GradientStrategyState.default()
        state.handle_key("\x1b[B")
        state.handle_key("\x1b[B")

        state.handle_key("/")
        self.assertEqual(state.display_value(StrategySection.OPEN, 0, EditableField.THRESHOLD), "-")

        state.handle_key("0")
        state.handle_key(".")
        state.handle_key("1")
        state.handle_key("\r")

        self.assertEqual(state.open_rows[0].threshold_pct, Decimal("-0.1"))
        self.assertEqual(len(state.open_rows), 1)

    def test_minus_still_deletes_gradient_row(self):
        state = GradientStrategyState.default()
        state.handle_key("\x1b[B")
        state.handle_key("\x1b[B")
        state.handle_key("+")

        state.handle_key("-")

        self.assertEqual(len(state.open_rows), 1)

    def test_enabled_row_ignores_editing_keys(self):
        state = GradientStrategyState.default()

        state.handle_key("+")
        state.handle_key("-")
        state.handle_key("1")
        state.handle_key(".")
        state.handle_key("\x7f")
        state.handle_key("\x1b[B")
        state.handle_key("\x1b[B")

        self.assertEqual(len(state.open_rows), 1)
        self.assertEqual(len(state.close_rows), 1)
        self.assertEqual(state.cursor_target, CursorTarget.ROW)
        self.assertEqual(state.cursor_section, StrategySection.OPEN)
        self.assertEqual(state.cursor_index, 0)
        self.assertIsNone(state.edit_buffer)

    def test_add_delete_and_navigation(self):
        state = GradientStrategyState.default()
        state.handle_key("\x1b[B")
        state.handle_key("\x1b[B")

        state.handle_key("+")
        self.assertEqual(len(state.open_rows), 2)
        self.assertEqual(state.cursor_index, 1)

        state.handle_key("\x1b[B")
        self.assertEqual(state.cursor_section, StrategySection.CLOSE)
        self.assertEqual(state.cursor_index, 0)

        state.handle_key("-")
        self.assertEqual(len(state.close_rows), 1)

    def test_open_signal_uses_target_position_delta(self):
        state = GradientStrategyState.default()
        state.open_rows[0].threshold_pct = Decimal("0.11")
        state.open_rows[0].target_qty = Decimal("0.001")
        state.add_row(StrategySection.OPEN)
        state.open_rows[1].threshold_pct = Decimal("0.12")
        state.open_rows[1].target_qty = Decimal("0.002")
        state.enabled = True

        signal = state.evaluate(
            open_spread_pct=Decimal("0.12"),
            close_spread_pct=Decimal("0.00"),
            current_position_qty=Decimal("0.001"),
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "open")
        self.assertEqual(signal.delta_qty, Decimal("0.001"))
        self.assertEqual(signal.target_qty, Decimal("0.002"))

    def test_strategy_is_disabled_by_default(self):
        state = GradientStrategyState.default()
        state.open_rows[0].threshold_pct = Decimal("0.11")
        state.open_rows[0].target_qty = Decimal("0.001")

        signal = state.evaluate(
            open_spread_pct=Decimal("0.12"),
            close_spread_pct=Decimal("0.00"),
            current_position_qty=Decimal("0"),
        )

        self.assertIsNone(signal)

    def test_short_fires_when_edge_at_or_below_threshold(self):
        # Short 阈值 -0.1：Edge -0.12（≤ -0.1）→ 命中。
        state = GradientStrategyState.default()
        state.close_rows[0].threshold_pct = Decimal("-0.1")
        state.close_rows[0].target_qty = Decimal("0")
        state.enabled = True

        signal = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("-0.12"),
            current_position_qty=Decimal("0.05"),
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "close")
        self.assertEqual(signal.delta_qty, Decimal("0.05"))
        self.assertEqual(signal.target_qty, Decimal("0"))

    def test_short_holds_when_edge_above_threshold(self):
        # Short 阈值 -0.1：Edge -0.08（> -0.1）→ 不命中。
        state = GradientStrategyState.default()
        state.close_rows[0].threshold_pct = Decimal("-0.1")
        state.close_rows[0].target_qty = Decimal("0")
        state.enabled = True

        signal = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("-0.08"),
            current_position_qty=Decimal("0.05"),
        )

        self.assertIsNone(signal)

    def test_short_gradient_picks_lower_level_as_edge_falls(self):
        state = GradientStrategyState.default()
        self._complete_minimum_config(state)
        state.handle_key("\r")
        state.close_rows[0].threshold_pct = Decimal("0.4")
        state.close_rows[0].target_qty = Decimal("0")
        state.add_row(StrategySection.CLOSE)
        state.close_rows[1].threshold_pct = Decimal("0.3")
        state.close_rows[1].target_qty = Decimal("-0.1")

        flat = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("0.35"),
            current_position_qty=Decimal("0.1"),
        )
        self.assertIsNotNone(flat)
        self.assertEqual(flat.target_qty, Decimal("0"))
        self.assertEqual(flat.delta_qty, Decimal("0.1"))

        short = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("0.25"),
            current_position_qty=Decimal("0"),
        )
        self.assertIsNotNone(short)
        self.assertEqual(short.target_qty, Decimal("-0.1"))
        self.assertEqual(short.delta_qty, Decimal("0.1"))
    def test_open_signal_uses_signed_position(self):
        state = GradientStrategyState.default()
        state.open_rows[0].threshold_pct = Decimal("0.11")
        state.open_rows[0].target_qty = Decimal("0.1")
        state.enabled = True

        signal = state.evaluate(
            open_spread_pct=Decimal("0.12"),
            close_spread_pct=Decimal("0.00"),
            current_position_qty=Decimal("-0.05"),
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "open")
        self.assertEqual(signal.delta_qty, Decimal("0.15"))

    def test_close_can_go_short_to_negative_target(self):
        state = GradientStrategyState.default()
        state.close_rows[0].threshold_pct = Decimal("0.07")
        state.close_rows[0].target_qty = Decimal("-0.1")
        state.enabled = True

        signal = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("0.07"),
            current_position_qty=Decimal("0"),
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "close")
        self.assertEqual(signal.target_qty, Decimal("-0.1"))
        self.assertEqual(signal.delta_qty, Decimal("0.1"))

    def test_close_stops_at_negative_target(self):
        state = GradientStrategyState.default()
        state.close_rows[0].threshold_pct = Decimal("0.07")
        state.close_rows[0].target_qty = Decimal("-0.1")
        state.enabled = True

        signal = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("0.07"),
            current_position_qty=Decimal("-0.1"),
        )

        self.assertIsNone(signal)

    def test_close_target_accepts_negative_input(self):
        state = GradientStrategyState.default()
        state.cursor_target = CursorTarget.ROW
        state.cursor_section = StrategySection.CLOSE
        state.cursor_index = 0
        state.cursor_field = EditableField.QUANTITY

        for key in ("/", "0", ".", "1"):
            state.handle_key(key)
        state.handle_key("\r")

        self.assertEqual(state.close_rows[0].target_qty, Decimal("-0.1"))

    def test_round_exit_target_uses_current_short_ladder_limit(self):
        state = GradientStrategyState.default()
        state.close_rows = [
            GradientRow(Decimal("0.05"), Decimal("-0.005")),
            GradientRow(Decimal("0.045"), Decimal("-0.010")),
            GradientRow(Decimal("0.04"), Decimal("-0.015")),
        ]
        self.assertEqual(
            state.round_exit_target(Decimal("0.03"), Decimal("0.0489"), Decimal("-0.015")),
            Decimal("-0.005"),
        )

    def test_round_exit_target_uses_current_long_ladder_limit(self):
        state = GradientStrategyState.default()
        state.open_rows = [
            GradientRow(Decimal("0.06"), Decimal("0.005")),
            GradientRow(Decimal("0.065"), Decimal("0.010")),
            GradientRow(Decimal("0.07"), Decimal("0.015")),
        ]
        self.assertEqual(
            state.round_exit_target(Decimal("0.062"), Decimal("0.08"), Decimal("0.015")),
            Decimal("0.005"),
        )


if __name__ == "__main__":
    unittest.main()
