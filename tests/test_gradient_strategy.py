import unittest
from decimal import Decimal

from variational.gradient_strategy import CursorTarget, EditableField, GradientStrategyState, StrategySection


class GradientStrategyStateTest(unittest.TestCase):
    def test_default_rows_and_cursor_editing(self):
        state = GradientStrategyState.default()

        self.assertEqual(len(state.open_rows), 1)
        self.assertEqual(len(state.close_rows), 1)
        self.assertEqual(state.single_order_qty, Decimal("0.001"))
        self.assertEqual(state.cursor_target, CursorTarget.ENABLED)
        self.assertFalse(state.enabled)

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
        state.handle_key("\r")
        state.open_rows[0].threshold_pct = Decimal("0.11")
        state.open_rows[0].target_qty = Decimal("0.001")
        state.add_row(StrategySection.OPEN)
        state.open_rows[1].threshold_pct = Decimal("0.12")
        state.open_rows[1].target_qty = Decimal("0.002")

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

    def test_close_fires_when_spread_at_or_above_threshold(self):
        # 阈值 -0.1：价差 -0.08（≥ -0.1）→ 平；无需穿越。
        state = GradientStrategyState.default()
        state.handle_key("\r")
        state.close_rows[0].threshold_pct = Decimal("-0.1")
        state.close_rows[0].target_qty = Decimal("0")

        signal = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("-0.08"),
            current_position_qty=Decimal("0.05"),
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "close")
        self.assertEqual(signal.delta_qty, Decimal("0.05"))
        self.assertEqual(signal.target_qty, Decimal("0"))

    def test_close_holds_when_spread_below_threshold(self):
        # 阈值 -0.1：价差 -0.12（< -0.1）→ 不平。
        state = GradientStrategyState.default()
        state.handle_key("\r")
        state.close_rows[0].threshold_pct = Decimal("-0.1")
        state.close_rows[0].target_qty = Decimal("0")

        signal = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("-0.12"),
            current_position_qty=Decimal("0.05"),
        )

        self.assertIsNone(signal)

    def test_close_gradient_picks_highest_satisfied_threshold(self):
        state = GradientStrategyState.default()
        state.handle_key("\r")
        state.close_rows[0].threshold_pct = Decimal("0.07")
        state.close_rows[0].target_qty = Decimal("0.002")
        state.add_row(StrategySection.CLOSE)
        state.close_rows[1].threshold_pct = Decimal("0.08")
        state.close_rows[1].target_qty = Decimal("0.001")
        state.add_row(StrategySection.CLOSE)
        state.close_rows[2].threshold_pct = Decimal("0.09")
        state.close_rows[2].target_qty = Decimal("0")

        # 只满足最低阈值 0.07 → target 0.002（平一点）
        shallow = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("0.075"),
            current_position_qty=Decimal("0.003"),
        )
        self.assertIsNotNone(shallow)
        self.assertEqual(shallow.target_qty, Decimal("0.002"))
        self.assertEqual(shallow.delta_qty, Decimal("0.001"))

        # 价差更高，满足全部 → 取最高阈值 0.09 → target 0（平更多）
        deep = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("0.10"),
            current_position_qty=Decimal("0.003"),
        )
        self.assertIsNotNone(deep)
        self.assertEqual(deep.target_qty, Decimal("0"))
        self.assertEqual(deep.delta_qty, Decimal("0.003"))
    def test_open_signal_uses_signed_position(self):
        state = GradientStrategyState.default()
        state.handle_key("\r")
        state.open_rows[0].threshold_pct = Decimal("0.11")
        state.open_rows[0].target_qty = Decimal("0.1")

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
        state.handle_key("\r")
        state.close_rows[0].threshold_pct = Decimal("0.07")
        state.close_rows[0].target_qty = Decimal("-0.1")

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
        state.handle_key("\r")
        state.close_rows[0].threshold_pct = Decimal("0.07")
        state.close_rows[0].target_qty = Decimal("-0.1")

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


if __name__ == "__main__":
    unittest.main()
