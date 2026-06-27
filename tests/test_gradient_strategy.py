import unittest
from decimal import Decimal

from variational.gradient_strategy import EditableField, GradientStrategyState, StrategySection


class GradientStrategyStateTest(unittest.TestCase):
    def test_default_rows_and_cursor_editing(self):
        state = GradientStrategyState.default()

        self.assertEqual(len(state.open_rows), 1)
        self.assertEqual(len(state.close_rows), 1)
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

    def test_add_delete_and_navigation(self):
        state = GradientStrategyState.default()

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

        signal = state.evaluate(
            open_spread_pct=Decimal("0.12"),
            close_spread_pct=Decimal("0.00"),
            current_position_qty=Decimal("0.001"),
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "open")
        self.assertEqual(signal.delta_qty, Decimal("0.001"))
        self.assertEqual(signal.target_qty, Decimal("0.002"))

    def test_close_signal_fires_on_downward_crossing(self):
        state = GradientStrategyState.default()
        state.close_rows[0].threshold_pct = Decimal("0.09")
        state.close_rows[0].target_qty = Decimal("0.002")
        state.add_row(StrategySection.CLOSE)
        state.close_rows[1].threshold_pct = Decimal("0.08")
        state.close_rows[1].target_qty = Decimal("0.001")
        state.add_row(StrategySection.CLOSE)
        state.close_rows[2].threshold_pct = Decimal("0.07")
        state.close_rows[2].target_qty = Decimal("0")

        state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("0.10"),
            current_position_qty=Decimal("0.003"),
        )
        signal = state.evaluate(
            open_spread_pct=Decimal("0.00"),
            close_spread_pct=Decimal("0.07"),
            current_position_qty=Decimal("0.003"),
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "close")
        self.assertEqual(signal.delta_qty, Decimal("0.003"))
        self.assertEqual(signal.target_qty, Decimal("0"))

    def test_close_does_not_fire_before_crossing(self):
        state = GradientStrategyState.default()
        state.open_rows[0].threshold_pct = Decimal("0.11")
        state.open_rows[0].target_qty = Decimal("0.001")
        state.close_rows[0].threshold_pct = Decimal("0.07")
        state.close_rows[0].target_qty = Decimal("0")

        signal = state.evaluate(
            open_spread_pct=Decimal("0.12"),
            close_spread_pct=Decimal("0.00"),
            current_position_qty=Decimal("0.001"),
        )

        self.assertIsNone(signal)


if __name__ == "__main__":
    unittest.main()
