import unittest
from decimal import Decimal

from variational.round_exit_strategy import RoundExitConfig, RoundExitLedger, RoundStateError


D = Decimal


class RoundExitLedgerTest(unittest.TestCase):
    def ledger(self, order_qty="0.001"):
        return RoundExitLedger(RoundExitConfig(D(order_qty)))

    def test_flat_has_no_edges_and_no_guard(self):
        ledger = self.ledger()
        self.assertIsNone(ledger.entry_edge_actual)
        self.assertIsNone(ledger.close_edge_actual)
        self.assertFalse(ledger.guard_started)
        self.assertEqual(ledger.decision(D("0.1")).reason, "flat")

    def test_short_and_long_entry_costs_are_quantity_weighted(self):
        short = self.ledger()
        short.apply_fill("sell", D("0.001"), D("0.04"), normal_close_threshold=D("0.06"))
        short.apply_fill("sell", D("0.003"), D("0.05"))
        self.assertEqual(short.entry_edge_actual, D("0.0475"))

        long = self.ledger()
        long.apply_fill("buy", D("0.001"), D("0.06"), normal_close_threshold=D("0.03"))
        long.apply_fill("buy", D("0.003"), D("0.04"))
        self.assertEqual(long.entry_edge_actual, D("0.045"))

    def test_first_exit_can_trigger_from_current_executable_edge(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.003"), D("0.042"), normal_close_threshold=D("0.06"))
        self.assertIsNone(ledger.close_edge_actual)
        self.assertEqual(ledger.decision(D("0.052")).action, "close")

    def test_actual_close_average_uses_closed_quantity(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.004"), D("0.042"), normal_close_threshold=D("0.06"))
        ledger.apply_fill("buy", D("0.001"), D("0.05"))
        ledger.apply_fill("buy", D("0.002"), D("0.056"))
        self.assertEqual(ledger.close_edge_actual, D("0.054"))

    def test_short_guard_starts_at_one_basis_point_profit(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.003"), D("0.042"), normal_close_threshold=D("0.06"))
        ledger.apply_fill("buy", D("0.001"), D("0.052"))
        self.assertTrue(ledger.guard_started)

    def test_long_guard_starts_at_one_basis_point_profit(self):
        ledger = self.ledger()
        ledger.apply_fill("buy", D("0.003"), D("0.050"), normal_close_threshold=D("0.03"))
        ledger.apply_fill("sell", D("0.001"), D("0.040"))
        self.assertTrue(ledger.guard_started)

    def test_less_than_one_basis_point_is_not_enough(self):
        short = self.ledger()
        short.apply_fill("sell", D("0.002"), D("0.042"), normal_close_threshold=D("0.06"))
        short.apply_fill("buy", D("0.001"), D("0.0519"))
        self.assertFalse(short.guard_started)

        long = self.ledger()
        long.apply_fill("buy", D("0.002"), D("0.050"), normal_close_threshold=D("0.03"))
        long.apply_fill("sell", D("0.001"), D("0.0401"))
        self.assertFalse(long.guard_started)

    def test_minimum_profit_is_independent_from_gradient_threshold(self):
        short = self.ledger()
        short.apply_fill("sell", D("0.002"), D("0.0448"), normal_close_threshold=D("0.05"))
        short.apply_fill("buy", D("0.001"), D("0.0548"))
        self.assertTrue(short.guard_started)

    def test_short_uses_current_executable_edge_not_projected_fill_average(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.003"), D("0.042"), normal_close_threshold=D("0.06"))
        ledger.apply_fill("buy", D("0.001"), D("0.030"))
        self.assertEqual(ledger.decision(D("0.0519")).action, "wait")
        decision = ledger.decision(D("0.052"))
        self.assertEqual(decision.action, "close")
        self.assertEqual(decision.projected_close_edge, D("0.052"))

    def test_short_waits_when_current_edge_has_not_reached_one_basis_point(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.003"), D("0.042"), normal_close_threshold=D("0.06"))
        ledger.apply_fill("buy", D("0.001"), D("0.052"))
        decision = ledger.decision(D("0.0519"))
        self.assertEqual(decision.action, "wait")
        self.assertEqual(decision.reason, "minimum_profit_not_reached")

    def test_long_projected_average_is_symmetric(self):
        ledger = self.ledger()
        ledger.apply_fill("buy", D("0.003"), D("0.050"), normal_close_threshold=D("0.03"))
        ledger.apply_fill("sell", D("0.001"), D("0.049"))
        allowed = ledger.decision(D("0.040"))
        blocked = ledger.decision(D("0.0401"))
        self.assertEqual(allowed.action, "close")
        self.assertEqual(allowed.projected_close_edge, D("0.040"))
        self.assertEqual(blocked.reason, "minimum_profit_not_reached")

    def test_same_direction_entry_after_close_updates_cost_and_position(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.003"), D("0.042"), normal_close_threshold=D("0.06"))
        ledger.apply_fill("buy", D("0.001"), D("0.040"))
        ledger.apply_fill("sell", D("0.001"), D("0.03"))
        self.assertEqual(ledger.position_qty, D("-0.003"))
        self.assertEqual(ledger.entry_edge_actual, D("0.038"))

    def test_partial_close_keeps_remaining_average_then_reentry_reweights_only_remainder(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.010"), D("0.0448"))
        ledger.apply_fill("buy", D("0.004"), D("0.0548"))

        self.assertEqual(ledger.position_qty, D("-0.006"))
        self.assertEqual(ledger.entry_qty, D("0.006"))
        self.assertEqual(ledger.entry_edge_actual, D("0.0448"))

        ledger.apply_fill("sell", D("0.002"), D("0.0400"))

        self.assertEqual(ledger.position_qty, D("-0.008"))
        self.assertEqual(ledger.entry_qty, D("0.008"))
        self.assertEqual(ledger.entry_edge_actual, D("0.0436"))

        ledger.apply_fill("buy", D("0.002"), D("0.0536"))

        self.assertEqual(ledger.entry_edge_actual, D("0.0436"))
        self.assertEqual(ledger.close_edge_actual, D("0.0544"))
        self.assertEqual(ledger.realized_edge_pnl, D("0.0100"))

    def test_adverse_actual_slippage_can_pause_then_recover(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.004"), D("0.042"), normal_close_threshold=D("0.06"))
        ledger.apply_fill("buy", D("0.001"), D("0.052"))
        ledger.apply_fill("buy", D("0.001"), D("0.030"))
        self.assertEqual(ledger.close_edge_actual, D("0.041"))
        self.assertEqual(ledger.decision(D("0.0519")).reason, "minimum_profit_not_reached")
        self.assertEqual(ledger.decision(D("0.052")).action, "close")

    def test_full_close_archives_and_clears_live_edges(self):
        ledger = self.ledger()
        ledger.apply_fill(
            "sell", D("0.002"), D("0.042"),
            normal_close_threshold=D("0.06"), reference_price=D("65000"),
        )
        completed = ledger.apply_fill(
            "buy", D("0.002"), D("0.052"), reference_price=D("65000")
        )
        self.assertEqual(completed[0].edge_pnl, D("0.010"))
        self.assertEqual(completed[0].estimated_quote_pnl, D("0.013000"))
        self.assertIsNone(ledger.entry_edge_actual)
        self.assertIsNone(ledger.close_edge_actual)
        self.assertEqual(len(ledger.completed_rounds), 1)

        restored = RoundExitLedger.from_state(ledger.config, ledger.to_state())
        self.assertEqual(restored.completed_rounds, completed)

    def test_version_two_state_remains_loadable_without_completed_rounds(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.002"), D("0.042"))
        state = ledger.to_state()
        state["version"] = 2
        state.pop("estimated_quote_pnl")
        state.pop("estimated_quote_pnl_qty")
        state.pop("completed_rounds")

        restored = RoundExitLedger.from_state(ledger.config, state)

        self.assertEqual(restored.position_qty, D("-0.002"))
        self.assertEqual(restored.entry_edge_actual, D("0.042"))
        self.assertEqual(restored.completed_rounds, [])

    def test_quote_pnl_estimate_uses_each_close_fill_reference_price(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.002"), D("0.050"))
        ledger.apply_fill("buy", D("0.001"), D("0.060"), reference_price=D("65000"))
        completed = ledger.apply_fill(
            "buy", D("0.001"), D("0.070"), reference_price=D("66000")
        )

        self.assertEqual(completed[0].estimated_quote_pnl, D("0.0197"))

    def test_cross_zero_fill_splits_completed_and_new_round(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.001"), D("0.042"), normal_close_threshold=D("0.06"))
        completed = ledger.apply_fill(
            "buy", D("0.003"), D("0.052"), next_normal_close_threshold=D("0.03")
        )
        self.assertEqual(len(completed), 1)
        self.assertEqual(ledger.side, "long")
        self.assertEqual(ledger.position_qty, D("0.002"))
        self.assertEqual(ledger.entry_edge_actual, D("0.052"))
        self.assertIsNone(ledger.close_edge_actual)

    def test_manual_first_close_works_without_normal_threshold(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.003"), D("0.042"), normal_close_threshold=None)
        ledger.apply_fill("buy", D("0.001"), D("0.052"))
        self.assertTrue(ledger.guard_started)

    def test_live_position_mismatch_halts(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.003"), D("0.042"), normal_close_threshold=D("0.06"))
        ledger.apply_fill("buy", D("0.001"), D("0.052"))
        decision = ledger.decision(D("0.06"), live_position_qty=D("-0.003"))
        self.assertEqual((decision.action, decision.reason), ("halt", "position_mismatch"))

    def test_parameter_matrix_is_long_short_symmetric(self):
        for entry in (D("0.02"), D("0.04"), D("0.08")):
            for advantage in (D("0.009"), D("0.01"), D("0.02")):
                with self.subTest(side="short", entry=entry, advantage=advantage):
                    short = self.ledger()
                    short.apply_fill("sell", D("0.003"), entry, normal_close_threshold=entry + D("0.03"))
                    short.apply_fill("buy", D("0.001"), entry + advantage)
                    self.assertEqual(short.guard_started, advantage >= D("0.01"))
                    self.assertEqual(short.decision(entry + D("0.01")).action, "close")
                with self.subTest(side="long", entry=entry, advantage=advantage):
                    long = self.ledger()
                    long.apply_fill("buy", D("0.003"), entry, normal_close_threshold=entry - D("0.03"))
                    long.apply_fill("sell", D("0.001"), entry - advantage)
                    self.assertEqual(long.guard_started, advantage >= D("0.01"))
                    self.assertEqual(long.decision(entry - D("0.01")).action, "close")

    def test_user_order_replay_allows_gradient_refill_after_profitable_close(self):
        ledger = self.ledger()
        entries = ".0561 .0492 .0497 .0653 .0500 .0426 .0422 .0447 .0443 .0421".split()
        for index, edge in enumerate(entries):
            ledger.apply_fill(
                "sell", D("0.001"), D(edge),
                normal_close_threshold=D("0.06") if index == 0 else None,
            )
        self.assertEqual(ledger.entry_edge_actual, D("0.04862"))
        ledger.apply_fill("buy", D("0.001"), D("0.0616"))
        self.assertTrue(ledger.guard_started)
        ledger.apply_fill("sell", D("0.001"), D("0.0424"))
        self.assertEqual(ledger.position_qty, D("-0.010"))

    def test_config_rejects_non_positive_order_qty(self):
        with self.assertRaises(ValueError):
            RoundExitConfig(D("0"))

    def test_decision_stops_at_gradient_limit(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.015"), D("0.0448"))
        ledger.apply_fill("buy", D("0.001"), D("0.0548"))
        decision = ledger.decision(D("0.0548"), target_position_qty=D("-0.005"))
        self.assertEqual(decision.action, "close")
        self.assertEqual(decision.qty, D("0.001"))

        ledger.position_qty = D("-0.005")
        stopped = ledger.decision(D("0.0548"), target_position_qty=D("-0.005"))
        self.assertEqual(stopped.reason, "position_within_gradient_limit")

    def test_non_flat_round_state_survives_serialization(self):
        ledger = self.ledger()
        ledger.apply_fill("sell", D("0.003"), D("0.042"), normal_close_threshold=D("0.06"))
        ledger.apply_fill("buy", D("0.001"), D("0.052"))

        restored = RoundExitLedger.from_state(ledger.config, ledger.to_state())

        self.assertEqual(restored.position_qty, D("-0.002"))
        self.assertEqual(restored.entry_edge_actual, D("0.042"))
        self.assertEqual(restored.close_edge_actual, D("0.052"))
        self.assertTrue(restored.guard_started)

    def test_corrupt_non_flat_round_state_is_rejected(self):
        ledger = self.ledger()
        state = ledger.to_state()
        state.update({"round_id": 1, "side": "long", "position_qty": "-0.1", "entry_qty": "0.1"})

        with self.assertRaises(ValueError):
            RoundExitLedger.from_state(ledger.config, state)


if __name__ == "__main__":
    unittest.main()
