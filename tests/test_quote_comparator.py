import unittest

from variational.quote_comparator import Divergence, QuoteComparator


class QuoteComparatorTest(unittest.TestCase):
    def _cmp(self, **kw):
        return QuoteComparator(("api", "dom"), **kw)

    def test_no_transition_when_price_unchanged(self):
        c = self._cmp()
        self.assertIsNone(c.update("api", 100, 101, 0, 0))
        self.assertIsNone(c.update("api", 100, 101, 5, 5))  # 同价，不算 transition
        self.assertEqual(c.transitions["api"], 1)

    def test_api_leads_dom_by_change_time(self):
        c = self._cmp()
        self.assertIsNone(c.update("api", 100, 101, 0, 0))          # api 先出，待定
        result = c.update("dom", 100, 101, 10, 10)                  # dom 后出，匹配
        self.assertIsNotNone(result)
        self.assertEqual(result.change_leader, "api")
        self.assertEqual(result.change_lead_ms, 10)
        self.assertEqual(result.acquire_leader, "api")
        self.assertEqual(result.acquire_lead_ms, 10)

    def test_dom_leads_api(self):
        c = self._cmp()
        self.assertIsNone(c.update("dom", 100, 101, 0, 0))
        result = c.update("api", 100, 101, 8, 8)
        self.assertEqual(result.change_leader, "dom")
        self.assertEqual(result.change_lead_ms, 8)

    def test_change_simultaneous_but_acquire_differs(self):
        # 价格同时变(change 相同)，但获取时间不同 → 源头持平、获取维度 api 先。
        c = self._cmp()
        c.update("api", 100, 101, 100, 100)
        result = c.update("dom", 100, 101, 100, 112)
        self.assertEqual(result.change_lead_ms, 0)
        self.assertEqual(result.acquire_leader, "api")
        self.assertEqual(result.acquire_lead_ms, 12)

    def test_divergence_when_other_never_shows_price(self):
        # api: 100 -> 101 -> 102(推进)；dom 冒出 99(api 从没有过) → 背离(dom)。
        c = self._cmp()
        c.update("api", 100, 100, 0, 0)
        c.update("dom", 100, 100, 10, 10)   # 匹配 100
        c.update("api", 101, 101, 20, 20)
        c.update("dom", 99, 99, 25, 25)     # 99 待定
        c.update("api", 102, 102, 40, 40)   # api 越过 25 且非 99
        c.update("dom", 101, 101, 30, 30)   # 匹配 101

        found = c.tick(now_ms=600)
        self.assertTrue(any(d.source == "dom" and d.bid == 99 for d in found))
        self.assertEqual(c.divergences["dom"], 1)

    def test_no_divergence_while_within_window(self):
        c = self._cmp(divergence_window_ms=500)
        c.update("api", 100, 100, 0, 0)     # api 待定 100
        # dom 还没确认，但未超窗
        self.assertEqual(c.tick(now_ms=100), [])
        # dom 在窗内确认 → 无背离，正常匹配
        result = c.update("dom", 100, 100, 120, 120)
        self.assertIsNotNone(result)
        self.assertEqual(c.divergences["api"], 0)

    def test_snapshot_aggregates(self):
        c = self._cmp()
        c.update("api", 100, 101, 0, 0)
        c.update("dom", 100, 101, 10, 10)   # api leads 10
        c.update("dom", 102, 103, 20, 20)
        c.update("api", 102, 103, 26, 26)   # dom leads 6
        snap = c.snapshot()
        self.assertEqual(snap["matched"], 2)
        self.assertEqual(snap["change_lead_counts"], {"api": 1, "dom": 1})
        self.assertEqual(snap["change_lead_avg_ms"], 8.0)  # (10+6)/2

    def test_freshness(self):
        c = self._cmp()
        self.assertIsNone(c.freshness_ms("api", 100))
        c.update("api", 100, 101, 0, 50)
        self.assertEqual(c.freshness_ms("api", 250), 200)


if __name__ == "__main__":
    unittest.main()
