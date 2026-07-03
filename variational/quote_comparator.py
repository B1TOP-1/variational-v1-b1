"""Var 报价双源对比器：对比 API(indicative) 与 DOM 两路报价流。

交易默认仍用 API；本模块仅做观测/记录：
- 分开记录"变化时间(change_ts)"与"获取时间(acquire_ts)"——价格可能同时变，但两路
  收到的时间不同（延迟在链路而非源头）。
- 领先/滞后：同一价格两路都出现时，判哪路先、差多少（源头维度 + 获取维度各一份）。
- 背离(不正确)：一路出现另一路"从没出现过"的价，且对方已越过该时刻 → 记为背离。

纯逻辑、无外部依赖，可喂合成序列本地单测。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(slots=True)
class _Transition:
    bid: float
    ask: float
    change_ms: float
    acquire_ms: float


@dataclass(slots=True)
class MatchResult:
    """两路都出现了同一个价时的对比结果。"""
    bid: float
    ask: float
    change_leader: str      # 源头先变的一路
    change_lead_ms: float   # 源头领先量(>=0)
    acquire_leader: str     # 我方先收到的一路
    acquire_lead_ms: float  # 获取领先量(>=0)


@dataclass(slots=True)
class Divergence:
    """一路出现了另一路从未确认的价（不正确）。"""
    source: str
    bid: float
    ask: float
    change_ms: float


class _LeadTally:
    """按方向累计领先次数/量。"""

    __slots__ = ("counts", "total_ms", "n")

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.total_ms = 0.0
        self.n = 0

    def add(self, leader: str, lead_ms: float) -> None:
        self.counts[leader] = self.counts.get(leader, 0) + 1
        self.total_ms += lead_ms
        self.n += 1

    def avg_ms(self) -> float | None:
        return self.total_ms / self.n if self.n else None


class QuoteComparator:
    def __init__(
        self,
        sources: tuple[str, str] = ("api", "dom"),
        *,
        match_tolerance: float = 0.0,
        match_window_ms: float = 200.0,
        divergence_window_ms: float = 500.0,
        history: int = 200,
    ) -> None:
        if len(sources) != 2:
            raise ValueError("QuoteComparator 需要恰好两个源")
        self.sources = tuple(sources)
        self.match_tolerance = float(match_tolerance)
        self.match_window_ms = float(match_window_ms)
        self.divergence_window_ms = float(divergence_window_ms)
        self._history: dict[str, deque[_Transition]] = {s: deque(maxlen=history) for s in self.sources}
        self._pending: dict[str, list[_Transition]] = {s: [] for s in self.sources}
        self._last: dict[str, tuple[float, float] | None] = {s: None for s in self.sources}
        self._last_acquire_ms: dict[str, float | None] = {s: None for s in self.sources}
        self.transitions: dict[str, int] = {s: 0 for s in self.sources}
        self.divergences: dict[str, int] = {s: 0 for s in self.sources}
        self.change_lead = _LeadTally()   # 源头领先
        self.acquire_lead = _LeadTally()  # 获取领先
        self.recent_divergences: deque[Divergence] = deque(maxlen=50)

    def _other(self, source: str) -> str:
        return self.sources[1] if source == self.sources[0] else self.sources[0]

    def _prices_match(self, a: tuple[float, float], b: tuple[float, float]) -> bool:
        return abs(a[0] - b[0]) <= self.match_tolerance and abs(a[1] - b[1]) <= self.match_tolerance

    def update(self, source: str, bid: float, ask: float, change_ms: float, acquire_ms: float) -> MatchResult | None:
        """吃一条报价。价格相对该源上一条没变则不算 transition。匹配到对方待定项时返回对比结果。"""
        if source not in self.sources:
            raise ValueError(f"未知报价源: {source}")
        bid = float(bid)
        ask = float(ask)
        change_ms = float(change_ms)
        acquire_ms = float(acquire_ms)
        self._last_acquire_ms[source] = acquire_ms
        last = self._last[source]
        if last is not None and self._prices_match(last, (bid, ask)):
            return None  # 价格没变
        self._last[source] = (bid, ask)
        self.transitions[source] += 1
        current = _Transition(bid, ask, change_ms, acquire_ms)
        self._history[source].append(current)

        other = self._other(source)
        matched = self._pop_pending(other, bid, ask, acquire_ms)
        if matched is not None:
            # 对方之前出过这个价、在等确认 → 对方领先。
            return self._record_match(leader_t=matched, leader=other, follow_t=current)
        # 对方还没出过 → 加入本源待定，等对方确认或超窗判背离。
        self._pending[source].append(current)
        return None

    def _pop_pending(self, source: str, bid: float, ask: float, now_ms: float) -> _Transition | None:
        lst = self._pending[source]
        for index, item in enumerate(lst):
            # 只与近窗内的待定项配对，避免价格重复出现时跨大时间差错配（虚高领先）。
            if now_ms - item.acquire_ms > self.divergence_window_ms:
                continue
            if self._prices_match((item.bid, item.ask), (bid, ask)):
                return lst.pop(index)
        return None

    def _record_match(self, *, leader_t: _Transition, leader: str, follow_t: _Transition) -> MatchResult:
        follower = self._other(leader)
        change_diff = follow_t.change_ms - leader_t.change_ms
        if change_diff >= 0:
            change_leader, change_lead = leader, change_diff
        else:
            change_leader, change_lead = follower, -change_diff
        self.change_lead.add(change_leader, change_lead)

        acquire_diff = follow_t.acquire_ms - leader_t.acquire_ms
        if acquire_diff >= 0:
            acquire_leader, acquire_lead = leader, acquire_diff
        else:
            acquire_leader, acquire_lead = follower, -acquire_diff
        self.acquire_lead.add(acquire_leader, acquire_lead)

        return MatchResult(
            bid=follow_t.bid,
            ask=follow_t.ask,
            change_leader=change_leader,
            change_lead_ms=change_lead,
            acquire_leader=acquire_leader,
            acquire_lead_ms=acquire_lead,
        )

    def tick(self, now_ms: float) -> list[Divergence]:
        """周期检查：待定项超背离窗且对方已越过该时刻(仍未出该价) → 判背离。返回新发现的背离。"""
        found: list[Divergence] = []
        for source in self.sources:
            other = self._other(source)
            keep: list[_Transition] = []
            for pending in self._pending[source]:
                if now_ms - pending.change_ms <= self.divergence_window_ms:
                    keep.append(pending)
                    continue
                other_advanced = any(h.change_ms > pending.change_ms for h in self._history[other])
                if other_advanced:
                    self.divergences[source] += 1
                    divergence = Divergence(source, pending.bid, pending.ask, pending.change_ms)
                    self.recent_divergences.append(divergence)
                    found.append(divergence)
                else:
                    keep.append(pending)  # 对方还没越过(可能卡住/滞后)，继续等
            self._pending[source] = keep
        return found

    def freshness_ms(self, source: str, now_ms: float) -> float | None:
        """该源距上次收到报价多久(ms)，用于过期判断。"""
        last = self._last_acquire_ms.get(source)
        return None if last is None else max(0.0, now_ms - last)

    def snapshot(self) -> dict:
        return {
            "transitions": dict(self.transitions),
            "matched": self.change_lead.n,
            "change_lead_avg_ms": self.change_lead.avg_ms(),
            "change_lead_counts": dict(self.change_lead.counts),
            "acquire_lead_avg_ms": self.acquire_lead.avg_ms(),
            "acquire_lead_counts": dict(self.acquire_lead.counts),
            "divergences": dict(self.divergences),
        }
