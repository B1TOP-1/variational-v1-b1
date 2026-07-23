from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


class RoundStateError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RoundExitConfig:
    single_order_qty: Decimal
    minimum_profit_pct: Decimal = Decimal("0.01")

    def __post_init__(self) -> None:
        if self.single_order_qty <= 0:
            raise ValueError("single_order_qty must be positive")
        if self.minimum_profit_pct < 0:
            raise ValueError("minimum_profit_pct must not be negative")


@dataclass(frozen=True, slots=True)
class RoundDecision:
    action: str
    reason: str
    order_side: str | None = None
    qty: Decimal = Decimal("0")
    projected_close_edge: Decimal | None = None


@dataclass(frozen=True, slots=True)
class CompletedRound:
    round_id: int
    side: str
    entry_qty: Decimal
    close_qty: Decimal
    entry_edge_actual: Decimal
    close_edge_actual: Decimal
    edge_pnl: Decimal
    estimated_quote_pnl: Decimal | None = None


class RoundExitLedger:
    """Actual-fill round ledger and cost-line exit decision reference model."""

    def __init__(self, config: RoundExitConfig) -> None:
        self.config = config
        self._next_round_id = 1
        self.completed_rounds: list[CompletedRound] = []
        self._clear_live_round()

    def _clear_live_round(self) -> None:
        self.round_id: int | None = None
        self.side: str | None = None
        self.position_qty = Decimal("0")
        self.normal_close_threshold: Decimal | None = None
        self.entry_qty = Decimal("0")
        self._entry_edge_weighted = Decimal("0")
        self.close_qty = Decimal("0")
        self._close_edge_weighted = Decimal("0")
        self._closed_entry_edge_weighted = Decimal("0")
        self._estimated_quote_pnl = Decimal("0")
        self._estimated_quote_pnl_qty = Decimal("0")
        self.guard_started = False

    @property
    def entry_edge_actual(self) -> Decimal | None:
        return None if self.entry_qty <= 0 else self._entry_edge_weighted / self.entry_qty

    @property
    def close_edge_actual(self) -> Decimal | None:
        return None if self.close_qty <= 0 else self._close_edge_weighted / self.close_qty

    @property
    def realized_edge_pnl(self) -> Decimal | None:
        if self.close_qty <= 0 or self.side is None:
            return None
        close = self._close_edge_weighted / self.close_qty
        entry_basis = self._closed_entry_edge_weighted / self.close_qty
        return close - entry_basis if self.side == "short" else entry_basis - close

    def _actual_close_profitable(self) -> bool:
        entry = self.entry_edge_actual
        close = self.close_edge_actual
        if entry is None or close is None:
            return False
        margin = self.config.minimum_profit_pct
        return close >= entry + margin if self.side == "short" else close <= entry - margin

    def _start_round(
        self,
        order_side: str,
        qty: Decimal,
        edge_pct: Decimal,
        normal_close_threshold: Decimal | None,
    ) -> None:
        self.round_id = self._next_round_id
        self._next_round_id += 1
        self.side = "long" if order_side == "buy" else "short"
        self.position_qty = qty if self.side == "long" else -qty
        self.normal_close_threshold = normal_close_threshold
        self.entry_qty = qty
        self._entry_edge_weighted = qty * edge_pct
        self.close_qty = Decimal("0")
        self._close_edge_weighted = Decimal("0")
        self.guard_started = False

    def _same_direction(self, order_side: str) -> bool:
        return (self.side == "long" and order_side == "buy") or (
            self.side == "short" and order_side == "sell"
        )

    def _complete_round(self) -> CompletedRound:
        entry = (
            self._closed_entry_edge_weighted / self.close_qty
            if self.close_qty > 0
            else None
        )
        close = self.close_edge_actual
        if self.round_id is None or self.side is None or entry is None or close is None:
            raise RoundStateError("cannot complete an incomplete round")
        pnl = self.realized_edge_pnl
        if pnl is None:
            raise RoundStateError("realized edge pnl is unavailable")
        completed = CompletedRound(
            self.round_id,
            self.side,
            self.close_qty,
            self.close_qty,
            entry,
            close,
            pnl,
            self._estimated_quote_pnl
            if self._estimated_quote_pnl_qty == self.close_qty
            else None,
        )
        self.completed_rounds.append(completed)
        return completed

    def apply_fill(
        self,
        order_side: str,
        qty: Decimal,
        edge_pct: Decimal,
        *,
        normal_close_threshold: Decimal | None = None,
        next_normal_close_threshold: Decimal | None = None,
        reference_price: Decimal | None = None,
    ) -> list[CompletedRound]:
        side = order_side.strip().lower()
        if side not in {"buy", "sell"}:
            raise ValueError("order_side must be buy or sell")
        if qty <= 0:
            raise ValueError("fill qty must be positive")

        if self.position_qty == 0:
            self._start_round(side, qty, edge_pct, normal_close_threshold)
            return []

        if self._same_direction(side):
            self.entry_qty += qty
            self._entry_edge_weighted += qty * edge_pct
            self.position_qty += qty if self.side == "long" else -qty
            return []

        close_part = min(qty, abs(self.position_qty))
        entry_before_close = self.entry_edge_actual
        if entry_before_close is None:
            raise RoundStateError("remaining entry cost is unavailable")
        self.close_qty += close_part
        self._close_edge_weighted += close_part * edge_pct
        self._closed_entry_edge_weighted += close_part * entry_before_close
        if reference_price is not None and reference_price > 0:
            edge_pnl = (
                edge_pct - entry_before_close
                if self.side == "short"
                else entry_before_close - edge_pct
            )
            self._estimated_quote_pnl += close_part * reference_price * edge_pnl / Decimal("100")
            self._estimated_quote_pnl_qty += close_part
        self.entry_qty -= close_part
        self._entry_edge_weighted -= close_part * entry_before_close
        self.position_qty += close_part if self.side == "short" else -close_part
        if self._actual_close_profitable():
            self.guard_started = True

        remainder = qty - close_part
        if self.position_qty != 0:
            return []

        completed = self._complete_round()
        self._clear_live_round()
        if remainder > 0:
            self._start_round(side, remainder, edge_pct, next_normal_close_threshold)
        return [completed]

    def projected_close_edge(self, next_qty: Decimal, next_edge: Decimal) -> Decimal | None:
        if self.close_qty <= 0 or next_qty <= 0:
            return None
        return (self._close_edge_weighted + next_qty * next_edge) / (self.close_qty + next_qty)

    def to_state(self) -> dict[str, object]:
        return {
            "version": 3,
            "next_round_id": self._next_round_id,
            "round_id": self.round_id,
            "side": self.side,
            "position_qty": str(self.position_qty),
            "normal_close_threshold": (
                str(self.normal_close_threshold) if self.normal_close_threshold is not None else None
            ),
            "entry_qty": str(self.entry_qty),
            "entry_edge_weighted": str(self._entry_edge_weighted),
            "close_qty": str(self.close_qty),
            "close_edge_weighted": str(self._close_edge_weighted),
            "closed_entry_edge_weighted": str(self._closed_entry_edge_weighted),
            "estimated_quote_pnl": str(self._estimated_quote_pnl),
            "estimated_quote_pnl_qty": str(self._estimated_quote_pnl_qty),
            "guard_started": self.guard_started,
            "completed_rounds": [
                {
                    "round_id": item.round_id,
                    "side": item.side,
                    "entry_qty": str(item.entry_qty),
                    "close_qty": str(item.close_qty),
                    "entry_edge_actual": str(item.entry_edge_actual),
                    "close_edge_actual": str(item.close_edge_actual),
                    "edge_pnl": str(item.edge_pnl),
                    "estimated_quote_pnl": (
                        str(item.estimated_quote_pnl)
                        if item.estimated_quote_pnl is not None
                        else None
                    ),
                }
                for item in self.completed_rounds[-50:]
            ],
        }

    @classmethod
    def from_state(cls, config: RoundExitConfig, state: dict) -> RoundExitLedger:
        version = int(state.get("version", 0))
        if version not in {2, 3}:
            raise ValueError("unsupported round state version")
        ledger = cls(config)
        ledger._next_round_id = int(state.get("next_round_id", 1))
        ledger.round_id = int(state["round_id"]) if state.get("round_id") is not None else None
        ledger.side = state.get("side")
        if ledger.side not in {None, "long", "short"}:
            raise ValueError("invalid round side")
        ledger.position_qty = Decimal(str(state.get("position_qty", "0")))
        threshold = state.get("normal_close_threshold")
        ledger.normal_close_threshold = Decimal(str(threshold)) if threshold is not None else None
        ledger.entry_qty = Decimal(str(state.get("entry_qty", "0")))
        ledger._entry_edge_weighted = Decimal(str(state.get("entry_edge_weighted", "0")))
        ledger.close_qty = Decimal(str(state.get("close_qty", "0")))
        ledger._close_edge_weighted = Decimal(str(state.get("close_edge_weighted", "0")))
        ledger._closed_entry_edge_weighted = Decimal(
            str(state.get("closed_entry_edge_weighted", "0"))
        )
        ledger._estimated_quote_pnl = Decimal(str(state.get("estimated_quote_pnl", "0")))
        ledger._estimated_quote_pnl_qty = Decimal(str(state.get("estimated_quote_pnl_qty", "0")))
        ledger.guard_started = bool(state.get("guard_started", False))
        if version >= 3:
            ledger.completed_rounds = [
                CompletedRound(
                    round_id=int(item["round_id"]),
                    side=str(item["side"]),
                    entry_qty=Decimal(str(item["entry_qty"])),
                    close_qty=Decimal(str(item["close_qty"])),
                    entry_edge_actual=Decimal(str(item["entry_edge_actual"])),
                    close_edge_actual=Decimal(str(item["close_edge_actual"])),
                    edge_pnl=Decimal(str(item["edge_pnl"])),
                    estimated_quote_pnl=(
                        Decimal(str(item["estimated_quote_pnl"]))
                        if item.get("estimated_quote_pnl") is not None
                        else None
                    ),
                )
                for item in state.get("completed_rounds", [])
            ]
        if ledger.position_qty == 0:
            ledger._clear_live_round()
        elif ledger.round_id is None or ledger.side is None or ledger.entry_qty <= 0:
            raise ValueError("incomplete non-flat round state")
        elif (ledger.position_qty > 0) != (ledger.side == "long"):
            raise ValueError("round side and position sign disagree")
        return ledger

    def decision(
        self,
        current_executable_close_edge: Decimal,
        *,
        live_position_qty: Decimal | None = None,
        order_qty: Decimal | None = None,
        target_position_qty: Decimal = Decimal("0"),
    ) -> RoundDecision:
        if self.position_qty == 0:
            return RoundDecision("wait", "flat")
        if live_position_qty is not None and live_position_qty != self.position_qty:
            return RoundDecision("halt", "position_mismatch")
        entry = self.entry_edge_actual
        if entry is None:
            return RoundDecision("wait", "entry_cost_unavailable")

        if self.side == "short":
            reducible = target_position_qty - self.position_qty
            valid_target = self.position_qty < target_position_qty <= 0
        else:
            reducible = self.position_qty - target_position_qty
            valid_target = 0 <= target_position_qty < self.position_qty
        if not valid_target or reducible <= 0:
            return RoundDecision("wait", "position_within_gradient_limit")

        margin = self.config.minimum_profit_pct
        profitable = (
            current_executable_close_edge >= entry + margin
            if self.side == "short"
            else current_executable_close_edge <= entry - margin
        )
        if not profitable:
            return RoundDecision(
                "wait",
                "minimum_profit_not_reached",
                projected_close_edge=current_executable_close_edge,
            )

        qty = min(order_qty or self.config.single_order_qty, reducible)
        order_side = "buy" if self.side == "short" else "sell"
        return RoundDecision(
            "close",
            "one_basis_point_exit",
            order_side,
            qty,
            current_executable_close_edge,
        )
