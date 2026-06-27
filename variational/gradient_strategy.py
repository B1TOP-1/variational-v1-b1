from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum


class StrategySection(str, Enum):
    OPEN = "open"
    CLOSE = "close"


class EditableField(str, Enum):
    THRESHOLD = "threshold"
    QUANTITY = "quantity"


class CursorTarget(str, Enum):
    ENABLED = "enabled"
    ROW = "row"


@dataclass(slots=True)
class GradientRow:
    threshold_pct: Decimal | None = None
    target_qty: Decimal | None = None

    def is_complete(self) -> bool:
        return self.threshold_pct is not None and self.target_qty is not None


@dataclass(frozen=True, slots=True)
class GradientSignal:
    action: str
    section: StrategySection
    spread_pct: Decimal
    threshold_pct: Decimal
    target_qty: Decimal
    current_qty: Decimal
    delta_qty: Decimal

    def signature(self) -> tuple[str, str, str, str, str]:
        return (
            self.action,
            self.section.value,
            format(self.threshold_pct, "f"),
            format(self.target_qty, "f"),
            format(self.delta_qty, "f"),
        )


@dataclass(slots=True)
class GradientStrategyState:
    open_rows: list[GradientRow] = field(default_factory=lambda: [GradientRow()])
    close_rows: list[GradientRow] = field(default_factory=lambda: [GradientRow()])
    cursor_target: CursorTarget = CursorTarget.ENABLED
    cursor_section: StrategySection = StrategySection.OPEN
    cursor_index: int = 0
    cursor_field: EditableField = EditableField.THRESHOLD
    enabled: bool = False
    edit_buffer: str | None = None
    _escape_buffer: str = ""
    _last_close_spread_pct: Decimal | None = None

    @classmethod
    def default(cls) -> GradientStrategyState:
        return cls()

    def rows_for(self, section: StrategySection) -> list[GradientRow]:
        return self.open_rows if section == StrategySection.OPEN else self.close_rows

    def current_row(self) -> GradientRow:
        self.cursor_target = CursorTarget.ROW
        rows = self.rows_for(self.cursor_section)
        self.cursor_index = min(max(self.cursor_index, 0), len(rows) - 1)
        return rows[self.cursor_index]

    def add_row(self, section: StrategySection | None = None) -> None:
        self._commit_edit()
        target_section = section or self.cursor_section
        rows = self.rows_for(target_section)
        insert_at = self.cursor_index + 1 if self.cursor_target == CursorTarget.ROW and target_section == self.cursor_section else len(rows)
        rows.insert(insert_at, GradientRow())
        self.cursor_target = CursorTarget.ROW
        self.cursor_section = target_section
        self.cursor_index = insert_at
        self.cursor_field = EditableField.THRESHOLD

    def delete_current_row(self) -> None:
        self._commit_edit()
        if self.cursor_target == CursorTarget.ENABLED:
            return
        rows = self.rows_for(self.cursor_section)
        if len(rows) == 1:
            rows[0] = GradientRow()
            self.cursor_index = 0
            self.cursor_field = EditableField.THRESHOLD
            return
        del rows[self.cursor_index]
        self.cursor_index = min(self.cursor_index, len(rows) - 1)

    def move_cursor(self, delta: int) -> None:
        self._commit_edit()
        flat = self._flat_cursor_rows()
        if not flat:
            return
        current = self._flat_cursor_position(flat)
        next_pos = min(max(current + delta, 0), len(flat) - 1)
        target, section, index = flat[next_pos]
        self.cursor_target = target
        if section is not None and index is not None:
            self.cursor_section = section
            self.cursor_index = index

    def switch_field(self, delta: int) -> None:
        self._commit_edit()
        if self.cursor_target == CursorTarget.ENABLED:
            return
        if self.cursor_field == EditableField.THRESHOLD:
            self.cursor_field = EditableField.QUANTITY
        else:
            self.cursor_field = EditableField.THRESHOLD

    def handle_key(self, key: str) -> bool:
        if self._handle_escape_sequence(key):
            return True
        if self.cursor_target == CursorTarget.ENABLED and key not in ("\r", "\n"):
            return key in ("+", "-", "\x7f", "\x08", ".") or key.isdigit()
        if key == "+":
            self.add_row()
            return True
        if key == "-":
            self.delete_current_row()
            return True
        if key in ("\r", "\n"):
            if self.cursor_target == CursorTarget.ENABLED:
                self.enabled = not self.enabled
                return True
            self._commit_edit()
            return True
        if key in ("\x7f", "\x08"):
            self._backspace()
            return True
        if key.isdigit() or key == ".":
            self._append_edit_char(key)
            return True
        return False

    def evaluate(
        self,
        open_spread_pct: Decimal | None,
        close_spread_pct: Decimal | None,
        current_position_qty: Decimal,
    ) -> GradientSignal | None:
        if not self.enabled:
            self._last_close_spread_pct = close_spread_pct
            return None
        current_long_qty = max(current_position_qty, Decimal("0"))
        close_signal = self._evaluate_close(close_spread_pct, current_long_qty)
        self._last_close_spread_pct = close_spread_pct
        if close_signal is not None:
            return close_signal
        return self._evaluate_open(open_spread_pct, current_long_qty)

    def display_value(self, section: StrategySection, index: int, field_name: EditableField) -> str:
        if (
            section == self.cursor_section
            and index == self.cursor_index
            and self.cursor_target == CursorTarget.ROW
            and field_name == self.cursor_field
            and self.edit_buffer is not None
        ):
            return self.edit_buffer or "_"
        row = self.rows_for(section)[index]
        value = row.threshold_pct if field_name == EditableField.THRESHOLD else row.target_qty
        return "-" if value is None else format(value, "f")

    def selected(self, section: StrategySection, index: int) -> bool:
        return self.cursor_target == CursorTarget.ROW and section == self.cursor_section and index == self.cursor_index

    def enabled_selected(self) -> bool:
        return self.cursor_target == CursorTarget.ENABLED

    def _evaluate_open(self, spread_pct: Decimal | None, current_qty: Decimal) -> GradientSignal | None:
        if spread_pct is None:
            return None
        rows = sorted((row for row in self.open_rows if row.is_complete()), key=lambda row: row.threshold_pct)
        selected: GradientRow | None = None
        for row in rows:
            if row.threshold_pct is not None and spread_pct >= row.threshold_pct:
                selected = row
        if selected is None or selected.target_qty is None or selected.threshold_pct is None:
            return None
        delta_qty = selected.target_qty - current_qty
        if delta_qty <= 0:
            return None
        return GradientSignal(
            action="open",
            section=StrategySection.OPEN,
            spread_pct=spread_pct,
            threshold_pct=selected.threshold_pct,
            target_qty=selected.target_qty,
            current_qty=current_qty,
            delta_qty=delta_qty,
        )

    def _evaluate_close(self, spread_pct: Decimal | None, current_qty: Decimal) -> GradientSignal | None:
        if spread_pct is None or current_qty <= 0 or self._last_close_spread_pct is None:
            return None
        previous_spread = self._last_close_spread_pct
        rows = sorted(
            (row for row in self.close_rows if row.is_complete()),
            key=lambda row: row.threshold_pct,
            reverse=True,
        )
        selected: GradientRow | None = None
        for row in rows:
            if row.threshold_pct is not None and previous_spread > row.threshold_pct and spread_pct <= row.threshold_pct:
                selected = row
        if selected is None or selected.target_qty is None or selected.threshold_pct is None:
            return None
        delta_qty = current_qty - selected.target_qty
        if delta_qty <= 0:
            return None
        return GradientSignal(
            action="close",
            section=StrategySection.CLOSE,
            spread_pct=spread_pct,
            threshold_pct=selected.threshold_pct,
            target_qty=selected.target_qty,
            current_qty=current_qty,
            delta_qty=delta_qty,
        )

    def _handle_escape_sequence(self, key: str) -> bool:
        if key in ("\x1b[A", "\x1b[B", "\x1b[C", "\x1b[D"):
            self._apply_arrow(key[-1])
            return True
        if key == "\x1b":
            if self.edit_buffer is not None:
                self.edit_buffer = None
                return True
            self._escape_buffer = key
            return True
        if self._escape_buffer == "\x1b" and key == "[":
            self._escape_buffer = "\x1b["
            return True
        if self._escape_buffer == "\x1b[" and key in ("A", "B", "C", "D"):
            self._escape_buffer = ""
            self._apply_arrow(key)
            return True
        if self._escape_buffer:
            self._escape_buffer = ""
        return False

    def _apply_arrow(self, code: str) -> None:
        if code == "A":
            self.move_cursor(-1)
        elif code == "B":
            self.move_cursor(1)
        elif code == "C":
            self.switch_field(1)
        elif code == "D":
            self.switch_field(-1)

    def _append_edit_char(self, key: str) -> None:
        if self.edit_buffer is None:
            self.edit_buffer = ""
        if key == "." and "." in self.edit_buffer:
            return
        self.edit_buffer += key

    def _backspace(self) -> None:
        if self.edit_buffer is None:
            current = self._current_field_value()
            self.edit_buffer = "" if current is None else format(current, "f")
        self.edit_buffer = self.edit_buffer[:-1]

    def _commit_edit(self) -> None:
        if self.edit_buffer is None:
            return
        row = self.current_row()
        value = self._parse_decimal(self.edit_buffer)
        if self.cursor_field == EditableField.THRESHOLD:
            row.threshold_pct = value
        else:
            row.target_qty = value
        self.edit_buffer = None

    def _current_field_value(self) -> Decimal | None:
        if self.cursor_target == CursorTarget.ENABLED:
            return None
        row = self.current_row()
        return row.threshold_pct if self.cursor_field == EditableField.THRESHOLD else row.target_qty

    @staticmethod
    def _parse_decimal(raw: str) -> Decimal | None:
        text = raw.strip()
        if not text:
            return None
        try:
            value = Decimal(text)
        except InvalidOperation:
            return None
        if value < 0:
            return None
        return value

    def _flat_cursor_rows(self) -> list[tuple[CursorTarget, StrategySection | None, int | None]]:
        rows: list[tuple[CursorTarget, StrategySection | None, int | None]] = [(CursorTarget.ENABLED, None, None)]
        rows.extend((CursorTarget.ROW, StrategySection.OPEN, index) for index in range(len(self.open_rows)))
        rows.extend((CursorTarget.ROW, StrategySection.CLOSE, index) for index in range(len(self.close_rows)))
        return rows

    def _flat_cursor_position(self, flat: list[tuple[CursorTarget, StrategySection | None, int | None]]) -> int:
        if self.cursor_target == CursorTarget.ENABLED:
            return 0
        current = (self.cursor_section, self.cursor_index)
        for index, (target, section, row_index) in enumerate(flat):
            if target == CursorTarget.ROW and (section, row_index) == current:
                return index
        return 0
