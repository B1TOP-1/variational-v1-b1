from __future__ import annotations

import io
from typing import Any

from rich.console import Console, RenderableType


class DifferentialScreen:
    """Render Rich content to a buffer and write only changed terminal rows."""

    def __init__(self, console: Console) -> None:
        self.console = console
        self._rows: list[str] = []
        self._size: tuple[int, int] | None = None
        self._active = False

    def __enter__(self) -> DifferentialScreen:
        self._active = True
        if self.console.is_terminal:
            self._write("\x1b[?1049h\x1b[?25l\x1b[?7l\x1b[2J\x1b[H")
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.console.is_terminal:
            self._write("\x1b[?7h\x1b[?25h\x1b[?1049l")
        self._active = False
        self._rows = []

    def update(self, renderable: RenderableType) -> int:
        if not self.console.is_terminal:
            self.console.print(renderable)
            return 0

        size = self.console.size
        current_size = (size.width, size.height)
        resized = current_size != self._size
        next_rows = self._render_rows(renderable, size.width, size.height)
        changed = self.changed_row_indexes([] if resized else self._rows, next_rows, size.height)

        output: list[str] = []
        if resized:
            output.append("\x1b[2J")
        for index in changed:
            content = next_rows[index] if index < len(next_rows) else ""
            output.append(f"\x1b[{index + 1};1H\x1b[2K{content}")
        if output:
            self._write("".join(output))

        self._rows = next_rows
        self._size = current_size
        return len(changed)

    @staticmethod
    def changed_row_indexes(previous: list[str], current: list[str], height: int) -> list[int]:
        limit = min(height, max(len(previous), len(current)))
        return [
            index
            for index in range(limit)
            if (previous[index] if index < len(previous) else "")
            != (current[index] if index < len(current) else "")
        ]

    def _render_rows(self, renderable: RenderableType, width: int, height: int) -> list[str]:
        buffer = io.StringIO()
        offscreen = Console(
            file=buffer,
            force_terminal=True,
            color_system=self.console.color_system,
            width=width,
            height=height,
            legacy_windows=False,
        )
        offscreen.print(renderable, crop=True, overflow="crop")
        return buffer.getvalue().splitlines()[:height]

    def _write(self, value: str) -> None:
        self.console.file.write(value)
        self.console.file.flush()
