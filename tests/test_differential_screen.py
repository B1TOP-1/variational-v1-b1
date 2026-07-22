import io
import unittest

from rich.console import Console
from rich.text import Text

from variational.differential_screen import DifferentialScreen


class DifferentialScreenTest(unittest.TestCase):
    def test_only_changed_rows_are_written_after_first_frame(self):
        output = io.StringIO()
        console = Console(file=output, force_terminal=True, width=40, height=10)

        with DifferentialScreen(console) as screen:
            first_changed = screen.update(Text("first\nstatic"))
            unchanged = screen.update(Text("first\nstatic"))
            one_changed = screen.update(Text("second\nstatic"))

        self.assertEqual(first_changed, 2)
        self.assertEqual(unchanged, 0)
        self.assertEqual(one_changed, 1)

    def test_removed_rows_are_cleared(self):
        changed = DifferentialScreen.changed_row_indexes(["one", "two"], ["one"], 10)
        self.assertEqual(changed, [1])
