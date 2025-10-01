"""
TaskLattice exceptions: a base TLException that wraps a Diagnostic and renders
using the same rich code-frame formatting.
"""

from __future__ import annotations

from dataclasses import dataclass

from rich.console import Console, ConsoleOptions, RenderResult

from tasklattice.reporting.diagnostics import Diagnostic, render_diagnostic

__all__ = ["TLException"]


@dataclass(slots=True)
class TLException(Exception):
    """
    Base TaskLattice exception that carries a Diagnostic and renders nicely with Rich.
    """

    diagnostic: Diagnostic

    # Plain-text fallback (CI/log files; or if user didn't use Console)
    def __str__(self) -> str:  # pragma: no cover - trivial formatting
        d = self.diagnostic
        (ln, col) = d.source.pos_to_line_col(d.span.start)
        code = f" [{d.code}]" if d.code else ""
        p = getattr(d.source, "file", None) or getattr(d.source, "path", None)
        label = str(p) if p is not None else "<string>"
        return f"{d.severity.upper()}{code}: {d.message} at {label}:{ln}:{col}"

    # Pretty rendering when printed via Rich Console
    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        _ = console
        _ = options
        yield render_diagnostic(self.diagnostic)
