"""
Opt-in bridge that routes TaskLattice warnings to rich code-frame rendering.
This preserves Python's warnings semantics and filtering.

Do NOT install this at import time. Let scripts/CLIs opt in.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from dataclasses import dataclass
from typing import TextIO

from rich.console import Console

from tasklattice.reporting.diagnostics import Diagnostic, Emitter

__all__ = [
    "TLWarning",
    "TemplateWarning",
    "PlaceholderWarning",
    "DiagnosticWarning",
    "install_warnings_bridge",
]


# ─────────── Warning categories (parity with exceptions) ───────────


class TLWarning(Warning):
    """Base TaskLattice warning category."""


class TemplateWarning(TLWarning):
    """Warnings related to template usage/structure."""


class PlaceholderWarning(TLWarning):
    """Warnings related to placeholder parsing/metadata."""


@dataclass(slots=True)
class DiagnosticWarning(TLWarning):
    """
    A warning carrying a Diagnostic. Works fine without the bridge (plain text via __str__),
    and pretty-prints when the bridge is installed.
    """

    diagnostic: Diagnostic

    def __str__(self) -> str:  # pragma: no cover - trivial formatting
        d = self.diagnostic
        (ln, col) = d.source.pos_to_line_col(d.span.start)
        code = f" [{d.code}]" if d.code else ""
        p = getattr(d.source, "file", None) or getattr(d.source, "path", None)
        label = str(p) if p is not None else "<string>"
        return f"{d.severity.upper()}{code}: {d.message} at {label}:{ln}:{col}"


# ─────────── Opt-in bridge (TaskLattice-only by default) ───────────


def install_warnings_bridge(
    *,
    emitter: Emitter | None = None,
    only_tasklattice: bool = True,
) -> Callable[[], None]:
    """
    Route Python's warnings display for TaskLattice warnings through Rich frames.

    - Returns an `uninstall()` function to restore the previous handler.
    - If `only_tasklattice=True` (default), non-TaskLattice warnings are passed through unchanged.
    """
    # Default to stderr per warnings convention
    em = emitter or Emitter(Console(stderr=True))

    prev_showwarning = warnings.showwarning

    def _showwarning(
        message: Warning | str,
        category: type[Warning],
        filename: str,
        lineno: int,
        file: TextIO | None = None,
        line: str | None = None,
    ) -> None:
        if isinstance(message, DiagnosticWarning):
            em.emit(message.diagnostic)
            return
        if only_tasklattice:
            return prev_showwarning(message, category, filename, lineno, file=file, line=line)
        em.console.print(f"{category.__name__}: {message} ({filename}:{lineno})")

    warnings.showwarning = _showwarning

    def uninstall() -> None:
        warnings.showwarning = prev_showwarning

    return uninstall
