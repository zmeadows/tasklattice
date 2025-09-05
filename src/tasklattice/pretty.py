"""
Script-friendly helpers for enabling diagnostics:
- `use_diagnostics(...)`: context manager that installs the warnings bridge (opt-in)
  and configures Rich color behavior with sensible 'auto' defaults.
- `run_with_diagnostics(...)`: decorator to wrap a function in the same context
  and pretty-print TLException on the way out.
"""

from __future__ import annotations

import contextvars
import functools
import os
import sys
from contextlib import contextmanager
from typing import Iterator

from rich.console import Console

from tasklattice.errors import TLException
from tasklattice.diagnostics import Emitter
from tasklattice.warnings_bridge import install_warnings_bridge


__all__ = ["use_diagnostics", "print_exception", "run_with_diagnostics"]


# Track the active Console so print_exception() can reuse the same settings.
_active_console: contextvars.ContextVar[Console | None] = contextvars.ContextVar(
    "_active_console", default=None
)


@contextmanager
def use_diagnostics(
    *,
    color: str | None = None,
    pretty: bool | str | None = None,
    only_tasklattice: bool = True,
) -> Iterator[None]:
    """
    Enable Rich diagnostics for *this script*.

    Args:
      color: 'auto' | 'always' | 'never' | None (env TASKLATTICE_COLOR or 'auto')
      pretty: True | False | 'auto' | None (env TASKLATTICE_PRETTY_WARNINGS or 'auto')
      only_tasklattice: if True, only TaskLattice warnings get prettified.

    Behavior:
      - pretty='auto' → install bridge only if a TTY is attached.
      - pretty=True   → always install bridge (Rich still disables color if not a TTY unless forced).
      - pretty=False  → never install bridge.
    """
    color = (color or os.getenv("TASKLATTICE_COLOR") or "auto").lower()
    pretty_val = pretty if pretty is not None else os.getenv("TASKLATTICE_PRETTY_WARNINGS", "auto")
    pretty_str = str(pretty_val).lower()
    is_tty = sys.stderr.isatty() or sys.stdout.isatty()
    enable_pretty = (pretty_str in {"true", "1"}) or (pretty_str == "auto" and is_tty)

    console = Console(
        stderr=True,
        force_terminal=(color == "always"),
        no_color=(color == "never"),
    )
    token = _active_console.set(console)

    uninstall = None
    try:
        if enable_pretty:
            # Use an emitter bound to this Console to ensure consistent stream/colors.
            emitter = Emitter(console=console)
            uninstall = install_warnings_bridge(
                emitter=emitter,
                only_tasklattice=only_tasklattice,
            )
        yield
    finally:
        if uninstall:
            uninstall()
        _active_console.reset(token)


def print_exception(e: TLException) -> None:
    """Pretty-print a TLException; uses the active Console if available."""
    ( _active_console.get() or Console(stderr=True) ).print(e)


def run_with_diagnostics( # type: ignore
    *,
    color: str | None = None,
    pretty: bool | str | None = None,
    only_tasklattice: bool = True,
    exit_on_exception: bool = True,
):
    """
    Decorator: runs the function inside `use_diagnostics(...)`.
    If a TLException escapes, pretty-print it and (by default) exit 2.
    """
    def deco(fn): # type: ignore
        @functools.wraps(fn)
        def wrapper(*args, **kwargs): # type: ignore
            with use_diagnostics(color=color, pretty=pretty, only_tasklattice=only_tasklattice):
                try:
                    return fn(*args, **kwargs)
                except TLException as e:
                    print_exception(e)
                    if exit_on_exception:
                        raise SystemExit(2)
                    raise
        return wrapper
    return deco

