"""
TaskLattice diagnostics: shared data model, rich renderer (code frames with carets),
and a lightweight Emitter. Designed to be used by both warnings and exceptions.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from rich.console import Console, Group, RenderableType
from rich.panel import Panel
from rich.rule import Rule
from rich.text import Text

from tasklattice.source import Source, SourceIndex, SourceSpan

__all__ = [
    "Severity",
    "Related",
    "Diagnostic",
    "FrameConfig",
    "Theme",
    "Emitter",
    "render_diagnostic",
    "info",
    "warn",
    "error",
]


# ────────────────────────── Core model ──────────────────────────


class Severity(StrEnum):
    INFO = "info"
    WARN = "warn"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class Related:
    label: str
    span: SourceSpan
    source: Source


@dataclass(frozen=True, slots=True)
class Diagnostic:
    message: str
    severity: Severity
    span: SourceSpan
    source: Source
    code: str | None = None
    notes: list[str] = field(default_factory=list)
    hint: str | None = None
    related: list[Related] = field(default_factory=list)


# ─────────────────────── Rendering config/theme ───────────────────────


@dataclass(frozen=True, slots=True)
class FrameConfig:
    context_lines: int = 2
    tab_width: int = 4
    show_line_numbers: bool = True
    max_related: int = 6  # cap to avoid huge dumps


@dataclass(frozen=True, slots=True)
class Theme:
    info_header: str = "bold cyan"
    warn_header: str = "bold yellow"
    error_header: str = "bold red"
    filename: str = "italic"
    line_no: str = "dim"
    code: str = ""
    caret: str = "bold red"
    note_bullet: str = "dim"
    hint_label: str = "italic dim"


def _sev_style(sev: Severity, theme: Theme) -> str:
    return {
        Severity.INFO: theme.info_header,
        Severity.WARN: theme.warn_header,
        Severity.ERROR: theme.error_header,
    }[sev]


# ────────────────────────── Line/column helpers ──────────────────────────


def _expand_tabs(s: str, tabw: int) -> str:
    return s.expandtabs(tabw)


def _display_col(raw_line: str, raw_col_1: int, tabw: int) -> int:
    """
    Convert a 1-indexed raw column (with tabs) into a 1-indexed display column
    after tab expansion, to keep carets visually aligned.
    """
    prefix = raw_line[: max(0, raw_col_1 - 1)]
    return len(_expand_tabs(prefix, tabw)) + 1


def _line_bounds(line_starts: Sequence[SourceIndex], i1: int, text_len: int) -> tuple[int, int]:
    idx = i1 - 1
    start = int(line_starts[idx])
    end = int(line_starts[idx + 1]) if idx + 1 < len(line_starts) else text_len
    return start, end


def _context_window(
    source: Source, start_line: int, end_line: int, cfg: FrameConfig
) -> tuple[int, int]:
    # `line_starts` includes a sentinel at len(contents); last real line is len-1
    max_line = max(1, len(source.line_starts) - 1)
    lo = max(1, start_line - cfg.context_lines)
    hi = min(max_line, end_line + cfg.context_lines)
    return lo, hi


def _source_label(source: Source) -> str:
    """
    Returns a short label for the source. Uses basename if a file/path is present,
    otherwise '<string>'.
    """
    p = getattr(source, "file", None) or getattr(source, "path", None)
    try:
        return p.name if p is not None else "<string>"
    except Exception:
        return "<string>"


# ────────────────────────── Frame builder ──────────────────────────


def _build_code_frame(
    source: Source,
    span: SourceSpan,
    severity: Severity,
    theme: Theme,
    cfg: FrameConfig,
) -> RenderableType:
    """
    Visual code frame with context lines and carets, handling multi-line spans and tabs.
    """
    text = source.contents
    s_line, s_col = source.pos_to_line_col(span.start)  # 1-indexed
    e_line, e_col = source.pos_to_line_col(span.end)  # 1-indexed (end-exclusive)

    lo_line, hi_line = _context_window(source, s_line, e_line, cfg)

    header = Text()
    header.append(_source_label(source), style=theme.filename)
    header.append(":")
    header.append(f"{s_line}:{s_col}", style=theme.line_no)

    gutter_w = len(str(hi_line))
    lines: list[Text] = []

    for line_no in range(lo_line, hi_line + 1):
        lstart, lend = _line_bounds(source.line_starts, line_no, len(text))
        raw_line = text[lstart:lend].rstrip("\n\r")
        disp_line = _expand_tabs(raw_line, cfg.tab_width)
        code_line = Text(disp_line, style=theme.code)

        if cfg.show_line_numbers:
            gutter = Text(f"{line_no:>{gutter_w}}", style=theme.line_no)
            lines.append(Text.assemble(gutter, Text(" | "), code_line))
        else:
            lines.append(code_line)

        if not (s_line <= line_no <= e_line):
            continue

        if line_no == s_line:
            start_disp_col = _display_col(raw_line, s_col, cfg.tab_width)
        else:
            start_disp_col = 1

        if line_no == e_line:
            end_disp_col = _display_col(raw_line, e_col, cfg.tab_width)
        else:
            end_disp_col = len(disp_line) + 1

        caret_w = max(1, end_disp_col - start_disp_col)
        prefix_spaces = " " * (
            gutter_w + (3 if cfg.show_line_numbers else 0) + (start_disp_col - 1)
        )
        caret = Text(prefix_spaces)
        caret.append("^" * caret_w, style=theme.caret)
        lines.append(caret)

    body = Text()
    for i, ln in enumerate(lines):
        if i:
            body.append("\n")
        body.append(ln)

    return Panel.fit(body, title=header, border_style=_sev_style(severity, theme), padding=(0, 1))


def render_diagnostic(
    d: Diagnostic,
    *,
    theme: Theme | None = None,
    cfg: FrameConfig | None = None,
) -> RenderableType:
    """
    Assemble a Rich renderable for a Diagnostic: header, rule, main code frame,
    related frames (capped), and optional notes/hint.
    """
    theme = theme or Theme()
    cfg = cfg or FrameConfig()

    head = Text()
    head.append(f"{d.severity.upper()}", style=_sev_style(d.severity, theme))
    if d.code:
        head.append(f" [{d.code}]")
    head.append(f": {d.message}")

    main = _build_code_frame(d.source, d.span, d.severity, theme, cfg)

    rel_blocks: list[RenderableType] = []
    if d.related:
        rel = d.related[: cfg.max_related]
        for r in rel:
            rel_head = Text(r.label, style=theme.line_no)
            rel_frame = _build_code_frame(r.source, r.span, d.severity, theme, cfg)
            rel_blocks.extend([rel_head, rel_frame])
        omitted = len(d.related) - len(rel)
        if omitted > 0:
            rel_blocks.append(
                Text(f"... and {omitted} more related locations", style=theme.line_no)
            )

    trailer = Text()
    for n in d.notes:
        trailer.append("\n• ", style=theme.note_bullet)
        trailer.append(n)
    if d.hint:
        trailer.append("\n")
        trailer.append("Hint: ", style=theme.hint_label)
        trailer.append(d.hint)

    return Group(
        head,
        Rule(style=_sev_style(d.severity, theme)),
        main,
        *rel_blocks,
        *([trailer] if trailer.plain else []),
    )


# ────────────────────────── Emitter (opt-in) ──────────────────────────


class Emitter:
    """
    Lightweight printer for diagnostics. You can create ad-hoc instances,
    or use the module-level helpers below.
    """

    def __init__(
        self,
        console: Console | None = None,
        theme: Theme | None = None,
        cfg: FrameConfig | None = None,
    ):
        self.console = console or Console()
        self.theme = theme or Theme()
        self.cfg = cfg or FrameConfig()

    def emit(self, d: Diagnostic) -> None:
        self.console.print(render_diagnostic(d, theme=self.theme, cfg=self.cfg))

    def info(
        self,
        message: str,
        source: Source,
        span: SourceSpan,
        *,
        code: str | None = None,
        hint: str | None = None,
        notes: Iterable[str] = (),
    ) -> None:
        self.emit(
            Diagnostic(
                message=message,
                severity=Severity.INFO,
                source=source,
                span=span,
                code=code,
                hint=hint,
                notes=list(notes),
            )
        )

    def warn(
        self,
        message: str,
        source: Source,
        span: SourceSpan,
        *,
        code: str | None = None,
        hint: str | None = None,
        notes: Iterable[str] = (),
        related: list[Related] | None = None,
    ) -> None:
        self.emit(
            Diagnostic(
                message=message,
                severity=Severity.WARN,
                source=source,
                span=span,
                code=code,
                hint=hint,
                notes=list(notes),
                related=related or [],
            )
        )

    def error(
        self,
        message: str,
        source: Source,
        span: SourceSpan,
        *,
        code: str | None = None,
        hint: str | None = None,
        notes: Iterable[str] = (),
        related: list[Related] | None = None,
    ) -> None:
        self.emit(
            Diagnostic(
                message=message,
                severity=Severity.ERROR,
                source=source,
                span=span,
                code=code,
                hint=hint,
                notes=list(notes),
                related=related or [],
            )
        )


# Optional module-level shortcuts (omit if you dislike globals)
_default_emitter = Emitter()


def info(*a, **k):  # type: ignore[no-untyped-def]
    _default_emitter.info(*a, **k)


def warn(*a, **k):  # type: ignore[no-untyped-def]
    _default_emitter.warn(*a, **k)


def error(*a, **k):  # type: ignore[no-untyped-def]
    _default_emitter.error(*a, **k)
