from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from tasklattice.source import SourceSpan
from tasklattice.template import Template
from tasklattice.placeholder.model import  ParamResolved, Placeholder

from tasklattice.core import ValueLiteral, SubstitutionMap

from tasklattice.profile import (
    Profile,
    ProfileId,
    ProfileKind,
    EscapePolicy,
    QuoteStyle,
    escape_json,
    escape_yaml_double,
    escape_yaml_single,
    escape_toml_basic,
    escape_properties_like,
    escape_xml_attr,
    escape_xml_text,
)


def _validate_map(tpt: Template, subs: SubstitutionMap) -> None:
    for sname, svalue in subs.items():
        param = tpt.params.get(sname, None)
        if param is None:
            raise RuntimeError(f"Parameter name not found: {sname}")
        if param.domain is not None and not param.domain.contains(svalue):
            raise RuntimeError(f"Domain for parameter {sname} does not contain value: {svalue}")
        if not isinstance(svalue, param.py_type):
            actual_type = type(svalue)
            raise RuntimeError(
                f"Attempted to substitute a value of type {actual_type} for parameter of type {param.py_type}"
            )


def _render_literal(param: ParamResolved, val: ValueLiteral) -> str:
    """Render a single ValueLiteral for the given parameter at its occurrence site.

    Assumptions (current phase):
    - Each parameter is defined and used exactly once (definition site == render site).
    - Aliases/expressions are not yet supported.
    - The defining placeholder is accessible via `param.placeholder`.

    Behavior:
    - Honors occurrence quote context from the placeholder (unquoted / single / double).
    - Applies profile-specific escaping and quoting rules.
    - Prints warnings for notable situations (YAML risky barewords auto-quoted; typed
      non-strings inside quotes become strings; XML attr unquoted).

    Note: explicit `null`/None is not supported yet at the type level; when that
    ValueLiteral variant is added later, handle it here.
    """
    ph: Placeholder = param.placeholder

    src = ph.source
    prof: Profile = src.profile

    occ_quote_style = None if ph.quote is None else ph.quote.style
    is_quoted = occ_quote_style is not None

    # XML: resolve local context (attr vs text)
    xml_ctx: str | None = None
    if prof.id is ProfileId.XML:
        xml_ctx = _resolve_xml_context(ph)
        if xml_ctx == "attr" and not is_quoted and prof.xml_attributes_must_remain_quoted:
            print(
                f"WARNING: XML attribute for parameter '{param.name}' is unquoted in template; "
                "escaping will be applied but XML may be invalid."
            )

    # Booleans
    if isinstance(val, bool):
        text = prof.bool_true if val else prof.bool_false
        if is_quoted and prof.kind is ProfileKind.TYPED and prof.warn_on_quoted_nonstring:
            print(
                f"WARNING: Parameter '{param.name}' is a boolean inside quotes; emitting a *string* (typed scalar lost)."
            )
        return _emit_scalar_like(
            text,
            is_string=is_quoted or prof.kind is ProfileKind.STRINGLY,
            prof=prof,
            occ_quote_style=occ_quote_style,
            xml_context=xml_ctx,
        )

    # Integers (bool is subclass of int; already handled)
    if isinstance(val, int):
        text = str(val)
        if is_quoted and prof.kind is ProfileKind.TYPED and prof.warn_on_quoted_nonstring:
            print(
                f"WARNING: Parameter '{param.name}' is numeric inside quotes; emitting a *string* (typed scalar lost)."
            )
        return _emit_scalar_like(
            text,
            is_string=is_quoted or prof.kind is ProfileKind.STRINGLY,
            prof=prof,
            occ_quote_style=occ_quote_style,
            xml_context=xml_ctx,
        )

    # Floats
    if isinstance(val, float):
        text = _format_float(val, prof)
        if is_quoted and prof.kind is ProfileKind.TYPED and prof.warn_on_quoted_nonstring:
            print(
                f"WARNING: Parameter '{param.name}' is numeric inside quotes; emitting a *string* (typed scalar lost)."
            )
        return _emit_scalar_like(
            text,
            is_string=is_quoted or prof.kind is ProfileKind.STRINGLY,
            prof=prof,
            occ_quote_style=occ_quote_style,
            xml_context=xml_ctx,
        )

    # Everything else → treat as string
    s = str(val)

    # YAML heuristic: if unquoted & risky, auto-quote and warn.
    if (
        not is_quoted
        and prof.id is ProfileId.YAML
        and prof.yaml_string_needs_quotes is not None
        and prof.yaml_string_needs_quotes(s)
    ):
        print(
            f"WARNING: YAML string for parameter '{param.name}' looked risky unquoted; auto-quoting."
        )
        return _emit_string(
            s,
            prof=prof,
            occ_quote_style=prof.preferred_string_quote_style,
            xml_context=xml_ctx,
        )

    # JSON/TOML: if strings must be quoted and site is unquoted, add quotes now.
    if not is_quoted and prof.strings_must_be_quoted:
        return _emit_string(
            s,
            prof=prof,
            occ_quote_style=prof.preferred_string_quote_style,
            xml_context=xml_ctx,
        )

    # Otherwise, honor occurrence quoting (if any) and escape appropriately.
    return _emit_string(s, prof=prof, occ_quote_style=occ_quote_style, xml_context=xml_ctx)


# ---------------------------------------------------------------------------
# Helpers (kept local to render.py for clarity)
# ---------------------------------------------------------------------------

def _format_float(val: float, prof: Profile) -> str:
    fmt = prof.float_format or "g"
    prec = prof.float_precision
    if prec is None:
        text = format(val, fmt)
    else:
        text = format(val, f".{prec}{fmt}")
    if prof.strip_trailing_zeros and ("e" not in text and "E" not in text) and "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _resolve_xml_context(ph: Any) -> str:
    """Best-effort local scan to decide if the placeholder sits in an XML attribute
    or text node. Returns "attr" or "text".
    """
    src = ph.source
    text: str = getattr(src, "text", "")
    outer = getattr(ph, "span_outer", None)
    start = int(getattr(outer, "start", 0)) if outer is not None else 0

    # Find the nearest '<' and '>' to the left of start
    left_lt = text.rfind("<", 0, start)
    left_gt = text.rfind(">", 0, start)
    if left_lt > left_gt:
        # inside a tag
        # Try to detect an '=' between '<... ' and the left quote position
        q = getattr(ph, "quote", None)
        qpos = int(getattr(q, "left_index", start)) if q is not None and hasattr(q, "left_index") else start
        eq = text.rfind("=", left_lt, qpos)
        if eq != -1 and text.rfind(">", eq, qpos) == -1:
            return "attr"
        return "text"
    return "text"


def _emit_scalar_like(
    text: str,
    *,
    is_string: bool,
    prof: Profile,
    occ_quote_style: QuoteStyle | None,
    xml_context: str | None,
) -> str:
    """Emit a scalar token that originated as a non-string value.
    If it's considered a string at this occurrence (quoted or stringly format),
    escape/quote like a string; otherwise emit bare token.
    """
    if not is_string and prof.kind is ProfileKind.TYPED:
        return text
    return _emit_string(text, prof=prof, occ_quote_style=occ_quote_style, xml_context=xml_context)


def _emit_string(
    s: str,
    *,
    prof: Profile,
    occ_quote_style: QuoteStyle | None,
    xml_context: str | None,
) -> str:
    policy = prof.escape_policy

    # XML: escape content only; quotes (for attributes) are provided by the template
    if prof.id is ProfileId.XML:
        if xml_context == "attr":
            prefer_apos = occ_quote_style == QuoteStyle.SINGLE if occ_quote_style is not None else False
            return escape_xml_attr(s, prefer_apos=prefer_apos)
        return escape_xml_text(s)

    # Properties-like formats: quotes are literal characters
    if policy in (EscapePolicy.PROPERTIES, EscapePolicy.DOTENV):
        content = escape_properties_like(s, extra_escapes=prof.properties_escape_set)
        # If site is unquoted but strings_must_be_quoted (rare), add preferred quotes
        if occ_quote_style is None and prof.strings_must_be_quoted:
            return _wrap_with_quotes(content, prof.preferred_string_quote_style)
        return content

    # JSON
    if policy is EscapePolicy.JSON:
        content = escape_json(s, ensure_ascii=prof.ensure_ascii)
        if occ_quote_style is None:
            return '"' + content + '"'
        return content

    # TOML (basic strings)
    if policy is EscapePolicy.TOML:
        content = escape_toml_basic(s)
        if occ_quote_style is None:
            quote = QuoteStyle.DOUBLE if prof.preferred_string_quote_style == QuoteStyle.DOUBLE else QuoteStyle.SINGLE
            return _wrap_with_quotes(content, quote)
        return content

    # YAML (choose escape based on single/double)
    if policy is EscapePolicy.YAML:
        quote = occ_quote_style or prof.preferred_string_quote_style
        content = escape_yaml_single(s) if quote is QuoteStyle.SINGLE else escape_yaml_double(s)
        if occ_quote_style is None:
            return _wrap_with_quotes(content, quote)
        return content

    # Fallback
    return s


def _wrap_with_quotes(content: str, quote_style: QuoteStyle) -> str:
    if quote_style is QuoteStyle.SINGLE:
        return "'" + content + "'"
    return '"' + content + '"'


# Public API

def render(tpt: Template, subs: SubstitutionMap) -> str:
    _validate_map(tpt, subs)

    chunks: list[str] = []
    for selem in tpt.sequence:
        if isinstance(selem, SourceSpan):
            chunks.append(tpt.source.slice(selem))
        else:
            pr = tpt.params[selem]
            val = subs.get(selem, pr.default)
            chunks.append(_render_literal(pr, val))

    return "".join(chunks)

@runtime_checkable
class Renderer(Protocol):
    """Capability surface for turning a parsed Template into rendered text."""
    def render_template(self, tpt: Template, subs: SubstitutionMap) -> str:
        ...

class TLRenderer:
    """Default renderer that delegates to tasklattice.render.render()."""
    def render_template(self, tpt: Template, subs: SubstitutionMap) -> str:
        return render(tpt, subs)

