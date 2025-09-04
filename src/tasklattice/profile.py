from __future__ import annotations

from dataclasses import dataclass, replace, fields
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
import re

"""
This module defines a data-driven Profile describing how to render literals for
various templated file formats (JSON, YAML, TOML, INI/.properties/.env, XML).
"""

# ---------------------------------------------------------------------------
# Enums (string-valued for nice reprs and simple comparisons)
# ---------------------------------------------------------------------------

class ProfileId(StrEnum):
    JSON = "json"
    YAML = "yaml"
    TOML = "toml"
    INI = "ini"
    PROPERTIES = "properties"
    DOTENV = "dotenv"
    XML = "xml"  # single id; attr/text is resolved at render-time


class ProfileKind(StrEnum):
    TYPED = "typed"        # json, toml, yaml
    STRINGLY = "stringly"  # ini, properties, dotenv
    XML = "xml"            # xml file; occurrence decides attr/text


class EscapePolicy(StrEnum):
    JSON = "json"
    YAML = "yaml"
    TOML = "toml"
    PROPERTIES = "properties"  # ini/properties/dotenv family
    DOTENV = "dotenv"          # optional separate behavior
    XML = "xml"                # actual escaping chosen by context


class QuoteStyle(StrEnum):
    SINGLE = "single"
    DOUBLE = "double"


# Strategy type alias (for YAML quoting heuristic)
YAMLNeedsQuotesFn = Callable[[str], bool]


# ---------------------------------------------------------------------------
# Profile dataclass (immutable config bag)
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Profile:
    # --- identity ---
    id: ProfileId
    kind: ProfileKind

    # --- string quoting & escaping (core) ---
    strings_must_be_quoted: bool
    allowed_string_quote_styles: frozenset[QuoteStyle]
    preferred_string_quote_style: QuoteStyle
    escape_policy: EscapePolicy
    auto_quote_unquoted_strings: bool

    # --- typed scalar policy (core) ---
    typed_scalars_supported: bool
    warn_on_quoted_nonstring: bool
    coerce_nonstring_when_quoted_to_string: bool

    # --- boolean spelling (core) ---
    bool_true: str
    bool_false: str

    # --- numeric formatting (core) ---
    float_format: str
    float_precision: int | None
    strip_trailing_zeros: bool

    # --- YAML heuristics (core for YAML only) ---
    yaml_bareword_blocklist: frozenset[str]
    yaml_string_needs_quotes: YAMLNeedsQuotesFn | None

    # --- XML rules (core for XML only; attr/text resolved at render-time) ---
    xml_attributes_must_remain_quoted: bool
    xml_escape_lt_gt_amp_quot_apos: bool

    # --- INI / .properties / .env (core) ---
    keep_quotes_verbatim: bool
    properties_escape_set: frozenset[str]
    comment_prefixes: tuple[str, ...]

    # --- FUTURE knobs (kept for forward-compat, mostly no-ops now) ---
    none_supported: bool = False
    none_literal: str = "null"

    bool_style: str | None = None
    case_policy: str | None = None

    ensure_ascii: bool | None = None  # JSON-style

    decimal_separator: str | None = None
    scientific_exponent_marker: str | None = None

    prefer_single_quotes_when_escaped_fraction_high: bool | None = None
    yaml_version: str | None = None
    toml_version: str | None = None

    key_value_delimiters: tuple[str, ...] | None = None
    escape_delimiters_in_values: bool | None = None
    fold_long_lines_with_backslash: bool | None = None

    xml_prefer_apos_for_attr: bool | None = None

    # Convenience: immutable evolve helper for overrides
    def evolve(self, **overrides: Any) -> "Profile":
        _validate_override_keys(Profile, overrides)
        return replace(self, **overrides)


# ---------------------------------------------------------------------------
# Escape helpers (practical, minimal implementations)
# ---------------------------------------------------------------------------

_CONTROL_MAP_JSON: dict[int, str] = {
    0x08: "\\b",
    0x09: "\\t",
    0x0A: "\\n",
    0x0C: "\\f",
    0x0D: "\\r",
}


def escape_json(s: str, *, ensure_ascii: bool | None = None) -> str:
    r"""Escape a Python str as a JSON string *content* (no surrounding quotes).
    Minimal but correct for common ASCII; emits \uXXXX for control chars and
    optionally non-ASCII when ensure_ascii=True.
    """
    out: list[str] = []
    for ch in s:
        code = ord(ch)
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\"')
        elif code in _CONTROL_MAP_JSON:
            out.append(_CONTROL_MAP_JSON[code])
        elif code < 0x20:
            out.append(f"\\u{code:04x}")
        elif ensure_ascii:
            if code > 0x7F:
                out.append(f"\\u{code:04x}")
            else:
                out.append(ch)
        else:
            out.append(ch)
    return "".join(out)


def escape_yaml_double(s: str) -> str:
    """Escape content for YAML double-quoted style (basic subset)."""
    # YAML double-quoted has JSON-like escapes
    return (
        escape_json(s)  # good enough baseline for common cases
        .replace("\x0b", "\\v")  # if present
    )


def escape_yaml_single(s: str) -> str:
    """Escape content for YAML single-quoted style: duplicate single quotes."""
    return s.replace("'", "''")


def escape_toml_basic(s: str) -> str:
    """Escape content for TOML basic strings (double-quoted)."""
    # TOML basic strings use similar escapes to JSON (not identical, but close).
    return escape_json(s)


def escape_properties_like(s: str, extra_escapes: frozenset[str] | None = None) -> str:
    """Escape for .ini/.properties/.env style values.
    We backslash-escape control chars and backslash; optionally '=', ':' too.
    """
    extras = set(extra_escapes or ())
    out: list[str] = []
    for ch in s:
        if ch == "\\":
            out.append("\\\\")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif ch in extras:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def escape_xml_attr(s: str, *, prefer_apos: bool = False) -> str:
    """Escape XML attribute value *content* (no surrounding quotes)."""
    # Always escape &, <, >. Also escape the quote we plan to use.
    s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    if prefer_apos:
        s = s.replace("'", "&apos;")
    else:
        s = s.replace('"', "&quot;")
    return s


def escape_xml_text(s: str) -> str:
    """Escape XML text node content."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# YAML risky-bareword heuristic (kept conservative)
_RISKY_YAML: frozenset[str] = frozenset(
    {
        "y",
        "n",
        "yes",
        "no",
        "on",
        "off",
        "true",
        "false",
        "null",
        "~",
        "nan",
        "inf",
    }
)


def default_yaml_needs_quotes(s: str) -> bool:
    """Decide if an unquoted YAML scalar is risky and should be quoted.

    Render layer should: if risky and unquoted → auto-quote and print a warning.
    """
    if s == "":
        return True
    stripped = s.strip()
    if stripped != s:
        return True
    if stripped.lower() in _RISKY_YAML:
        return True
    # Obvious punctuation/patterns that often break YAML barewords
    if any(ch in s for ch in (":", "{", "}", "[", "]", ",", "#", "&", "*", "?", "|", ">", "<")):
        return True
    if s.startswith(("-", ":", "?", "@", "`")):
        return True
    return False


# ---------------------------------------------------------------------------
# Factories (built-ins). Use overrides via dataclasses.replace for tweaks.
# ---------------------------------------------------------------------------

def _mk(id: ProfileId, kind: ProfileKind, **kw: Any) -> Profile:
    return Profile(id=id, kind=kind, **kw)


def make_json_profile(overrides: Mapping[str, Any] | None = None) -> Profile:
    base = _mk(
        ProfileId.JSON,
        ProfileKind.TYPED,
        strings_must_be_quoted=True,
        allowed_string_quote_styles=frozenset({QuoteStyle.DOUBLE}),
        preferred_string_quote_style=QuoteStyle.DOUBLE,
        escape_policy=EscapePolicy.JSON,
        auto_quote_unquoted_strings=True,
        typed_scalars_supported=True,
        warn_on_quoted_nonstring=True,
        coerce_nonstring_when_quoted_to_string=True,
        bool_true="true",
        bool_false="false",
        float_format="g",
        float_precision=None,
        strip_trailing_zeros=False,
        yaml_bareword_blocklist=frozenset(),
        yaml_string_needs_quotes=None,
        xml_attributes_must_remain_quoted=False,
        xml_escape_lt_gt_amp_quot_apos=False,
        keep_quotes_verbatim=False,
        properties_escape_set=frozenset(),
        comment_prefixes=("#",),
        none_supported=True,
        none_literal="null",
        ensure_ascii=None,
    )
    return base.evolve(**(overrides or {}))


def make_yaml_profile(overrides: Mapping[str, Any] | None = None) -> Profile:
    base = _mk(
        ProfileId.YAML,
        ProfileKind.TYPED,
        strings_must_be_quoted=False,
        allowed_string_quote_styles=frozenset({QuoteStyle.SINGLE, QuoteStyle.DOUBLE}),
        preferred_string_quote_style=QuoteStyle.DOUBLE,
        escape_policy=EscapePolicy.YAML,
        auto_quote_unquoted_strings=True,
        typed_scalars_supported=True,
        warn_on_quoted_nonstring=False,
        coerce_nonstring_when_quoted_to_string=True,
        bool_true="true",
        bool_false="false",
        float_format="g",
        float_precision=None,
        strip_trailing_zeros=False,
        yaml_bareword_blocklist=_RISKY_YAML,
        yaml_string_needs_quotes=default_yaml_needs_quotes,
        xml_attributes_must_remain_quoted=False,
        xml_escape_lt_gt_amp_quot_apos=False,
        keep_quotes_verbatim=False,
        properties_escape_set=frozenset(),
        comment_prefixes=("#",),
        none_supported=True,
        none_literal="null",
        yaml_version="1.2",
    )
    return base.evolve(**(overrides or {}))


def make_toml_profile(overrides: Mapping[str, Any] | None = None) -> Profile:
    base = _mk(
        ProfileId.TOML,
        ProfileKind.TYPED,
        strings_must_be_quoted=True,
        allowed_string_quote_styles=frozenset({QuoteStyle.SINGLE, QuoteStyle.DOUBLE}),
        preferred_string_quote_style=QuoteStyle.DOUBLE,
        escape_policy=EscapePolicy.TOML,
        auto_quote_unquoted_strings=True,
        typed_scalars_supported=True,
        warn_on_quoted_nonstring=True,
        coerce_nonstring_when_quoted_to_string=True,
        bool_true="true",
        bool_false="false",
        float_format="g",
        float_precision=None,
        strip_trailing_zeros=False,
        yaml_bareword_blocklist=frozenset(),
        yaml_string_needs_quotes=None,
        xml_attributes_must_remain_quoted=False,
        xml_escape_lt_gt_amp_quot_apos=False,
        keep_quotes_verbatim=False,
        properties_escape_set=frozenset(),
        comment_prefixes=("#",),
        none_supported=False,  # TOML 1.0 has no null
        toml_version="1.0",
    )
    return base.evolve(**(overrides or {}))


def make_ini_profile(overrides: Mapping[str, Any] | None = None) -> Profile:
    base = _mk(
        ProfileId.INI,
        ProfileKind.STRINGLY,
        strings_must_be_quoted=False,
        allowed_string_quote_styles=frozenset({QuoteStyle.SINGLE, QuoteStyle.DOUBLE}),
        preferred_string_quote_style=QuoteStyle.DOUBLE,
        escape_policy=EscapePolicy.PROPERTIES,
        auto_quote_unquoted_strings=False,
        typed_scalars_supported=False,
        warn_on_quoted_nonstring=False,
        coerce_nonstring_when_quoted_to_string=True,
        bool_true="true",
        bool_false="false",
        float_format="g",
        float_precision=None,
        strip_trailing_zeros=False,
        yaml_bareword_blocklist=frozenset(),
        yaml_string_needs_quotes=None,
        xml_attributes_must_remain_quoted=False,
        xml_escape_lt_gt_amp_quot_apos=False,
        keep_quotes_verbatim=True,
        properties_escape_set=frozenset({"\\", "\n", "\r", "\t"}),
        comment_prefixes=("#", ";"),
        none_supported=False,
    )
    return base.evolve(**(overrides or {}))


def make_properties_profile(overrides: Mapping[str, Any] | None = None) -> Profile:
    base = make_ini_profile(overrides).evolve(id=ProfileId.PROPERTIES)
    return base


def make_dotenv_profile(overrides: Mapping[str, Any] | None = None) -> Profile:
    base = make_ini_profile(overrides).evolve(id=ProfileId.DOTENV, escape_policy=EscapePolicy.DOTENV)
    return base


def make_xml_profile(overrides: Mapping[str, Any] | None = None) -> Profile:
    base = _mk(
        ProfileId.XML,
        ProfileKind.XML,
        strings_must_be_quoted=False,  # for text; attributes must remain quoted
        allowed_string_quote_styles=frozenset({QuoteStyle.SINGLE, QuoteStyle.DOUBLE}),
        preferred_string_quote_style=QuoteStyle.DOUBLE,
        escape_policy=EscapePolicy.XML,
        auto_quote_unquoted_strings=False,  # render decides based on attr/text
        typed_scalars_supported=False,
        warn_on_quoted_nonstring=False,
        coerce_nonstring_when_quoted_to_string=True,
        bool_true="true",
        bool_false="false",
        float_format="g",
        float_precision=None,
        strip_trailing_zeros=False,
        yaml_bareword_blocklist=frozenset(),
        yaml_string_needs_quotes=None,
        xml_attributes_must_remain_quoted=True,
        xml_escape_lt_gt_amp_quot_apos=True,
        keep_quotes_verbatim=True,
        properties_escape_set=frozenset(),
        comment_prefixes=("<!--",),
        none_supported=False,
        xml_prefer_apos_for_attr=False,
    )
    return base.evolve(**(overrides or {}))


# ---------------------------------------------------------------------------
# Registry helpers (clone_profile, get_profile, list, infer, default)
# ---------------------------------------------------------------------------

_FACTORY_BY_ID: dict[ProfileId, Callable[[Mapping[str, Any] | None], Profile]] = {
    ProfileId.JSON: make_json_profile,
    ProfileId.YAML: make_yaml_profile,
    ProfileId.TOML: make_toml_profile,
    ProfileId.INI: make_ini_profile,
    ProfileId.PROPERTIES: make_properties_profile,
    ProfileId.DOTENV: make_dotenv_profile,
    ProfileId.XML: make_xml_profile,
}

# Lazily-filled registry: built-ins added on first access; customs on clone.
_PROFILE_REGISTRY: dict[str, Profile] = {}
_RESERVED_NAMES: frozenset[str] = frozenset(pid.value for pid in ProfileId)
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _normalize_name(name: str) -> str:
    return name.strip().lower()


def _validate_custom_name(name: str) -> None:
    if name in _RESERVED_NAMES:
        msg = (
            f"Profile name '{name}' is reserved for built-ins; choose a different name."
        )
        print(msg)
        raise ValueError(msg)
    if not _NAME_RE.match(name):
        msg = (
            "Profile names must follow C identifier rules: start with a letter or underscore, "
            "then letters/digits/underscores only."
        )
        print(msg)
        raise ValueError(msg)


def _validate_override_keys(cls: type[Profile], overrides: Mapping[str, Any] | None) -> None:
    if not overrides:
        return
    allowed = {f.name for f in fields(cls)}
    unknown = [k for k in overrides.keys() if k not in allowed]
    if unknown:
        msg = (
            "Unknown Profile override keys: " + ", ".join(sorted(unknown))
        )
        print(msg)
        raise KeyError(msg)


def _get_or_create_builtin(pid: ProfileId) -> Profile:
    key = pid.value
    prof = _PROFILE_REGISTRY.get(key)
    if prof is None:
        prof = _FACTORY_BY_ID[pid](None)
        _PROFILE_REGISTRY[key] = prof
    return prof


def get_profile(name_or_id: str | ProfileId) -> Profile:
    """Fetch a profile by custom name or by built-in id (string or enum).

    Built-ins are created lazily on first access and then cached.
    """
    if isinstance(name_or_id, ProfileId):
        return _get_or_create_builtin(name_or_id)

    key = _normalize_name(name_or_id)

    # Custom or previously-created builtin?
    prof = _PROFILE_REGISTRY.get(key)
    if prof is not None:
        return prof

    # If it's a builtin id string, create lazily.
    try:
        pid = ProfileId(key)
    except ValueError:
        msg = f"Unknown profile: {name_or_id!r}"
        print(msg)
        raise KeyError(msg)

    return _get_or_create_builtin(pid)


def clone_profile(new_name: str, /, *, base: str | ProfileId, overrides: Mapping[str, Any] | None = None) -> Profile:
    """Create and register a custom profile by cloning an existing one.

    - new_name: must follow C identifier rules (case-insensitive; stored lowercase)
    - base: an existing custom name or a built-in id (e.g., "yaml")
    - overrides: dict of Profile field overrides (validated)
    """
    name = _normalize_name(new_name)
    _validate_custom_name(name)

    if name in _PROFILE_REGISTRY:
        msg = f"A profile named '{name}' already exists. Choose a different name."
        print(msg)
        raise ValueError(msg)

    base_prof = get_profile(base)
    _validate_override_keys(Profile, overrides)
    prof = base_prof.evolve(**(overrides or {}))
    _PROFILE_REGISTRY[name] = prof
    return prof


def list_profiles() -> Iterable[str]:
    """Return all available profile names currently in the registry (customs + any built-ins that were accessed), sorted."""
    return sorted(_PROFILE_REGISTRY.keys())


# Extension mapping for inference
_EXT_TO_ID: dict[str, ProfileId] = {
    ".json": ProfileId.JSON,
    ".yaml": ProfileId.YAML,
    ".yml": ProfileId.YAML,
    ".toml": ProfileId.TOML,
    ".ini": ProfileId.INI,
    ".cfg": ProfileId.INI,
    ".properties": ProfileId.PROPERTIES,
    ".env": ProfileId.DOTENV,
    ".xml": ProfileId.XML,
}


def infer_profile(path: Path) -> Profile:
    pid = _EXT_TO_ID.get(path.suffix.lower(), ProfileId.YAML)
    return get_profile(pid)


# Thin alias some modules already expect

def default_profile(path: Path | None = None) -> Profile:
    if path is None:
        return get_profile(ProfileId.YAML)
    return infer_profile(path)


__all__ = [
    "Profile",
    "ProfileId",
    "ProfileKind",
    "EscapePolicy",
    "QuoteStyle",
    "escape_json",
    "escape_yaml_double",
    "escape_yaml_single",
    "escape_toml_basic",
    "escape_properties_like",
    "escape_xml_attr",
    "escape_xml_text",
    "default_yaml_needs_quotes",
    "make_json_profile",
    "make_yaml_profile",
    "make_toml_profile",
    "make_ini_profile",
    "make_properties_profile",
    "make_dotenv_profile",
    "make_xml_profile",
    "get_profile",
    "clone_profile",
    "list_profiles",
    "infer_profile",
    "default_profile",
]

