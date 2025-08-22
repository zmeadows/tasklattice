from __future__ import annotations

# from tests.utils import tl
# from tasklattice.template import Template
# import pytest

# [ FUTURE TESTS ]
# Empty & trivial sources
# - Empty string → sequence == (), params == {}
# - String with no placeholders → sequence == (SourceSpan(0, len),), params == {}
#
# Basic placeholder extraction
# - Single placeholder with leading/trailing text: "aa{{TL x = 1}}bb"
#   → sequence alternates SourceSpan, ParamName, SourceSpan
# - Placeholder-only string: "{{TL x = 1}}" → sequence == (ParamName(...),)
#
# Adjacent placeholders
# - "{{TL a=1}}{{TL b=2}}" → sequence == (ParamName("a"), ParamName("b")) with no empty spans
#
# Whitespace tolerance
# - "{{   TL   x   =   1   }}" parses same as "{{TL x = 1}}"
#
# Case sensitivity
# - "{{tl x = 1}}" should not match if regex is case-sensitive
#
# Complex bodies
# - Sets/braces inside: "{{TL y = {1,2,3}}}"
# - Strings with braces/commas: '{{TL s = "a}b{c"}}'
# - Numbers: ints, floats, exponentials ("1", "1.5", "1e-3", "-2.0")
#
# Span correctness
# - Spans are strictly increasing and non-overlapping
# - Outer span matches full {{...}}, inner span inside outer
#
# Zero-length spans
# - No SourceSpan(s, s) should appear when no literal text exists between placeholders
#
# Leading/trailing text
# - Verify text before first placeholder and after last is captured correctly
#
# Mapping immutability
# - Template.params is MappingProxyType; attempting to mutate raises TypeError
#
# Sequence content typing
# - Sequence contains only SourceSpan and ParamName
# - Number of ParamName entries == number of placeholders
#
# Placeholder provenance
# - ParamNames in sequence correspond to regex matches at expected offsets
#
# Unicode & newlines
# - Placeholders across lines parse correctly
# - Non-ASCII identifiers/literals parse without issue
#
# Invalid/non-matching edge cases
# - Lone "{{" or "}}" → no placeholders, entire text is a literal span
# - "{{TL}}" (missing content) → not matched or handled as intended
#
# Round-trip reconstruction
# - Rebuilding non-placeholder text from spans matches original text minus placeholders
#
# Performance sanity check
# - Large input with many placeholders parses in reasonable time
#
# API stubs
# - render_to_object returns Render with correct Template + subs
# - render_to_file is callable and no-ops
#
# Interoperability
# - Placeholder.from_match spans line up with Template.sequence elements
