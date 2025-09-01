import pytest
from tasklattice.placeholder.model import Domain, DomainInterval, DomainSet, Number


# --- DomainInterval tests -----------------------------------------------------

@pytest.mark.parametrize(
    "interval,value,expected",
    [
        # Inclusive boundaries
        (DomainInterval(0, 10, True, True), 0, True),
        (DomainInterval(0, 10, True, True), 10, True),
        # Exclusive boundaries
        (DomainInterval(0, 10, False, True), 0, False),
        (DomainInterval(0, 10, True, False), 10, False),
        # Inside range
        (DomainInterval(0, 10, True, True), 5, True),
        # Outside range
        (DomainInterval(0, 10, True, True), -1, False),
        (DomainInterval(0, 10, True, True), 11, False),
        # Non-numeric values
        (DomainInterval(0, 10, True, True), "5", False),
        (DomainInterval(0, 10, True, True), True, False),  # bool excluded
        (DomainInterval(0, 10, True, True), None, False),
    ],
)
def test_domain_interval_contains(interval: DomainInterval, value: Number, expected: Number) -> None:
    """Check that DomainInterval.contains enforces bounds and type rules."""
    assert interval.contains(value) is expected


# --- DomainSet tests ----------------------------------------------------------

def test_domain_set_contains_basic() -> None:
    """DomainSet should report membership for exact matches."""
    domain = DomainSet(values={1, "a", 3.14})
    assert domain.contains(1)
    assert domain.contains("a")
    assert domain.contains(3.14)
    assert not domain.contains(2)
    assert not domain.contains("b")


def test_domain_set_contains_bool_edge_case() -> None:
    """Booleans are distinct from ints in DomainSet membership."""
    domain = DomainSet(values={1, False})
    # True is not in the set even though True == 1 in Python
    assert not domain.contains(True)
    # False is in the set
    assert domain.contains(False)


# --- Abstract base class guard ------------------------------------------------

def test_domain_is_abstract() -> None:
    """The Domain ABC should not be instantiable directly."""
    with pytest.raises(TypeError):
        Domain() # type: ignore[abstract]

# [ FUTURE TESTS ]
# --- Regex & Placeholder construction ---
# - PLACEHOLDER_RE.fullmatch succeeds for simple: "{{TL x = 1}}"
# - PLACEHOLDER_RE does NOT match non-TL double-brace blocks: "{{ not TL }}"
# - PLACEHOLDER_RE handles tricky inner content without overrunning:
#   * "{{TL s = 'a}b{c'}}"
#   * "{{TL y = {1,2,3}}}" (set braces inside)
#   * "{{TL s = \"}}\"}}" (inner string contains two braces)
# - Placeholder.from_string:
#   * Returns object with span_outer covering full string and span_inner covering just "TL …"
#   * Raises ValueError for invalid inputs: "TL x = 1", "{{TL}}", "{{tl x=1}}" (case-sensitive), "{{TL x=}}"
# - Placeholder.from_match:
#   * Using a larger Source with multiple placeholders, build each Placeholder via from_match;
#     verify .text matches the Source text at the recorded spans.
#
# --- Placeholder properties & positions ---
# - .line_col() with a multi-line Source:
#   * Placeholder starting/ending on same line: columns correct
#   * Placeholder spanning lines: start line/col and end line/col are accurate
#
# --- type_raw_to_python_type mapping ---
# - "str" -> str, "int" -> int, "float" -> float, "bool" -> bool
# - Unknown strings (e.g., "float32", "number") -> None
# - Case sensitivity: "Int" or "BOOL" -> None (if mapping is intentionally case-sensitive)
#
# --- ParamName behavior ---
# - Valid names: "x", "foo_bar9" construct successfully
# - Invalid names raise ValueError: "9x", "has-dash", " space", "", "a.b"
# - __str__ returns underlying value; __repr__ equals "ParamName('name')"
# - Equality and hashing:
#   * ParamName("x") == ParamName("x") and hash equal
#   * ParamName("x") != ParamName("y")
#   * Using as dict keys works as expected
#
# --- ParamUnresolved basics ---
# - Construct with only required fields; optional fields default to None
# - Construct with all fields: py_type (str), domain (DomainIntervalUnresolved/DomainSetUnresolved),
#   description (str) are stored intact
#
# --- ParamResolved behavior ---
# - __post_init__ sets py_type to type(default):
#   * default="a" -> py_type is str
#   * default=1 -> int
#   * default=1.0 -> float
#   * default=True -> bool
# - Domain and description fields preserve passed objects/values
# - Frozen dataclass: attempting to assign to any attribute raises FrozenInstanceError
#
# --- DomainInterval.contains edge cases not covered yet ---
# - Float boundaries precisely equal to lower/upper with inclusive/exclusive flags
# - Large/small magnitudes: contains(1e-12) within [0, 1], contains(1e12) outside
# - NaN handling: value=float('nan') should be rejected (currently comparisons with NaN are False,
#   so add a test to assert desired behavior and document it)
#
# --- DomainSet.contains additional checks ---
# - Mixed types: values={"1", 1, 1.0} ensure membership semantics are as intended
# - Empty set: contains(anything) is False
# - Duplicates in construction (e.g., {1, 1.0}) collapse as per Python set semantics; membership reflects that
# - Bool identity rule already tested; add a case with True present and 1 absent (and vice versa)
#
# --- Unresolved domain holders ---
# - DomainIntervalUnresolved stores raw bounds and bracket tokens exactly as provided: "(" vs "[" and ")" vs "]"
# - DomainSetUnresolved stores entries in given order and preserves literal types (int/float/str)
#
# --- Integration: scanning + Placeholder + from_match ---
# - Given a Source string with leading text, multiple placeholders, and trailing text:
#   * Use PLACEHOLDER_RE.finditer + Placeholder.from_match on each match
#   * Verify that concatenating (non-placeholder slices) + (placeholder.text) in order reproduces original Source
