import pytest
from tasklattice.placeholder.model import Domain, DomainInterval, DomainSet


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
def test_domain_interval_contains(interval, value, expected):
    """Check that DomainInterval.contains enforces bounds and type rules."""
    assert interval.contains(value) is expected


# --- DomainSet tests ----------------------------------------------------------

def test_domain_set_contains_basic():
    """DomainSet should report membership for exact matches."""
    domain = DomainSet(values={1, "a", 3.14})
    assert domain.contains(1)
    assert domain.contains("a")
    assert domain.contains(3.14)
    assert not domain.contains(2)
    assert not domain.contains("b")


def test_domain_set_contains_bool_edge_case():
    """Booleans are distinct from ints in DomainSet membership."""
    domain = DomainSet(values={1, False})
    # True is not in the set even though True == 1 in Python
    assert not domain.contains(True)
    # False is in the set
    assert domain.contains(False)


# --- Abstract base class guard ------------------------------------------------

def test_domain_is_abstract():
    """The Domain ABC should not be instantiable directly."""
    with pytest.raises(TypeError):
        Domain() # type: ignore[abstract]

