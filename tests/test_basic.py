
from lattice import Lattice

def test_lattice_creation() -> None:
    lattice = Lattice(templates=["input.yaml"])
    assert lattice.templates == ["input.yaml"]
    assert lattice.sweeps == []

def test_add_zip() -> None:
    lattice = Lattice(templates=["input.yaml"])
    lattice.add_zip({"x": [1, 2], "y": [3, 4]})
    assert lattice.sweeps[0]["type"] == "zip"

def test_add_product() -> None:
    lattice = Lattice(templates=["input.yaml"])
    lattice.add_product({"a": [10, 20], "b": [30, 40]})
    assert lattice.sweeps[0]["type"] == "product"
