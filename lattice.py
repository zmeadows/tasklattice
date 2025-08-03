from typing import Any


class Lattice:
    def __init__(self, templates: list[str]) -> None:
        self.templates = templates
        self.sweeps: list[dict[str, Any]] = []

    def add_zip(self, param_dict: dict[str, list[Any]]) -> None:
        self.sweeps.append({"type": "zip", "params": param_dict})

    def add_product(self, param_dict: dict[str, list[Any]]) -> None:
        self.sweeps.append({"type": "product", "params": param_dict})
