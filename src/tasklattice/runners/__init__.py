"""
tasklattice.runners
===================

Unified import surface + tiny registry.
"""

from __future__ import annotations
from importlib import import_module

from tasklattice.runners.base import (
    Runner,
    RunHandle,
    RunStatus,
    TERMINAL_STATES,
    Resources,
    LaunchSpec,
    LaunchSpecFactory,
    UserLaunchInput,
    ensure_launchspec,
    ensure_launch_factory,
    validate_spec_common,
)

_REGISTRY: dict[str, str] = {
    "local": "tasklattice.runners.local:LocalRunner",
}

def resolve_runner(name: str, **kwargs) -> Runner: # type: ignore
    """
    Instantiate a runner by registry name. Intended for internal use by higher-level code.

    Example:
        runner = resolve_runner("local", launch="python main.py")
    """
    try:
        target = _REGISTRY[name]
    except KeyError as e:
        raise KeyError(f"Unknown runner '{name}'. Known: {sorted(_REGISTRY)}") from e
    mod_name, cls_name = target.split(":")
    cls = getattr(import_module(mod_name), cls_name)
    return cls(**kwargs)  # type: ignore

__all__ = [
    "Runner",
    "RunHandle",
    "RunStatus",
    "TERMINAL_STATES",
    "Resources",
    "LaunchSpec",
    "LaunchSpecFactory",
    "UserLaunchInput",
    "ensure_launchspec",
    "ensure_launch_factory",
    "validate_spec_common",
    "resolve_runner",
]

