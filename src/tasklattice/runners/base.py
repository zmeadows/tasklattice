"""
tasklattice.runners.base
========================

Runner-side API and data models.

- RunStatus (+ TERMINAL_STATES)
- Resources, LaunchSpec
- UserLaunchInput (LaunchSpec | factory | "str cmd" | ["argv"])
- Normalization helpers for launch inputs
- Common validation helper (run_dir-aware)
- Protocols: RunHandle, Runner
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence, TypeAlias, runtime_checkable

# One-way dependency: runners -> materialize
from tasklattice.materialize import RunMaterialized

# -----------------------------------------------------------------------------
# Lifecycle model
# -----------------------------------------------------------------------------

class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"

TERMINAL_STATES: frozenset[RunStatus] = frozenset({
    RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT
})


# -----------------------------------------------------------------------------
# Portable submission model (runner-owned)
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Resources:
    """Portable resource hints (backends may ignore unsupported fields)."""
    cpus: int | None = None
    gpus: int | dict[str, int] | None = None
    mem_mb: int | None = None
    time_limit_s: int | None = None  # wall-clock timeout if > 0
    nodes: int | None = None
    tasks_per_node: int | None = None
    exclusive: bool | None = None


@dataclass(frozen=True, slots=True)
class LaunchSpec:
    """
    How to launch a materialized run.

    cmd: argv (cmd[0] must be an executable name or path).
    env: overlay on the runner process environment.
    cwd: MUST be None or a RELATIVE path (resolved under run_dir by the backend).
    stdout_path / stderr_path: optional paths. If provided, they MUST resolve under run_dir.
    resources: portable hints (advisory; time_limit_s may be enforced).
    backend_opts: free-form backend-specific knobs (namespaced like "slurm.partition").
    """
    cmd: Sequence[str]
    env: Mapping[str, str] | None = None
    cwd: Path | None = None
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    resources: Resources = Resources()
    backend_opts: Mapping[str, Any] = field(default_factory=dict)


# -----------------------------------------------------------------------------
# Launch factory normalization
# -----------------------------------------------------------------------------

LaunchSpecFactory: TypeAlias = Callable[[RunMaterialized], LaunchSpec]
UserLaunchInput: TypeAlias = LaunchSpec | LaunchSpecFactory | str | Sequence[str]

def _cmd_from_string(s: str) -> list[str]:
    """Split a shell-style command string into argv using POSIX rules."""
    return shlex.split(s, posix=True)

def ensure_launchspec(obj: UserLaunchInput) -> LaunchSpec:
    """Convert UserLaunchInput into a constant LaunchSpec (callables not accepted here)."""
    if isinstance(obj, LaunchSpec):
        return obj
    if isinstance(obj, str):
        return LaunchSpec(cmd=_cmd_from_string(obj))
    if isinstance(obj, Sequence) and not isinstance(obj, str):
        return LaunchSpec(cmd=[str(x) for x in obj])
    raise TypeError(
        "ensure_launchspec() expected LaunchSpec | str | Sequence[str]; "
        "for callables use ensure_launch_factory()."
    )

def ensure_launch_factory(obj: UserLaunchInput) -> LaunchSpecFactory:
    """Normalize any UserLaunchInput into a LaunchSpecFactory."""
    if callable(obj) and not isinstance(obj, str):
        return obj
    spec = ensure_launchspec(obj)
    def _factory(_: RunMaterialized) -> LaunchSpec:
        return spec
    return _factory


# -----------------------------------------------------------------------------
# Validation helpers
# -----------------------------------------------------------------------------

# Namespaces we accept for backend_opts keys. Add more as you add backends.
ALLOWED_BACKEND_OPT_NAMESPACES: set[str] = {"local", "slurm", "k8s"}

def _is_within(child: Path, root: Path) -> bool:
    """Return True if 'child' is inside 'root' (after resolving symlinks)."""
    try:
        child.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False

def validate_spec_common(spec: LaunchSpec, *, run_dir: Path) -> None:
    """
    Runner-agnostic sanity checks. Backends may add stricter checks.

    - cmd must be non-empty strings
    - env must be str->str if provided
    - time_limit_s must be positive (or None)
    - cwd must be None or RELATIVE (resolved under run_dir)
    - stdout/stderr, if provided, must resolve under run_dir
    - backend_opts keys must be namespaced: "<ns>.<key>", where ns is in ALLOWED_BACKEND_OPT_NAMESPACES
    """
    # cmd
    if not spec.cmd or any(not isinstance(c, str) for c in spec.cmd):
        raise ValueError("LaunchSpec.cmd must be a non-empty sequence of strings")

    # ncpus
    if spec.resources.cpus is not None and spec.resources.cpus <= 0:
        raise ValueError("Resources.cpus must be a positive integer (or None)")

    # ngpus
    if isinstance(spec.resources.gpus, int) and spec.resources.gpus <= 0:
        raise ValueError("Resources.gpus must be a positive integer (or None)")

    # mem
    if spec.resources.mem_mb is not None and spec.resources.mem_mb <= 0:
        raise ValueError("Resources.mem_mb must positive (or None)")

    # timeout
    if spec.resources.time_limit_s is not None and spec.resources.time_limit_s <= 0:
        raise ValueError("Resources.time_limit_s must be a positive integer (or None)")

    # env
    if spec.env is not None:
        for k, v in spec.env.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise ValueError("LaunchSpec.env must map str->str")

    # cwd policy
    if spec.cwd is not None:
        if Path(spec.cwd).is_absolute():
            raise ValueError("LaunchSpec.cwd must be None or a RELATIVE path (resolved under run_dir)")

    # stdout/stderr must live under run_dir if provided
    for label, p in (("stdout_path", spec.stdout_path), ("stderr_path", spec.stderr_path)):
        if p is None:
            continue
        p_abs = p if Path(p).is_absolute() else (run_dir / p)
        if not _is_within(p_abs, run_dir):
            raise ValueError(f"LaunchSpec.{label} must resolve under the run directory (got: {p})")

    # backend_opts namespacing
    for key in spec.backend_opts.keys():
        if not isinstance(key, str):
            raise ValueError("backend_opts keys must be strings")
        parts = key.split(".", 1)
        if len(parts) != 2 or not parts[0] or parts[0] not in ALLOWED_BACKEND_OPT_NAMESPACES:
            raise ValueError(
                f"backend_opts key '{key}' must be namespaced (ns.key) and ns must be one of "
                f"{sorted(ALLOWED_BACKEND_OPT_NAMESPACES)}"
            )


# -----------------------------------------------------------------------------
# Protocols
# -----------------------------------------------------------------------------

@runtime_checkable
class RunHandle(Protocol):
    def run_id(self) -> str: ...
    def external_id(self) -> str | None: ...
    def status(self) -> RunStatus: ...
    def wait(self, timeout_s: float | None = None) -> RunStatus: ...
    def cancel(self, force: bool = False, reason: str | None = None) -> None: ...
    def return_code(self) -> int | None: ...
    def stdout_path(self) -> Path | None: ...
    def stderr_path(self) -> Path | None: ...

@runtime_checkable
class Runner(Protocol):
    name: str
    def submit(self, run: RunMaterialized) -> RunHandle: ...
    def attach(self, run: RunMaterialized) -> RunHandle | None: ...
    def close(self) -> None: ...
    # Optional introspection/validation hooks:
    def effective_spec(self, run: RunMaterialized) -> LaunchSpec: ...
    def validate_spec(self, spec: LaunchSpec, *, run_dir: Path) -> None: ...

