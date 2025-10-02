from enum import StrEnum
from pathlib import Path
from typing import Any

from tasklattice.constants import default_stderr_path, default_stdout_path, runstate_path
from tasklattice.runners.base import LaunchSpec
from tasklattice.utils.json_utils import json_atomic_write, json_load
from tasklattice.utils.time_utils import now_iso


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


TERMINAL_STATES: frozenset[RunStatus] = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT}
)


def read_runstate(run_dir: Path) -> dict[str, Any]:
    return json_load(runstate_path(run_dir)) or {}


def write_runstate(run_dir: Path, doc: dict[str, Any]) -> None:
    json_atomic_write(runstate_path(run_dir), doc)


def update_runstate(run_dir: Path, updates: dict[str, Any]) -> None:
    doc = read_runstate(run_dir)
    doc.update(updates)
    write_runstate(run_dir, doc)


def append_runstate_event(run_dir: Path, *, state: str, reason: str) -> None:
    """
    Single path to append an event to run.json. If/when we add trimming, do it here.
    """
    doc = read_runstate(run_dir)
    ev = list(doc.get("events", []))
    ev.append({"timestamp": now_iso(), "state": state, "reason": reason})
    # TODO: If we ever want to trim: ev = ev[-MAX_EVENTS:]
    doc["events"] = ev
    write_runstate(run_dir, doc)


def spec_to_jsonable(spec: LaunchSpec, *, run_dir: Path) -> dict[str, Any]:
    """
    JSON-friendly view of the effective LaunchSpec for provenance.
    We keep cwd relative if provided, otherwise we render run_dir (absolute).
    """
    return {
        "cmd": list(spec.cmd),
        "env": dict(spec.env) if spec.env is not None else None,
        "cwd": str(spec.cwd) if spec.cwd is not None else str(run_dir),
        "stdout_path": (
            str(spec.stdout_path) if spec.stdout_path else str(default_stdout_path(run_dir))
        ),
        "stderr_path": (
            str(spec.stderr_path) if spec.stderr_path else str(default_stderr_path(run_dir))
        ),
        "resources": {
            "cpus": spec.resources.cpus,
            "gpus": spec.resources.gpus,
            "mem_mb": spec.resources.mem_mb,
            "time_limit_s": spec.resources.time_limit_s,
        },
        "backend_opts": dict(spec.backend_opts),
    }
