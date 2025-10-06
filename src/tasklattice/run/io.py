from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any

import msgspec
import msgspec.json

from tasklattice.constants import RUNFILE_SCHEMA, run_file_path
from tasklattice.utils.fs_utils import write_bytes_atomic
from tasklattice.utils.time_utils import now_iso

# TODO[@zmeadows][P2]: Better error handling in this file, potentially using
# our own TaskLattice-specific errors.


class RunStatus(StrEnum):
    STAGED = "staged"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"

    def is_terminal(self: RunStatus) -> bool:
        return self in {
            RunStatus.SUCCEEDED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.TIMED_OUT,
        }


class RunFile(msgspec.Struct, frozen=True):
    status: RunStatus = RunStatus.STAGED
    created_at: str | None = None
    submitted_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    updated_at: str | None = None
    update_reason: str | None = None
    runner_kind: str | None = None
    runner_meta: dict[str, Any] = msgspec.field(default_factory=dict)
    schema: int = RUNFILE_SCHEMA
    variant_hash: str | None = None
    exit_code: int | None = None

    def save(self, run_dir: Path) -> None:
        _ = msgspec.convert(self, type(self))  # validate
        out_bytes = msgspec.json.encode(self) + b"\n"
        write_bytes_atomic(run_file_path(run_dir), out_bytes)

    @classmethod
    def load(cls, run_dir: Path) -> RunFile:
        # TODO[@zmeadows][P2]: cross-check RUNFILE_SCHEMA here
        data = run_file_path(run_dir).read_bytes()
        return msgspec.json.decode(data, type=cls)

    def evolve(self, touch: bool = True, **changes: Any) -> RunFile:
        if touch:
            changes["updated_at"] = now_iso()
        return msgspec.structs.replace(self, **changes)

    def evolve_meta(self, touch: bool = True, **changes: Any) -> RunFile:
        new_meta = self.runner_meta | changes
        if touch:
            new_meta["updated_at"] = now_iso()
        return msgspec.structs.replace(self, runner_meta=new_meta)
