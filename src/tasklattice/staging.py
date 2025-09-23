# tasklattice/staging.py
from __future__ import annotations

from typing import Protocol, runtime_checkable
from pathlib import Path
import os
import tempfile

"""
Staging backends for building run directories.

A staging backend controls:
- where the temporary build directory lives,
- where the final (published) run directory should be,
- and how the temporary directory is finalized into the final location
  (e.g., atomic rename, copy, rsync, filesystem snapshots, etc.).

The default behavior mirrors the previous implementation: build in a mkdtemp
folder under the runs root and atomically move it into place with os.replace.
"""


@runtime_checkable
class StagingBackend(Protocol):
    # TODO: standardize runs_root vs. runs_dir
    def temp_dir(self, runs_root: Path, run_id: str) -> Path: ...
    def final_dir(self, runs_root: Path, run_id: str) -> Path: ...
    def finalize(self, tmp: Path, final_: Path) -> None: ...  # default: os.replace


class DefaultStaging:
    """Atomic staging under runs_root via mkdtemp + os.replace."""

    def temp_dir(self, runs_root: Path, run_id: str) -> Path:
        runs_root.mkdir(parents=True, exist_ok=True)
        # Prefix temp dir with the run_id to aid debugging; add random suffix for uniqueness.
        return Path(tempfile.mkdtemp(prefix=f".tmp-{run_id}-", dir=str(runs_root)))

    def final_dir(self, runs_root: Path, run_id: str) -> Path:
        return runs_root / run_id

    def finalize(self, tmp: Path, final_: Path) -> None:
        final_.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, final_)
