"""
tasklattice.constants
=====================

Single place for names/paths and schema versions. Both materialization and runner
code import from here so we never duplicate strings like "_tl".
"""

from __future__ import annotations

from pathlib import Path

# ---- directory & filenames ---------------------------------------------------

RUN_METADATA_DIR = "_tl"

INPUTS_BASENAME = "inputs.json"  # static, written once at materialization time
RUNSTATE_BASENAME = "run.json"  # dynamic, updated by runners

STDOUT_BASENAME = "stdout.log"
STDERR_BASENAME = "stderr.log"

# ---- schema versions ---------------------------------------------------------

INPUTS_SCHEMA = 1  # version of the inputs.json format
RUNSTATE_SCHEMA = 1  # version of the run.json format

# ---- helpers ----------------------------------------------------------------


def meta_dir(run_dir: Path) -> Path:
    """Return the metadata directory inside a run directory."""
    return run_dir / RUN_METADATA_DIR


def inputs_path(run_dir: Path) -> Path:
    """Path to the static materialization inputs file."""
    return meta_dir(run_dir) / INPUTS_BASENAME


def runstate_path(run_dir: Path) -> Path:
    """Path to the dynamic run-state file written by runners."""
    return meta_dir(run_dir) / RUNSTATE_BASENAME


def stdout_path(run_dir: Path) -> Path:
    """Default stdout log path for a run."""
    return meta_dir(run_dir) / STDOUT_BASENAME


def stderr_path(run_dir: Path) -> Path:
    """Default stderr log path for a run."""
    return meta_dir(run_dir) / STDERR_BASENAME
