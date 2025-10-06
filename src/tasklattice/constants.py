"""
tasklattice.constants
=====================

Single place for names/paths and schema versions. Both materialization and runner
code import from here so we never duplicate strings like "_tl".
"""

from __future__ import annotations

from pathlib import Path

# ---- meta directory & filenames ---------------------------------------------------

RUN_METADATA_DIR = "_tl"

# static, written once at materialization time
INPUTS_BASENAME = "inputs.json"
FILES_BASENAME = "files.json"

# dynamic, updated by runners
RUNFILE_BASENAME = "run.json"
STDOUT_BASENAME = "stdout.log"
STDERR_BASENAME = "stderr.log"

# ---- schema versions ---------------------------------------------------------

# TODO(@zmeadows): check carefully that we are validating this where needed
FILES_SCHEMA = 0  # version of the 'files' format
INPUTS_SCHEMA = 0  # version of the 'inputs' format
RUNFILE_SCHEMA = 0  # version of the run-state format

# ---- helpers ----------------------------------------------------------------


def meta_dir(run_dir: Path) -> Path:
    """Return the metadata directory inside a run directory."""
    return run_dir / RUN_METADATA_DIR


def inputs_path(run_dir: Path) -> Path:
    """Path to the static materialization 'inputs' file."""
    return meta_dir(run_dir) / INPUTS_BASENAME


def files_path(run_dir: Path) -> Path:
    """Path to the static materialization 'files' file."""
    return meta_dir(run_dir) / FILES_BASENAME


def run_file_path(run_dir: Path) -> Path:
    """Path to the dynamic run-state file written by runners."""
    return meta_dir(run_dir) / RUNFILE_BASENAME


def default_stdout_path(run_dir: Path) -> Path:
    """Default stdout log path for a run."""
    return meta_dir(run_dir) / STDOUT_BASENAME


def default_stderr_path(run_dir: Path) -> Path:
    """Default stderr log path for a run."""
    return meta_dir(run_dir) / STDERR_BASENAME
