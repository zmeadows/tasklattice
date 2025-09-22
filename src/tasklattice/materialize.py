# tasklattice/materialize.py
#
# OVERVIEW
# ========
# This module performs the "define → produce" transition. Given a **RunPlan** (a
# pure description of what should be copied and which files should be rendered)
# and a **SubstitutionMap** (the concrete parameter values for one run), it
# materializes a *real* run directory on disk and returns a compact, immutable
# description of what was written.
#
# Overall flow:
#   1) Compute a deterministic run_id (from a fingerprint of plan knobs + the
#      substitutions) and allocate a hidden temp directory under the plan’s
#      results root.
#   2) Copy the prototype tree into the temp directory, honoring include/exclude
#      globs, but **skipping** any files that are declared render targets.
#   3) For each RenderSpec, read the template **from the prototype** (not the
#      temp dir), render it using your existing `tasklattice.render.render`
#      function, apply newline policy, and write the result at the target path
#      in the temp directory.
#   4) Atomically rename (os.replace) the temp directory to the final run
#      directory `<results_dir>/<run_id>`.
#
# The returned value, **RunMaterialized**, points at the final directory, carries
# the fingerprints/ids and a per-file audit record (target path, optional source
# path, size and sha256). You can hand this object directly to a Runner later.
#
# Design Notes
# ------------
# - This module is **runner-agnostic** (local vs slurm doesn’t matter).
# - It depends on your existing modules and naming:
#     * `_paths.AbsDir`, `_paths.RelPath`
#     * `runplan.RunPlan`, `runplan.RenderSpec`
#     * `render.Template`, `render.Source`, and `render.render(Template, subs)`
# - We keep helpers tiny and local (copy/globs/hash). If later you want a
#   StagingBackend abstraction or different copy strategies (symlink/hardlink),
#   it can be added without changing the public `materialize_run` surface.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import fnmatch
import hashlib
import os
import shutil
import tempfile

from tasklattice._paths import AbsDir, RelPath
from tasklattice.core import SubstitutionMap, ValueLiteral
from tasklattice.template import Template
from tasklattice.source import Source
from tasklattice.render import render
from tasklattice.runplan import RunPlan


# -----------------------------------------------------------------------------
# FileRecord
# -----------------------------------------------------------------------------
# A compact, immutable record of one file present in the *materialized* run
# directory. For now we only record files created by rendering (was_rendered=True),
# but you can also extend this to include copied files later if you want a full
# inventory. `source_relpath` is included to make it easy to trace which template
# produced a given target.
@dataclass(frozen=True, slots=True)
class FileRecord:
    target_relpath: RelPath          # target path (relative to run dir)
    source_relpath: RelPath | None   # None if not applicable (reserved for future copies)
    was_rendered: bool               # True for rendered files
    size_bytes: int | None = None    # size of the file written at target
    sha256: str | None = None        # digest of the file content at target


# -----------------------------------------------------------------------------
# RunMaterialized
# -----------------------------------------------------------------------------
# The immutable description of one *real* run directory on disk. The `run_dir`
# is an AbsDir that exists when this object is returned. The two short hashes
# are useful for caching and reproducibility:
#   - plan_fingerprint: hash of plan knobs that affect on-disk results but
#     do not depend on parameter values (include/exclude/newline policy, etc.)
#   - subs_fingerprint: hash of the concrete substitutions for this run
@dataclass(frozen=True, slots=True)
class RunMaterialized:
    run_id: str
    run_dir: AbsDir                     # final directory: <results_dir>/<run_id>
    plan_fingerprint: str               # 12-char hex digest
    subs_fingerprint: str               # 12-char hex digest
    records: tuple[FileRecord, ...]     # rendered files (extendable later)


# -----------------------------------------------------------------------------
# materialize_run
# -----------------------------------------------------------------------------
# The single entry point: materialize one run described by `plan` with concrete
# substitutions `subs`. This function is intentionally narrow and pure:
#   - It does **not** submit or execute the run; it only creates the directory.
#   - It is **runner-agnostic**; callers can feed the result to any Runner.
#   - It always renders from the **prototype** and writes to a temp dir first.
#
# Error semantics:
#   - If the final run directory already exists, raise FileExistsError.
#   - If a template listed in RenderSpec is missing (repo drift), raise
#     FileNotFoundError.
#   - If any filesystem operation fails, let the OSError bubble up; callers can
#     decide how to retry/clean up.
def materialize_run(plan: RunPlan, *, subs: SubstitutionMap) -> RunMaterialized:
    # 1) Compute identifiers and allocate staging paths
    plan_fp = _plan_fingerprint(plan)
    subs_fp = _subs_fingerprint(subs)
    run_id = _make_run_id(plan_fp, subs_fp)

    runs_root: Path = plan.runs_dir.path
    final_dir = runs_root / run_id
    if final_dir.exists():
        # You can switch to "reuse" or "nuke & recreate" later if you prefer.
        raise FileExistsError(final_dir)

    tmp_dir = _mktemp_under(runs_root, prefix=f".tmp-{run_id}-")

    # 2) Copy prototype → temp, honoring include/exclude and skipping render targets
    deny_set = {str(rs.target_relpath) for rs in plan.render_files}  # use POSIX rel strings
    _copy_tree(
        src=plan.prototype_dir.path,
        dst=tmp_dir,
        include=plan.include_globs,
        exclude=plan.exclude_globs,
        deny=deny_set,
    )

    # 3) Render each template (read from prototype; write into tmp)
    records: list[FileRecord] = []

    for rs in plan.render_files:
        # Read template from the prototype dir (guard against repo drift)
        src_abs = rs.source_relpath.join_under(plan.prototype_dir.path)
        if not src_abs.is_file():
            raise FileNotFoundError(
                f"Template not found: {rs.source_relpath} under {plan.prototype_dir.path}"
            )

        # Build a Template using your existing parsing pipeline.
        # Source.from_file() will infer the Profile based on the file path (extension),
        # decode with the given encoding (default utf-8), and raise if the file is empty
        # or unreadable. Then Template.from_source(...) builds the parsed structure
        # (spans + placeholders), which `render(...)` consumes.
        # NOTE: If/when you add a per-file encoding to RenderSpec, use it here.
        src = Source.from_file(src_abs, encoding="utf-8")
        tpt = Template.from_source(src)

        # Render with your engine. The engine already validates placeholder names,
        # types, and domains via _validate_map(...) before materializing the output.
        rendered_text = render(tpt, subs)

        # Apply newline policy from the plan (kept outside the renderer so the
        # renderer stays purely "Template → str").
        if plan.newline is not None:
            # Normalize all inputs to "\n", then convert to requested newline.
            normalized = rendered_text.replace("\r\n", "\n").replace("\r", "\n")
            if plan.newline != "\n":
                normalized = normalized.replace("\n", plan.newline)
            if plan.ensure_trailing_newline and not normalized.endswith(plan.newline):
                normalized += plan.newline
            rendered_text = normalized

        # Write the rendered text to the target path under the **temp** dir.
        dst_abs = rs.target_relpath.join_under(tmp_dir)
        dst_abs.parent.mkdir(parents=True, exist_ok=True)
        dst_abs.write_text(rendered_text, encoding="utf-8")

        # (Optional) If/when RenderSpec gains a `mode` field, do a best-effort chmod here.

        # Record size/digest for auditing and later integrity checks.
        stat = dst_abs.stat()
        records.append(
            FileRecord(
                target_relpath=rs.target_relpath,
                source_relpath=rs.source_relpath,
                was_rendered=True,
                size_bytes=stat.st_size,
                sha256=_sha256_file(dst_abs),
            )
        )

    # 4) Atomically move the finished directory into place so consumers never
    #    see a partially written run.
    os.replace(tmp_dir, final_dir)

    # Return the immutable description. We wrap final_dir in AbsDir.existing()
    # to guarantee the invariant for downstream code.
    return RunMaterialized(
        run_id=run_id,
        run_dir=AbsDir.existing(final_dir),
        plan_fingerprint=plan_fp,
        subs_fingerprint=subs_fp,
        records=tuple(records),
    )


# -----------------------------------------------------------------------------
# _mktemp_under
# -----------------------------------------------------------------------------
# Create a unique temporary directory *under* a given parent directory. Keeping
# the temp dir on the same filesystem as the final dir ensures `os.replace`
# remains atomic (and fast). The parent is created if needed.
def _mktemp_under(parent: Path, *, prefix: str) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=str(parent)))


# -----------------------------------------------------------------------------
# _copy_tree
# -----------------------------------------------------------------------------
# Copy a directory tree (the prototype) into a destination (the temp run dir),
# honoring:
#   - `include`: a whitelist of glob patterns (POSIX-style relpaths)
#   - `exclude`: a blacklist of glob patterns
#   - `deny`:    an explicit set of POSIX-style relpaths that must never be copied
#
# Notes:
# - Paths are compared using POSIX-style relative strings (e.g., "a/b/c.txt"),
#   which keeps matching consistent across platforms.
# - We currently always *copy* files (shutil.copy2). If you later add a link
#   mode (symlink/hardlink), you can select the method here.
def _copy_tree(
    *,
    src: Path,
    dst: Path,
    include: Sequence[str],
    exclude: Sequence[str],
    deny: set[str],
) -> None:
    for root, _, files in os.walk(src):
        root_path = Path(root)
        rel_root = root_path.relative_to(src).as_posix()  # "." or "a/b"

        for fname in files:
            rel_posix = f"{rel_root}/{fname}" if rel_root != "." else fname

            # include/exclude filters
            if include and not any(fnmatch.fnmatch(rel_posix, pat) for pat in include):
                continue
            if exclude and any(fnmatch.fnmatch(rel_posix, pat) for pat in exclude):
                continue

            # never copy declared render targets (deny-set)
            if rel_posix in deny:
                continue

            src_file = root_path / fname
            dst_file = dst / Path(*rel_posix.split("/"))
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dst_file)


# -----------------------------------------------------------------------------
# _sha256_file
# -----------------------------------------------------------------------------
# Compute the SHA-256 digest of a file in streaming fashion (1 MiB chunks).
# Useful for detecting accidental mutations later or verifying correctness in
# tests without loading entire files into RAM.
def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# -----------------------------------------------------------------------------
# _plan_fingerprint
# -----------------------------------------------------------------------------
# Create a short hash capturing only the **Plan knobs** that affect the on-disk
# results independent of parameter values. Keeping this deliberately narrow
# allows you to recognize when two runs differ *only* by substitutions.
#
# If/when you add additional knobs (e.g., a renderer profile, link mode, etc.),
# extend this payload in a backward-compatible way.
def _plan_fingerprint(plan: RunPlan) -> str:
    payload = {
        "include": tuple(plan.include_globs),
        "exclude": tuple(plan.exclude_globs),
        "newline": plan.newline,
        "ensure_trailing_newline": plan.ensure_trailing_newline,
        # NOTE: If RunPlan grows fields like link_mode or renderer profile,
        # include them here too.
    }
    return _hash_stable(payload)


# -----------------------------------------------------------------------------
# _subs_fingerprint
# -----------------------------------------------------------------------------
def _subs_fingerprint(subs: SubstitutionMap) -> str:
    items: list[tuple[str, ValueLiteral]] = [(str(k), v) for k, v in subs.items()]
    items.sort(key=lambda kv: kv[0])
    return _hash_stable(items)


# -----------------------------------------------------------------------------
# _hash_stable
# -----------------------------------------------------------------------------
# Stable JSON-based hashing utility used by both plan/subs fingerprints.
# - sort_keys=True + compact separators give stable, minimal digests
# - we shorten to 12 hex chars for readability; adjust if you want more entropy
def _hash_stable(obj: Any) -> str:
    import json

    blob = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:12]


# -----------------------------------------------------------------------------
# _make_run_id
# -----------------------------------------------------------------------------
# Compose the final run_id from the plan/subs fingerprints. This default keeps
# the id short and reproducible. If you later want a human-readable KV slug
# suffix (like "nx=256__ny=128"), you can add it here without changing callers.
def _make_run_id(plan_fp: str, subs_fp: str) -> str:
    return f"{plan_fp}-{subs_fp}"

