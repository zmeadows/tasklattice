from __future__ import annotations

import fnmatch
import hashlib
import json
import math
import os
import shutil
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tasklattice._paths import AbsDir, RelPath
from tasklattice.constants import FILES_SCHEMA, INPUTS_SCHEMA, files_path, inputs_path, meta_dir
from tasklattice.core import SubstitutionMap, ValueLiteral
from tasklattice.render import Renderer, TLRenderer
from tasklattice.run.plan import LinkMode, RenderSpec, RunPlan
from tasklattice.run.staging import DefaultStaging, StagingBackend
from tasklattice.source import Source
from tasklattice.template import Template
from tasklattice.utils.fs_utils import ensure_parent_dirs


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class FileRecord:
    """One file produced in a run directory."""

    target_relpath: RelPath  # target path (relative to run dir)
    source_relpath: (
        RelPath | None
    )  # for rendered files: template source; for copies: original relpath
    was_rendered: bool  # True for rendered files
    size_bytes: int | None = None  # size of the file written at target
    sha256: str | None = None  # digest of the file content at target


@dataclass(frozen=True, slots=True)
class RunMaterialized:
    """Immutable description of a single *realized* run directory."""

    run_id: str
    run_dir: AbsDir  # final directory: <results_dir>/<run_id>
    plan_fingerprint: str  # 12-char hex digest
    subs_fingerprint: str  # 12-char hex digest
    file_records: tuple[FileRecord, ...]  # rendered files (optionally includes copies later)


# -----------------------------------------------------------------------------
# Public API (function)
# -----------------------------------------------------------------------------
def materialize_run(
    plan: RunPlan,
    *,
    subs: SubstitutionMap,
    renderer: Renderer | None = None,
    staging: StagingBackend | None = None,
    index_copied: bool = False,
    hash_rendered: bool = True,
    hash_copied: bool = False,
) -> RunMaterialized:
    """Materialize exactly one run for the given plan + substitutions.

    Parameters
    ----------
    plan : RunPlan
        The blueprint for what to copy and which files to render.
    subs : SubstitutionMap
        Concrete parameter values for this variation.
    renderer : Renderer | None
        Rendering engine. Defaults to TLRenderer() which implements the Renderer protocol.
    staging : StagingBackend | None
        Staging backend for temp directory creation and finalization. Defaults to DefaultStaging().
    index_copied : bool
        If True, include FileRecord entries for *copied/linked* files (not just rendered outputs).
    hash_rendered : bool
        If True, compute SHA-256 digests for rendered outputs.
    hash_copied : bool
        If True and index_copied is True, compute SHA-256 for copied/linked files as well.

    Notes
    -----
    If you will materialize many runs from the *same* plan, prefer constructing
    a Materializer(plan, ...) once and calling .run(subs) repeatedly to avoid
    re-reading/parsing templates from disk.
    """
    renderer_inst: Renderer = TLRenderer() if renderer is None else renderer
    staging_inst: StagingBackend = DefaultStaging() if staging is None else staging

    mat = Materializer(
        plan,
        renderer=renderer_inst,
        staging=staging_inst,
        index_copied=index_copied,
        hash_rendered=hash_rendered,
        hash_copied=hash_copied,
    )
    return mat.run(subs)


def load_materialized(run_dir: str | os.PathLike[str] | AbsDir) -> RunMaterialized:
    """
    Load an existing, fully materialized run directory created by TaskLattice.

    Assumptions:
    - ``_tl/inputs.json`` exists and contains ``plan_fingerprint`` and ``subs_fingerprint``.
    - ``_tl/files.json`` exists (materialization-complete flag) and fully indexes the run's files.

    Raises
    ------
    FileNotFoundError
        If ``run_dir`` (or required metadata files) are missing.
    ValueError
        If metadata files are present but malformed or inconsistent.
    """
    # Normalize/validate directory handle
    rd = run_dir if isinstance(run_dir, AbsDir) else AbsDir.existing(run_dir)

    # Required metadata paths
    ip = inputs_path(rd.path)
    fp = files_path(rd.path)

    # --- inputs.json (required) ---
    if not ip.is_file():
        raise FileNotFoundError(f"Not a materialized TaskLattice run (missing {ip})")
    try:
        with ip.open("r", encoding="utf-8") as f:
            inputs = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Corrupt inputs.json at {ip}: {e}") from e

    plan_fp = inputs.get("plan_fingerprint")
    subs_fp = inputs.get("subs_fingerprint")
    if not isinstance(plan_fp, str) or not isinstance(subs_fp, str):
        raise ValueError(
            f"Malformed inputs.json at {ip}: expected string fields "
            "'plan_fingerprint' and 'subs_fingerprint'"
        )

    # Canonical run_id (stable even if dir was renamed)
    run_id = _make_run_id(plan_fp, subs_fp)

    # --- files.json (required / completion flag) ---
    if not fp.is_file():
        raise FileNotFoundError(
            f"Run is not fully materialized (missing {fp}). "
            "This usually indicates a failed or incomplete atomic finalize."
        )

    try:
        with fp.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Corrupt files.json at {fp}: {e}") from e

    if not isinstance(data, list):
        raise ValueError(f"Malformed files.json at {fp}: expected a JSON array")

    records: list[FileRecord] = []
    seen_targets: set[str] = set()

    for i, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(
                f"Malformed files.json entry #{i}: expected object, got {type(item).__name__}"
            )

        try:
            target_relpath_s = item["target_relpath"]
            if not isinstance(target_relpath_s, str):
                raise TypeError("target_relpath must be a string")

            if target_relpath_s in seen_targets:
                raise ValueError(f"Duplicate target_relpath in files.json: {target_relpath_s!r}")
            seen_targets.add(target_relpath_s)

            source_relpath_s = item.get("source_relpath")
            if source_relpath_s is not None and not isinstance(source_relpath_s, str):
                raise TypeError("source_relpath must be a string or null")

            was_rendered = bool(item["was_rendered"])

            size_bytes_v = item.get("size_bytes")
            if size_bytes_v is not None:
                size_bytes_v = int(size_bytes_v)

            sha256_v = item.get("sha256")
            if sha256_v is not None and not isinstance(sha256_v, str):
                raise TypeError("sha256 must be a string or null")

        except KeyError as e:
            raise ValueError(
                f"Malformed files.json entry #{i}: missing key {e.args[0]!r}"
            ) from None
        except Exception as e:
            raise ValueError(f"Malformed files.json entry #{i}: {e}") from e

        # Optional consistency check: referenced file exists on disk
        # (If you prefer to trust the manifest blindly, you can remove this block.)
        target_abs = rd.path / target_relpath_s
        if not target_abs.exists():
            raise FileNotFoundError(f"files.json references missing file: {target_abs}")

        records.append(
            FileRecord(
                target_relpath=RelPath(target_relpath_s),
                source_relpath=(
                    RelPath(source_relpath_s) if source_relpath_s is not None else None
                ),
                was_rendered=was_rendered,
                size_bytes=size_bytes_v,
                sha256=sha256_v,
            )
        )

    return RunMaterialized(
        run_id=run_id,
        run_dir=rd,
        plan_fingerprint=plan_fp,
        subs_fingerprint=subs_fp,
        file_records=tuple(records),
    )


# -----------------------------------------------------------------------------
# Public API (class with template caching)
# -----------------------------------------------------------------------------
class Materializer:
    """Materializes run directories for a fixed plan, caching parsed templates.

    Caching policy
    --------------
    - Templates are loaded and parsed once per Materializer instance.
    - We do not currently watch for on-disk template changes; construct a new
      Materializer if your plan or source files change.
    """

    def __init__(
        self,
        plan: RunPlan,
        *,
        renderer: Renderer | None = None,
        staging: StagingBackend | None = None,
        index_copied: bool = False,
        hash_rendered: bool = True,
        hash_copied: bool = False,
    ) -> None:
        self.plan = plan
        self.renderer: Renderer = TLRenderer() if renderer is None else renderer
        self.staging: StagingBackend = DefaultStaging() if staging is None else staging
        self.index_copied = index_copied
        self.hash_rendered = hash_rendered
        self.hash_copied = hash_copied

        # Preload and parse all template sources so subsequent runs avoid I/O & parsing.
        self._template_cache: dict[RenderSpec, Template] = {}
        for rs in self.plan.render_files:
            src_abs = rs.source_relpath.join_under(self.plan.prototype_dir.path)
            if not src_abs.is_file():
                raise FileNotFoundError(
                    f"Template not found: {rs.source_relpath} under {self.plan.prototype_dir.path}"
                )
            src = Source.from_file(src_abs, rs.encoding)
            tpt = Template.from_source(src)
            self._template_cache[rs] = tpt

        # Build deny set once (POSIX rel strings) for copy phase.
        self._deny_set: set[str] = {str(rs.target_relpath) for rs in self.plan.render_files}

    # -- main entry
    def run(self, subs: SubstitutionMap) -> RunMaterialized:
        # 1) Compute identifiers and staging paths
        plan_fp = _plan_fingerprint(self.plan)
        subs_fp = _subs_fingerprint(subs)
        run_id = _make_run_id(plan_fp, subs_fp)

        runs_root: Path = self.plan.runs_root.path
        final_dir = self.staging.final_dir(runs_root, run_id)
        if final_dir.exists():
            # You can switch to "reuse" or "nuke & recreate" later if you prefer.
            raise FileExistsError(final_dir)

        tmp_dir = self.staging.temp_dir(runs_root, run_id)

        # 2) Copy prototype → temp, honoring include/exclude and skipping render targets
        _copy_tree(
            src=self.plan.prototype_dir.path,
            dst=tmp_dir,
            include=self.plan.include_globs,
            exclude=self.plan.exclude_globs,
            deny=self._deny_set,
            link_mode=self.plan.link_mode,
            index_sink=None,  # we optionally index after rendering to include sizes/hashes
        )

        # 3) Render each template (write into tmp)
        records: list[FileRecord] = []

        for rs, tpt in self._template_cache.items():
            rendered_text = self.renderer.render_template(tpt, subs)

            # Apply newline policy from the plan (kept outside the renderer so the
            # renderer stays purely "Template → str").
            if self.plan.newline is not None:
                normalized = rendered_text.replace("\r\n", "\n").replace("\r", "\n")
                if self.plan.newline != "\n":
                    normalized = normalized.replace("\n", self.plan.newline)
                if self.plan.ensure_trailing_newline and not normalized.endswith(self.plan.newline):
                    normalized += self.plan.newline
                rendered_text = normalized

            # Write to target path under the temp dir.
            dst_abs = rs.target_relpath.join_under(tmp_dir)
            dst_abs.parent.mkdir(parents=True, exist_ok=True)
            dst_abs.write_text(rendered_text, encoding="utf-8")

            # Record size/digest for auditing and later integrity checks.
            stat = dst_abs.stat()
            sha = _sha256_file(dst_abs) if self.hash_rendered else None
            records.append(
                FileRecord(
                    target_relpath=rs.target_relpath,
                    source_relpath=rs.source_relpath,
                    was_rendered=True,
                    size_bytes=stat.st_size,
                    sha256=sha,
                )
            )

        # 4) Finalize the staged directory into place (atomic by default)
        self.staging.finalize(tmp_dir, final_dir)

        _write_inputs_json(
            final_dir,  # Path to the run's permanent directory
            params=subs,  # Mapping[ParamName, ValueLiteral]
            plan_fingerprint=plan_fp,  # str
            subs_fingerprint=subs_fp,  # str
        )

        # 5) Optionally index copied/linked files (after move so relpaths are stable)
        if self.index_copied:
            copied_records = _index_copied_files(
                root=final_dir,
                include=self.plan.include_globs,
                exclude=self.plan.exclude_globs,
                deny=self._deny_set,
                hash_files=self.hash_copied,
            )
            records.extend(copied_records)

        _write_files_json_streaming(final_dir, records)

        return RunMaterialized(
            run_id=run_id,
            run_dir=AbsDir.existing(final_dir),
            plan_fingerprint=plan_fp,
            subs_fingerprint=subs_fp,
            file_records=tuple(records),
        )


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _to_json_scalar(value: Any) -> Any:
    """
    Ensure value is JSON-serializable scalar (str/int/float/bool) and finite.
    Raise ValueError on NaN/Inf so inputs.json is always valid JSON.
    """
    # bool is a subclass of int; check order carefully
    if isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        raise ValueError(f"Non-finite float not allowed in inputs.json: {value!r}")
    # If you need lists/dicts later, relax here; for now we keep ValueLiteral scalars only.
    raise ValueError(f"Unsupported parameter value type for inputs.json: {type(value).__name__}")


def _flatten_subs_for_inputs(subs: Mapping[Any, Any]) -> dict[str, Any]:
    """
    Convert your SubstitutionMap into a plain JSON-serializable dict with string keys.
    """
    out: dict[str, Any] = {}
    for k, v in subs.items():
        out[str(k)] = _to_json_scalar(v)
    return out


def _write_inputs_json(
    run_dir: Path, *, params: Mapping[Any, Any], plan_fingerprint: str, subs_fingerprint: str
) -> None:
    """
    Write the static materialization metadata for a run (once, post-finalize).
    """
    path = inputs_path(run_dir)
    ensure_parent_dirs(path)

    payload = {
        "schema": INPUTS_SCHEMA,
        "plan_fingerprint": plan_fingerprint,
        "subs_fingerprint": subs_fingerprint,
        "params": _flatten_subs_for_inputs(params),
    }

    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")

    os.replace(tmp, path)


def _write_files_json_streaming(run_dir: Path, records: Iterable[FileRecord]) -> None:
    path = files_path(run_dir)
    ensure_parent_dirs(path)

    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write("[")
        f.write(json.dumps({"schema": FILES_SCHEMA}, separators=(",", ":")))

        for r in records:
            item = {
                "target_relpath": str(r.target_relpath),
                "source_relpath": (str(r.source_relpath) if r.source_relpath is not None else None),
                "was_rendered": r.was_rendered,
                "size_bytes": r.size_bytes,
                "sha256": r.sha256,
            }
            f.write(",")
            # compact separators keep the file small
            f.write(json.dumps(item, separators=(",", ":")))

        f.write("]\n")
        f.flush()
        os.fsync(f.fileno())

    # make the rename durable too
    os.replace(tmp, path)

    # TODO: extract this to stand-alone function to use elsewhere, perhaps just once.
    try:
        dir_fd = os.open(str(meta_dir(run_dir)), os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        pass  # best-effort on platforms that support it


def _copy_tree(
    *,
    src: Path,
    dst: Path,
    include: Sequence[str],
    exclude: Sequence[str],
    deny: set[str],
    link_mode: LinkMode,
    index_sink: list[FileRecord] | None,
) -> None:
    """Copy/link a directory tree honoring include/exclude/deny lists.

    Paths are compared using POSIX-style relative strings (e.g., "a/b/c.txt").
    """
    root_path = Path(src)
    for root, _, files in os.walk(root_path):
        rel_root = Path(root).relative_to(root_path)
        for fname in files:
            relpath = (rel_root / fname).as_posix()
            # Filter using include/exclude first
            if include and not any(fnmatch.fnmatch(relpath, pat) for pat in include):
                continue
            if exclude and any(fnmatch.fnmatch(relpath, pat) for pat in exclude):
                continue
            # Never copy declared render targets
            if relpath in deny:
                continue

            src_file = Path(root) / fname
            dst_file = Path(dst) / Path(*relpath.split("/"))
            dst_file.parent.mkdir(parents=True, exist_ok=True)

            # Choose copy strategy
            if link_mode is LinkMode.COPY:
                shutil.copy2(src_file, dst_file)
            elif link_mode is LinkMode.SYMLINK:
                try:
                    # On Windows, symlink perms may require admin; if it fails, fallback.
                    if dst_file.exists():
                        dst_file.unlink()
                    os.symlink(src_file, dst_file)
                except OSError:
                    shutil.copy2(src_file, dst_file)
            elif link_mode is LinkMode.HARDLINK:
                try:
                    if dst_file.exists():
                        dst_file.unlink()
                    os.link(src_file, dst_file)
                except OSError:
                    shutil.copy2(src_file, dst_file)

            if index_sink is not None:
                st = dst_file.stat()
                index_sink.append(
                    FileRecord(
                        target_relpath=RelPath(relpath),
                        source_relpath=RelPath(relpath),
                        was_rendered=False,
                        size_bytes=st.st_size,
                        sha256=None,  # filled later if needed
                    )
                )


def _index_copied_files(
    *,
    root: Path,
    include: Sequence[str],
    exclude: Sequence[str],
    deny: set[str],
    hash_files: bool,
) -> list[FileRecord]:
    """Create FileRecord entries for copied/linked files under root."""
    out: list[FileRecord] = []
    root_path = Path(root)
    for r, _, files in os.walk(root_path):
        rel_root = Path(r).relative_to(root_path)
        for fname in files:
            relpath = (rel_root / fname).as_posix()
            if include and not any(fnmatch.fnmatch(relpath, pat) for pat in include):
                continue
            if exclude and any(fnmatch.fnmatch(relpath, pat) for pat in exclude):
                continue
            if relpath in deny:
                continue

            p = Path(r) / fname
            st = p.stat()
            sha = _sha256_file(p) if hash_files else None
            out.append(
                FileRecord(
                    target_relpath=RelPath(relpath),
                    source_relpath=RelPath(relpath),
                    was_rendered=False,
                    size_bytes=st.st_size,
                    sha256=sha,
                )
            )
    return out


def _sha256_file(p: Path) -> str:
    """Compute SHA-256 digest of a file in streaming fashion (1 MiB chunks)."""
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _plan_fingerprint(plan: RunPlan) -> str:
    """Hash plan knobs that affect on-disk results (independent of subs)."""
    payload = {
        "include": tuple(plan.include_globs),
        "exclude": tuple(plan.exclude_globs),
        "newline": plan.newline,
        "ensure_trailing_newline": plan.ensure_trailing_newline,
        "link_mode": str(plan.link_mode),
        "render_pairs": tuple(
            (str(rs.source_relpath), str(rs.target_relpath)) for rs in plan.render_files
        ),
    }
    return _hash_stable(payload)


def _subs_fingerprint(subs: SubstitutionMap) -> str:
    """Order-independent, stable fingerprint of the substitution map."""
    items: list[tuple[str, ValueLiteral]] = [(str(k), v) for k, v in subs.items()]
    items.sort(key=lambda kv: kv[0])
    return _hash_stable(items)


def _hash_stable(obj: object) -> str:
    """Stable JSON-based hashing utility used by both plan/subs fingerprints."""
    import json

    blob = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:12]


def _make_run_id(plan_fp: str, subs_fp: str) -> str:
    """Compose final run_id from the plan/subs fingerprints."""
    return f"{plan_fp}-{subs_fp}"
