# tasklattice/materialize.py
#
# OVERVIEW
# ========
# This module performs the "define → produce" transition. Given a RunPlan (a
# pure description of what should be copied and which files should be rendered)
# and a SubstitutionMap (the concrete parameter values for one run), it
# materializes a real run directory on disk and returns a compact, immutable
# description of what was written.
#
# Design goals:
# - Runner-agnostic (local vs slurm doesn’t matter).
# - Deterministic run IDs constructed from a fingerprint of plan knobs and the
#   substitution map (order-independent).
# - Strict about inputs (fail fast if sources are missing or empty).
# - Small surface area with room to grow (e.g., indexing copied files, link modes).
#
# What’s new vs the very first draft:
# - materialize_run now accepts a Renderer instance (default: TLRenderer) so you
#   can swap in custom rendering strategies without touching this module.
# - A Materializer class caches parsed Templates so multiple variations of the
#   *same* plan don’t re-read/parse template files from disk.
# - _copy_tree now honors RunPlan.link_mode (copy/symlink/hardlink) with graceful
#   fallbacks to copy() if the platform/filesystem disallows links.
#
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
from tasklattice.render import Renderer, TLRenderer
from tasklattice.runplan import RunPlan, RenderSpec, LinkMode


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class FileRecord:
    '''One file produced in a run directory.'''
    target_relpath: RelPath          # target path (relative to run dir)
    source_relpath: RelPath | None   # for rendered files: template source; for copies: original relpath
    was_rendered: bool               # True for rendered files
    size_bytes: int | None = None    # size of the file written at target
    sha256: str | None = None        # digest of the file content at target


@dataclass(frozen=True, slots=True)
class RunMaterialized:
    '''Immutable description of a single *realized* run directory.'''
    run_id: str
    run_dir: AbsDir                     # final directory: <results_dir>/<run_id>
    plan_fingerprint: str               # 12-char hex digest
    subs_fingerprint: str               # 12-char hex digest
    records: tuple[FileRecord, ...]     # rendered files (optionally includes copies later)


# -----------------------------------------------------------------------------
# Public API (function)
# -----------------------------------------------------------------------------
def materialize_run(
    plan: RunPlan,
    *,
    subs: SubstitutionMap,
    renderer: Renderer | None = None,
    index_copied: bool = False,
    hash_rendered: bool = True,
    hash_copied: bool = False,
) -> RunMaterialized:
    '''Materialize exactly one run for the given plan + substitutions.

    Parameters
    ----------
    plan : RunPlan
        The blueprint for what to copy and which files to render.
    subs : SubstitutionMap
        Concrete parameter values for this variation.
    renderer : Renderer | None
        Rendering engine. Defaults to TLRenderer() which delegates to
        tasklattice.render.render(). Accepting a protocol here lets callers inject
        custom behavior or diagnostics without changing this module.
    index_copied : bool
        If True, include FileRecord entries for *copied/linked* files (not just
        rendered outputs). Defaults to False for speed.
    hash_rendered : bool
        If True, compute SHA-256 digests for rendered outputs and store them in
        FileRecord.sha256. Defaults to True.
    hash_copied : bool
        If True and index_copied is True, compute SHA-256 digests for copied files
        as well. Defaults to False (can be expensive in large trees).

    Returns
    -------
    RunMaterialized
        Summary of what was created, including the final run directory path.

    Notes
    -----
    This function is a convenience wrapper around the Materializer class below.
    If you will materialize many runs from the *same* plan, prefer constructing
    a Materializer(plan, ...) once and calling .run(subs) repeatedly to avoid
    re-reading/parsing templates from disk.
    '''
    mat = Materializer(
        plan,
        renderer=renderer or TLRenderer(),
        index_copied=index_copied,
        hash_rendered=hash_rendered,
        hash_copied=hash_copied,
    )
    return mat.run(subs)


# -----------------------------------------------------------------------------
# Public API (class with template caching)
# -----------------------------------------------------------------------------
class Materializer:
    '''Materializes run directories for a fixed plan, caching parsed templates.

    Caching policy
    --------------
    - Templates are loaded and parsed once per Materializer instance.
    - We do not currently watch for on-disk template changes; construct a new
      Materializer if your plan or source files change.
    '''
    def __init__(
        self,
        plan: RunPlan,
        *,
        renderer: Renderer | None = None,
        index_copied: bool = False,
        hash_rendered: bool = True,
        hash_copied: bool = False,
    ) -> None:
        self.plan = plan
        self.renderer: Renderer = renderer or TLRenderer()
        self.index_copied = index_copied
        self.hash_rendered = hash_rendered
        self.hash_copied = hash_copied

        # Preload and parse all template sources so subsequent runs avoid I/O & parsing.
        self._template_cache: dict[RenderSpec, Template] = {}
        for rs in self.plan.render_files:
            src_abs = rs.source_relpath.join_under(self.plan.prototype_dir.path)
            if not src_abs.is_file():
                raise FileNotFoundError(
                    f'Template not found: {rs.source_relpath} under {self.plan.prototype_dir.path}'
                )
            src = Source.from_file(src_abs, rs.encoding)
            tpt = Template.from_source(src)
            self._template_cache[rs] = tpt

        # Build deny set once (POSIX rel strings) for copy phase.
        self._deny_set: set[str] = {str(rs.target_relpath) for rs in self.plan.render_files}

    # -- main entry
    def run(self, subs: SubstitutionMap) -> RunMaterialized:
        # 1) Compute identifiers and allocate staging paths
        plan_fp = _plan_fingerprint(self.plan)
        subs_fp = _subs_fingerprint(subs)
        run_id = _make_run_id(plan_fp, subs_fp)

        runs_root: Path = self.plan.runs_dir.path
        final_dir = runs_root / run_id
        if final_dir.exists():
            # You can switch to 'reuse' or 'nuke & recreate' later if you prefer.
            raise FileExistsError(final_dir)

        tmp_dir = _mktemp_under(runs_root, prefix=f'.tmp-{run_id}-')

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
            # Render text with the injected engine (validates names/types via renderer)
            rendered_text = self.renderer.render_template(tpt, subs)

            # Apply newline policy from the plan (kept outside the renderer so the
            # renderer stays purely 'Template → str').
            if self.plan.newline is not None:
                normalized = rendered_text.replace('\\r\\n', '\\n').replace('\\r', '\\n')
                if self.plan.newline != '\\n':
                    normalized = normalized.replace('\\n', self.plan.newline)
                if self.plan.ensure_trailing_newline and not normalized.endswith(self.plan.newline):
                    normalized += self.plan.newline
                rendered_text = normalized

            # Write to target path under the temp dir.
            dst_abs = rs.target_relpath.join_under(tmp_dir)
            dst_abs.parent.mkdir(parents=True, exist_ok=True)
            dst_abs.write_text(rendered_text, encoding='utf-8')

            # (Optional) If/when RenderSpec gains a mode field, chmod here.

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

        # 4) Atomically move the staged directory into place
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp_dir, final_dir)

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

        return RunMaterialized(
            run_id=run_id,
            run_dir=AbsDir.existing(final_dir),
            plan_fingerprint=plan_fp,
            subs_fingerprint=subs_fp,
            records=tuple(records),
        )


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _mktemp_under(parent: Path, *, prefix: str) -> Path:
    '''Create a unique temporary directory under a given parent directory.'''
    parent.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=str(parent)))


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
    '''Copy/link a directory tree honoring include/exclude/deny lists.

    Paths are compared using POSIX-style relative strings (e.g., 'a/b/c.txt').
    '''
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
            dst_file = Path(dst) / Path(*relpath.split('/'))
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
            else:
                # Future-proof: default to copy
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
    '''Create FileRecord entries for copied/linked files under root.'''
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
    '''Compute SHA-256 digest of a file in streaming fashion (1 MiB chunks).'''
    h = hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _plan_fingerprint(plan: RunPlan) -> str:
    '''Hash plan knobs that affect on-disk results (independent of subs).'''
    payload = {
        'include': tuple(plan.include_globs),
        'exclude': tuple(plan.exclude_globs),
        'newline': plan.newline,
        'ensure_trailing_newline': plan.ensure_trailing_newline,
        'link_mode': str(plan.link_mode),
        'render_pairs': tuple((str(rs.source_relpath), str(rs.target_relpath)) for rs in plan.render_files),
    }
    return _hash_stable(payload)


def _subs_fingerprint(subs: SubstitutionMap) -> str:
    '''Order-independent, stable fingerprint of the substitution map.'''
    items: list[tuple[str, ValueLiteral]] = [(str(k), v) for k, v in subs.items()]
    items.sort(key=lambda kv: kv[0])
    return _hash_stable(items)


def _hash_stable(obj: Any) -> str:
    '''Stable JSON-based hashing utility used by both plan/subs fingerprints.

    sort_keys=True + compact separators give stable, minimal digests.
    '''
    import json
    blob = json.dumps(obj, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()[:12]


def _make_run_id(plan_fp: str, subs_fp: str) -> str:
    '''Compose final run_id from the plan/subs fingerprints.'''
    return f'{plan_fp}-{subs_fp}'
