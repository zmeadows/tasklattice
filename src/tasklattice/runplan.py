from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Tuple, Mapping
from types import MappingProxyType


# --------------------------- enums & small types ---------------------------

class LinkMode(StrEnum):
    COPY = "copy"        # safest
    SYMLINK = "symlink"  # fastest for large trees; beware external mutation
    HARDLINK = "hardlink"


class ConflictPolicy(StrEnum):
    FAIL = "fail"
    OVERWRITE = "overwrite"
    SKIP_IF_IDENTICAL = "skip_if_identical"


# ======================= RenderFileSpec (rename later) ======================
# Alternate names you could choose from (pick one and rename the class):
# - RenderFileSpec
# - TemplateTarget
# - FileRender
# - RenderMapping
# - TemplateRender
# - GeneratedFileSpec
# - RenderInstruction
# - RenderRule
# - InputTemplateSpec
# - RenderedInput
# - TemplateArtifact
# - RenderPlanFile

@dataclass(frozen=True, slots=True)
class RenderFileSpec:
    """
    Describe one file to be rendered from placeholders.

    Assumptions:
    - The *template text* lives in the prototype directory at `source_relpath`.
    - During materialization, we DO NOT copy that path from prototype → temp run dir.
      Instead, we render it and write back to the target path.
    - By default, the target path is the same as `source_relpath`. You can override with
      `target_relpath` to write elsewhere inside the run directory.
    """
    # Required
    source_relpath: str                          # e.g., "input/config.yaml" (relative to prototype_dir)

    # Optional
    target_relpath: str | None = None     # default: same as relpath
    on_conflict: ConflictPolicy = ConflictPolicy.OVERWRITE
    encoding: str = "utf-8"               # text rendering; None not yet supported
    mode: int = 0o644


# ============================ RunPlan (rename later) =========================
# Alternate names you could choose from (pick one and rename the class):
# - RunPlan
# - RunBlueprint
# - RunRecipe
# - MaterializationPlan
# - DirectoryPlan
# - PrototypeOverlayPlan
# - RunScaffold
# - RunSetup
# - RenderPlan
# - BuildPlan
# - RunDesign
# - RunAssembly

@dataclass(frozen=True, slots=True)
class RunPlan:
    """
    Blueprint to materialize a single run directory via Plan A:
      1) Create a fresh temp directory
      2) Copy/symlink/hardlink the prototype directory into it with include/exclude filters,
         always skipping all RenderFileSpec target paths (to avoid stale files)
      3) For each RenderFileSpec, read template from prototype_dir/<source_relpath>,
         render with subs, normalize text, and write to target (default: same path)
      4) Atomic-rename temp → final run directory

    No SubstitutionMap is baked in. This is the last "pure definition" object
    before crossing into actual results (Experiment).
    """
    # Identity & where runs live
    name: str
    root_dir: Path
    prototype_dir: Path

    # What to render (paths relative to prototype_dir)
    render_files: Tuple[RenderFileSpec, ...]

    # Prototype transfer behavior (copy/link + filtering)
    include_globs: Tuple[str, ...] = ("**/*",)  # applied before exclude
    exclude_globs: Tuple[str, ...] = (          # safe defaults
        ".git/**", ".hg/**", ".svn/**",
        "__pycache__/**", ".DS_Store", "Thumbs.db",
        ".tl/**", "._tl/**",
    )
    link_mode: LinkMode = LinkMode.COPY

    # Text rendering normalization (applies only to rendered writes)
    newline: str | None = "\n"            # None = leave as produced by renderer
    ensure_trailing_newline: bool = True  # if newline is not None and missing, append

    # Optional post-run space reclamation (runner/util deletes after success)
    post_run_prune_globs: Tuple[str, ...] = tuple()

    # Constant provenance copied into each run's metadata (same across variations)
    meta: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))

    # --- to be implemented elsewhere (signatures shown for orientation) ---
    # def run_id_for(self, subs: SubstitutionMap) -> str: ...
    # def suggested_dir_for(self, subs: SubstitutionMap) -> Path: ...
    # def materialize(self, subs: SubstitutionMap) -> RunMaterialized: ...
    # def file_manifest_for(self, subs: SubstitutionMap) -> list[PlannedFile]: ...
    # (Plan A uses a fresh temp dir + atomic rename; no in-place overlays.)


# -----------------------------------------------------------------------------
# NOTES / REMINDERS (agreed concepts not yet encoded here; keep until implemented)
# -----------------------------------------------------------------------------
# - Boundary: RunPlan + Lattice are pure definitions. Producing results happens via
#   go(plan, lattice, runner, ...) -> Experiment, which materializes/launches runs.
#
# - Plan A only: always create a temp run dir, copy prototype (filtered), render targets,
#   then atomic-rename into place. No "reuse in place" overlays right now.
#
# - Deny-set during prototype copy: skip every target path for each RenderFileSpec.
#   (target = spec.target_relpath or spec.source_relpath)
#
# - No placeholders in target paths for now. Templates are valid YAML/JSON/etc.;
#   the source path equals the default target path.
#
# - Newline normalization: after render(), if newline is not None, normalize all CRLF/CR to newline.
#   If ensure_trailing_newline is True, append one if missing. Do this only for text (encoding not None).
#
# - post_run_prune_globs: declared on RunPlan but executed by the Runner (or a helper)
#   after a successful run to reclaim disk space.
#
# - Run IDs: recommend "hash + kv slug" scheme (e.g., ab12cd34__nx=256__scheme=rk4);
#   hash over canonical subs + input-affecting plan knobs; slug shows a few key params.
#
# - Experiment: stores snapshots of plan + lattice, index of runs, and provides refresh/validate.
#
# - Future (out of scope here, but planned):
#   * ExistingDirPolicy for optional "reuse in place" mode
#   * pre_clean_globs before rendering (dangerous; default off)
#   * assets layer (copy/symlink extra static files beyond prototype)
#   * lightweight output schema validation per run
#   * streaming vs eager enumeration; concurrency knobs for local runs
#   * cross-experiment dedupe (symlink identical materializations)
