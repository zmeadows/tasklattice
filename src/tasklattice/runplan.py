from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping, Sequence, Tuple, TypeAlias
from functools import partial

from tasklattice._paths import (
    UserAbsPath,
    UserRelPath,
    AbsDir,
    RelPath
)

# TODO: handle/validate expected file encodings
# TODO: what if prototype directory gets modified during course of TaskLattice script...?

UserRenderSpec: TypeAlias = UserRelPath | Tuple[UserRelPath, UserRelPath]

@dataclass(frozen=True, slots=True)
class RenderSpec:
    source_relpath: RelPath                 # e.g., "input/config.yaml" (relative to prototype_dir)
    target_relpath: RelPath | None = None   # default: same as relpath

    @staticmethod
    def construct(prototype_dir: AbsDir, item: UserRenderSpec) -> RenderSpec:
        match item:
            case str(rel):
                src_rel = tgt_rel = RelPath(rel)
            case (str(src), str(tgt)):
                src_rel, tgt_rel = RelPath(src), RelPath(tgt)
            case _:
                raise TypeError(f"item input type must be str or (str, str), not {type(item)}")

        src_abs = src_rel.join_under(prototype_dir)
        if not src_abs.is_file():
            raise FileNotFoundError(f"Source path doesn't exist: {src_abs}")

        return RenderSpec(src_rel, tgt_rel)


_DEFAULT_EXCLUDE_GLOBS: Tuple[str, ...] = (
    # safe defaults
    ".git/**", ".hg/**", ".svn/**",
    "__pycache__/**", ".DS_Store", "Thumbs.db",
    ".tl/**", "._tl/**",
)

@dataclass(frozen=True, slots=True, init=False)
class RunPlan:
    name: str
    runs_dir: AbsDir
    prototype_dir: AbsDir

    render_files: Tuple[RenderSpec, ...]

    include_globs: Tuple[str, ...] # applied before exclude
    exclude_globs: Tuple[str, ...] # safe defaults

    # Text rendering normalization (applies only to rendered writes)
    newline: str | None            # None = leave as produced by renderer
    ensure_trailing_newline: bool  # if newline is not None and missing, append

    # Optional post-run space reclamation (runner/util deletes after success)
    post_run_prune_globs: Tuple[str, ...]

    # Constant provenance copied into each run's metadata (same across variations)
    meta: Mapping[str, Any]

    def __init__(self,
             name: str,
             runs_dir: UserAbsPath,
             prototype_dir: UserAbsPath,
             render_files: Sequence[UserRenderSpec],
             include_globs: Sequence[str] = ("**/*",),
             exclude_globs: Sequence[str] = _DEFAULT_EXCLUDE_GLOBS,
             newline: str | None = "\n",
             ensure_trailing_newline: bool = True,
             meta: Mapping[str, Any] | None = None):
        object.__setattr__(self, "name", name)

        #TODO: validate/check runs_dir 
        object.__setattr__(self, "runs_dir", AbsDir.any(runs_dir))

        pd = AbsDir.existing(prototype_dir)
        object.__setattr__(self, "prototype_dir", pd)

        rs = map(partial(RenderSpec.construct, pd), render_files)
        object.__setattr__(self, "render_files", tuple(rs))

        targets = [str(rs.target_relpath) for rs in self.render_files]
        dupes = {t for t in targets if targets.count(t) > 1}
        if dupes:
            raise ValueError(f"Duplicate render targets: {sorted(dupes)}")

        #TODO: systematize/centralize names/locations of metadata folder(s)
        if any(t.startswith("_tl/") or t.startswith("._tl/") for t in targets):
            raise ValueError("Render targets may not write under reserved prefixes like '_tl/'.")

        object.__setattr__(self, "include_globs", tuple(include_globs))
        object.__setattr__(self, "exclude_globs", tuple(exclude_globs))
        object.__setattr__(self, "newline", newline)
        object.__setattr__(self, "ensure_trailing_newline", ensure_trailing_newline)
        object.__setattr__(self, "meta", MappingProxyType(dict(meta or {})))

