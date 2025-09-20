from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Sequence, Tuple, TypeAlias

from tasklattice._paths import (
    UserAbsPath,
    UserRelPath,
    AbsDir,
    AbsFile,
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
    def construct(prototype_dir: Path, item: UserRenderSpec) -> RenderSpec:
        match item:
            case str(rel):
                src_rel = tgt_rel = RelPath(rel)
            case (str(src), str(tgt)):
                src_rel, tgt_rel = RelPath(src), RelPath(tgt)
            case _:
                raise TypeError("UserRenderSpec must be str or (str, str)")

        src_abs = src_rel.join_under(prototype_dir)
        if not src_abs.is_file():
            raise ValueError(f"Source path doesn't exist: {src_abs}")

        return RenderSpec(src_rel, tgt_rel)

@dataclass(frozen=True, slots=True, init=False)
class RunPlan:
    name: str
    root_dir: AbsDir
    prototype_dir: AbsFile

    render_files: Tuple[RenderSpec, ...]

    include_globs: Tuple[str, ...] = ("**/*",)  # applied before exclude
    exclude_globs: Tuple[str, ...] = (          # safe defaults
        ".git/**", ".hg/**", ".svn/**",
        "__pycache__/**", ".DS_Store", "Thumbs.db",
        ".tl/**", "._tl/**",
    )

    # Text rendering normalization (applies only to rendered writes)
    newline: str | None = "\n"            # None = leave as produced by renderer
    ensure_trailing_newline: bool = True  # if newline is not None and missing, append

    # Optional post-run space reclamation (runner/util deletes after success)
    post_run_prune_globs: Tuple[str, ...] = tuple()

    # Constant provenance copied into each run's metadata (same across variations)
    meta: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __init__(self,
             name: str,
             root_dir: UserAbsPath,
             prototype_dir: UserAbsPath,
             render_files: Sequence[UserRenderSpec],
             include_globs: Sequence[str] = ("**/*",),
             exclude_globs: Sequence[str] = (".git/**","__pycache__/**",".tl/**"),
             newline: str | None = "\n",
             ensure_trailing_newline: bool = True,
             meta: Mapping[str, Any] | None = None):
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "root_dir", AbsDir.any(root_dir)) #TODO: check root_dir doesnt exist and/or is empty?
        object.__setattr__(self, "prototype_dir", AbsDir.existing(prototype_dir))
        object.__setattr__(self, "render_files", tuple(render_files))
        object.__setattr__(self, "include_globs", tuple(include_globs))
        object.__setattr__(self, "exclude_globs", tuple(exclude_globs))
        object.__setattr__(self, "newline", newline)
        object.__setattr__(self, "ensure_trailing_newline", ensure_trailing_newline)
        object.__setattr__(self, "meta", MappingProxyType(dict(meta or {})))

