from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from functools import partial
from types import MappingProxyType
from typing import Any, TypeAlias

from tasklattice._paths import AbsDir, RelPath, UserAbsPath, UserRelPath

# TODO: handle/validate expected file encodings
# TODO: what if prototype directory gets modified during course of TaskLattice script...?

UserRenderSpec: TypeAlias = UserRelPath | tuple[UserRelPath, UserRelPath]


@dataclass(frozen=True, slots=True)
class RenderSpec:
    source_relpath: RelPath  # e.g., "input/config.yaml" (relative to prototype_dir)
    target_relpath: RelPath  # default: same as relpath
    encoding: str = "utf-8"
    mode: int = 0o644

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


class LinkMode(StrEnum):
    """How to materialize files copied from the prototype tree."""

    COPY = "copy"  # shutil.copy2 (portable; preserves mtime/metadata)
    SYMLINK = "symlink"  # symlink to prototype (fast; requires perms on Windows)
    HARDLINK = "hardlink"  # hardlink to prototype (fast; same filesystem only)


_DEFAULT_EXCLUDE_GLOBS: tuple[str, ...] = (
    # safe defaults
    ".git/**",
    ".hg/**",
    ".svn/**",
    "__pycache__/**",
    ".DS_Store",
    "Thumbs.db",
    ".tl/**",
    "._tl/**",
)


@dataclass(frozen=True, slots=True, init=False)
class RunPlan:
    name: str
    runs_root: AbsDir
    prototype_dir: AbsDir

    render_files: tuple[RenderSpec, ...]

    link_mode: LinkMode

    include_globs: tuple[str, ...]  # applied before exclude
    exclude_globs: tuple[str, ...]  # safe defaults

    # Text rendering normalization (applies only to rendered writes)
    newline: str | None  # None = leave as produced by renderer
    ensure_trailing_newline: bool  # if newline is not None and missing, append

    # Optional post-run space reclamation (runner/util deletes after success)
    post_run_prune_globs: tuple[str, ...]

    # Constant provenance copied into each run's metadata (same across variations)
    meta: Mapping[str, Any]

    def __init__(
        self,
        name: str,
        runs_root: UserAbsPath,
        prototype_dir: UserAbsPath,
        render_files: Sequence[UserRenderSpec],
        link_mode: LinkMode = LinkMode.COPY,
        include_globs: Sequence[str] = ("**/*",),
        exclude_globs: Sequence[str] = _DEFAULT_EXCLUDE_GLOBS,
        newline: str | None = "\n",
        ensure_trailing_newline: bool = True,
        meta: Mapping[str, Any] | None = None,
    ):
        object.__setattr__(self, "name", name)

        # TODO: validate/check runs_root
        object.__setattr__(self, "runs_root", AbsDir.any(runs_root))

        pd = AbsDir.existing(prototype_dir)
        object.__setattr__(self, "prototype_dir", pd)

        object.__setattr__(self, "link_mode", link_mode)

        rs = map(partial(RenderSpec.construct, pd), render_files)
        object.__setattr__(self, "render_files", tuple(rs))

        targets = [str(rs.target_relpath) for rs in self.render_files]
        dupes = {t for t in targets if targets.count(t) > 1}
        if dupes:
            raise ValueError(f"Duplicate render targets: {sorted(dupes)}")

        # TODO: systematize/centralize names/locations of metadata folder(s)
        if any(t.startswith("_tl/") or t.startswith("._tl/") for t in targets):
            raise ValueError("Render targets may not write under reserved prefixes like '_tl/'.")

        object.__setattr__(self, "include_globs", tuple(include_globs))
        object.__setattr__(self, "exclude_globs", tuple(exclude_globs))
        object.__setattr__(self, "newline", newline)
        object.__setattr__(self, "ensure_trailing_newline", ensure_trailing_newline)
        object.__setattr__(self, "meta", MappingProxyType(dict(meta or {})))
