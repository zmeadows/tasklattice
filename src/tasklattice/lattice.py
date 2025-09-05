from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import (
    Callable,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Sequence,
    IO,
)
from abc import ABC, abstractmethod
import hashlib
import json
import sys

# External project types (assumed to exist in your repo)
from tasklattice.core import ParamName, ValueLiteral, SubstitutionMap

"""
TaskLattice — Lattice (skeleton, v2)

Goal: build a composable sweep engine that yields a sequence of
`SubstitutionMap` (Mapping[ParamName, ValueLiteral]) to be consumed by
Template.render*(...). Rendering/validation lives elsewhere.

Design principles (favor clarity over cleverness):
- A Lattice is a *pipeline of operations* (product, zip, derive/map, filter, concat, dedup, constrained product).
- Iteration is lazy; nothing is realized until you iterate `for subs in lattice`.
- Operations are immutable; builder methods return a new Lattice.
- The Lattice is template-agnostic. Validate at render-time or via a thin
  binding layer (e.g., BoundLattice) later if you want preflight checks.

This file includes:
- ConflictPolicy as an Enum, with a `match` in `_merge` for exhaustiveness.
- Flexible `ParamKey` (str | ParamName) accepted in public APIs.
- `exact_cardinality()` (iterates, optional limit).
- `to_dataframe()` (lazy import of pandas).
- `iter_with_ids()` to yield `(variant_id, SubstitutionMap)` with stable hashing.
- `_ConstrainedProductOp` for cartesian with early-pruning by predicate.
- `explain()` to print the pipeline with concise per-op details.
"""

# ————————————————————————————————————————————————————————————————
# Policy, helpers
# ————————————————————————————————————————————————————————————————

class ConflictPolicy(Enum):
    ERROR = "error"
    FIRST_WINS = "first_wins"
    LAST_WINS = "last_wins"


ParamKey = str | ParamName  # inputs can be strings for convenience

def _to_param(p: ParamKey) -> ParamName:
    return p if isinstance(p, ParamName) else ParamName(p)


def _merge(
    base: MutableMapping[ParamName, ValueLiteral],
    extra: Mapping[ParamName, ValueLiteral],
    *,
    conflict: ConflictPolicy,
) -> None:
    """Merge *extra* into *base* honoring a conflict policy.

    - ERROR: raise if a key exists in both with differing values
    - FIRST_WINS: keep the existing value in *base*
    - LAST_WINS: overwrite with *extra*
    """
    for k, v in extra.items():
        if k in base:
            if base[k] == v:
                continue
            match conflict:
                case ConflictPolicy.ERROR:
                    raise ValueError(
                        f"conflicting assignments for {k!r}: {base[k]!r} vs {v!r}"
                    )
                case ConflictPolicy.FIRST_WINS:
                    # keep base[k]
                    continue
                case ConflictPolicy.LAST_WINS:
                    base[k] = v
        else:
            base[k] = v


# ————————————————————————————————————————————————————————————————
# Operation protocol + concrete ops
# ————————————————————————————————————————————————————————————————

class _Op(ABC):
    @abstractmethod
    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        ...

    def cardinality_factor(self) -> int | None:
        """Multiplicative factor introduced by this op if statically known.
        None if unknown/variable (e.g., filters or data-dependent ops)."""
        return None

    # Introspection helpers for `explain()`
    def brief(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def details(self) -> str:
        ...


@dataclass(frozen=True, slots=True)
class _SeedOp(_Op):
    """Start the stream with a single empty mapping (optionally with defaults)."""

    defaults: Mapping[ParamName, ValueLiteral]
    conflict: ConflictPolicy

    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        _ = upstream  # explicitly mark unused
        base: dict[ParamName, ValueLiteral] = {}
        if self.defaults:
            _merge(base, self.defaults, conflict=self.conflict)
        yield MappingProxyType(base)

    def cardinality_factor(self) -> int | None:
        return 1

    def details(self) -> str:
        keys = list(self.defaults.keys())
        show = ", ".join(repr(k) for k in keys[:5])
        extra = "" if len(keys) <= 5 else f", … (+{len(keys)-5} more)"
        return f"defaults=[{show}{extra}]"


@dataclass(frozen=True, slots=True)
class _ConstOp(_Op):
    const: Mapping[ParamName, ValueLiteral]
    conflict: ConflictPolicy

    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        for u in upstream:
            d: dict[ParamName, ValueLiteral] = dict(u)
            _merge(d, self.const, conflict=self.conflict)
            yield MappingProxyType(d)

    def cardinality_factor(self) -> int | None:
        return 1

    def details(self) -> str:
        keys = list(self.const.keys())
        show = ", ".join(repr(k) for k in keys[:5])
        extra = "" if len(keys) <= 5 else f", … (+{len(keys)-5} more)"
        return f"keys=[{show}{extra}]"


@dataclass(frozen=True, slots=True)
class _ProductOp(_Op):
    name: ParamName
    values: tuple[ValueLiteral, ...]
    conflict: ConflictPolicy

    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        for u in upstream:
            for v in self.values:
                d: dict[ParamName, ValueLiteral] = dict(u)
                _merge(d, {self.name: v}, conflict=self.conflict)
                yield MappingProxyType(d)

    def cardinality_factor(self) -> int | None:
        return len(self.values)

    def details(self) -> str:
        preview = ", ".join(repr(v) for v in self.values[:3])
        extra = "" if len(self.values) <= 3 else f", … (+{len(self.values)-3} more)"
        return f"name={self.name!r}, values=[{preview}{extra}]"


@dataclass(frozen=True, slots=True)
class _ZipOp(_Op):
    columns: tuple[ParamName, ...]
    rows: tuple[tuple[ValueLiteral, ...], ...]  # shape: (length, n_cols)
    conflict: ConflictPolicy

    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        for u in upstream:
            for row in self.rows:
                d: dict[ParamName, ValueLiteral] = dict(u)
                _merge(d, dict(zip(self.columns, row)), conflict=self.conflict)
                yield MappingProxyType(d)

    def cardinality_factor(self) -> int | None:
        return len(self.rows)

    def details(self) -> str:
        cols = ", ".join(repr(c) for c in self.columns)
        sample = self.rows[0] if self.rows else ()
        sp = ", ".join(repr(v) for v in sample)
        return f"cols=[{cols}], rows={len(self.rows)}, sample=({sp})"


@dataclass(frozen=True, slots=True)
class _MapOp(_Op):
    func: Callable[[Mapping[ParamName, ValueLiteral]], Mapping[ParamName, ValueLiteral] | None]
    conflict: ConflictPolicy

    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        for u in upstream:
            out = dict(u)
            extra = self.func(u)
            if extra:
                _merge(out, extra, conflict=self.conflict)
            yield MappingProxyType(out)

    def details(self) -> str:
        name = getattr(self.func, "__qualname__", getattr(self.func, "__name__", "<fn>"))
        return f"derive={name}"


@dataclass(frozen=True, slots=True)
class _FilterOp(_Op):
    pred: Callable[[Mapping[ParamName, ValueLiteral]], bool]

    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        for u in upstream:
            if self.pred(u):
                yield u

    def details(self) -> str:
        name = getattr(self.pred, "__qualname__", getattr(self.pred, "__name__", "<pred>"))
        return f"pred={name}"


@dataclass(frozen=True, slots=True)
class _ConcatOp(_Op):
    """Concatenate another lattice's stream after the current stream."""
    tail: Lattice

    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        yield from upstream
        yield from self.tail

    def details(self) -> str:
        est = self.tail.estimated_cardinality()
        return f"tail_est={est}"


@dataclass(frozen=True, slots=True)
class _DedupOp(_Op):
    """Best-effort deduplication.

    Values must be hashable for perfect dedup; otherwise we fall back to repr().
    """

    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        seen: set[tuple[tuple[ParamName, object], ...]] = set()
        for u in upstream:
            try:
                key = tuple(sorted((k, u[k]) for k in u))
            except TypeError:
                key = tuple(sorted((k, repr(u[k])) for k in u))
            if key in seen:
                continue
            seen.add(key)
            yield u

    def details(self) -> str:
        return "dedup=True"


@dataclass(frozen=True, slots=True)
class _ConstrainedProductOp(_Op):
    """Cartesian expansion with early pruning via a constraint predicate.

    `space` provides candidate values for each parameter. The predicate `ok`
    is called on *partial* assignments during the search; it should return
    True if the current partial assignment is acceptable (so far).

    Conflict policy interactions with existing assignments:
    - ERROR: conflicting values raise immediately.
    - FIRST_WINS: if a key already exists with a different value, the branch is skipped.
    - LAST_WINS: the new value overwrites for this branch (restored on backtrack).
    """

    space: Mapping[ParamName, Sequence[ValueLiteral]]
    ok: Callable[[Mapping[ParamName, ValueLiteral]], bool]
    conflict: ConflictPolicy

    def apply(self, upstream: Iterable[SubstitutionMap]) -> Iterator[SubstitutionMap]:
        keys = list(self.space.keys())
        values = [list(self.space[k]) for k in keys]

        def dfs(idx: int, acc: dict[ParamName, ValueLiteral]) -> Iterator[SubstitutionMap]:
            if idx == len(keys):
                yield MappingProxyType(dict(acc))
                return
            k = keys[idx]
            for v in values[idx]:
                had = k in acc
                old = acc.get(k)
                did_assign = False
                # Handle conflicts against existing assignment (from base or prior dims)
                if had and old != v:
                    match self.conflict:
                        case ConflictPolicy.ERROR:
                            raise ValueError(
                                f"conflicting assignments for {k!r}: {old!r} vs {v!r}"
                            )
                        case ConflictPolicy.FIRST_WINS:
                            # skip this value; keep existing binding
                            continue
                        case ConflictPolicy.LAST_WINS:
                            acc[k] = v
                            did_assign = True
                elif not had:
                    acc[k] = v
                    did_assign = True
                # Early pruning
                if self.ok(acc):
                    yield from dfs(idx + 1, acc)
                # backtrack
                if did_assign:
                    if had:
                        if old is not None:
                            acc[k] = old
                        else:
                            acc.pop(k, None)
                    else:
                        acc.pop(k, None)

        for u in upstream:
            base = dict(u)
            # DFS works on a mutable dict we seed from `u`
            yield from dfs(0, base)

    def details(self) -> str:
        ks = list(self.space.keys())
        lens = [len(self.space[k]) for k in ks]
        show = ", ".join(f"{k!r}:{n}" for k, n in zip(ks[:5], lens[:5]))
        extra = "" if len(ks) <= 5 else f", … (+{len(ks)-5} more)"
        return f"space=[{show}{extra}], constraint={getattr(self.ok, '__name__', '<fn>')}"


# ————————————————————————————————————————————————————————————————
# Public Lattice API
# ————————————————————————————————————————————————————————————————

@dataclass(frozen=True, slots=True)
class Lattice:
    """Composable sweep description that yields SubstitutionMaps.

    This class is *not* aware of templates; pair it with Template/TemplateSet
    at render time. Keep this lightweight and iteration-focused.
    """

    _ops: tuple[_Op, ...] = field(default_factory=tuple)
    _conflict: ConflictPolicy = ConflictPolicy.ERROR

    # ——— construction ———
    def __init__(
        self,
        *,
        defaults: Mapping[ParamName, ValueLiteral] | None = None,
        conflict: ConflictPolicy = ConflictPolicy.ERROR,
    ) -> None:
        object.__setattr__(self, "_conflict", conflict)
        seed = _SeedOp(defaults or {}, conflict)
        object.__setattr__(self, "_ops", (seed,))

    def _append(self, op: _Op) -> Lattice:
        return Lattice._from_ops(self._ops + (op,), conflict=self._conflict)

    @classmethod
    def _from_ops(cls, ops: tuple[_Op, ...], *, conflict: ConflictPolicy) -> Lattice:
        obj = cls.__new__(cls)  # bypass __init__
        object.__setattr__(obj, "_ops", ops)
        object.__setattr__(obj, "_conflict", conflict)
        return obj

    # ——— user-facing ops ———
    def set_constants(self, const: Mapping[ParamKey, ValueLiteral]) -> Lattice:
        """Assign constant parameters to every variant (merging with conflicts)."""
        if not const:
            return self
        norm = { _to_param(k): v for k, v in const.items() }
        return self._append(_ConstOp(norm, self._conflict))

    def add_product(self, name: ParamKey, values: Sequence[ValueLiteral]) -> Lattice:
        """Cartesian-expand over *values* of *name*."""
        return self._append(_ProductOp(_to_param(name), tuple(values), self._conflict))

    def add_zip(self, cols: Mapping[ParamKey, Sequence[ValueLiteral]]) -> Lattice:
        """Zip *aligned* parameter sequences.

        All sequences must have equal length; they are assigned together.
        Multiple calls to `add_zip` create independent zip groups that product
        with each other.
        """
        if not cols:
            return self
        lengths = {len(v) for v in cols.values()}
        if len(lengths) != 1:
            raise ValueError(
                f"add_zip requires all columns to have same length, got {sorted(lengths)}"
            )
        n = lengths.pop()
        columns = tuple(_to_param(k) for k in cols.keys())
        rows = tuple(tuple(seq[i] for seq in cols.values()) for i in range(n))
        return self._append(_ZipOp(columns, rows, self._conflict))

    def derive(
        self,
        fn: Callable[[Mapping[ParamName, ValueLiteral]], Mapping[ParamName, ValueLiteral] | None],
    ) -> Lattice:
        """Compute/attach derived parameters from a variant.

        The function receives a read-only mapping of the current variant and
        returns a mapping to merge (or None/no-op).
        """
        return self._append(_MapOp(fn, self._conflict))

    def filter(self, pred: Callable[[Mapping[ParamName, ValueLiteral]], bool]) -> Lattice:
        """Keep only variants where pred(subs) is True."""
        return self._append(_FilterOp(pred))

    def dedup(self) -> Lattice:
        """Drop duplicates (best-effort; see _DedupOp notes)."""
        return self._append(_DedupOp())

    def concat(self, other: Lattice) -> Lattice:
        """Append another lattice's variants after this one's variants."""
        return self._append(_ConcatOp(other))

    def add_constrained_product(
        self,
        space: Mapping[ParamKey, Sequence[ValueLiteral]],
        ok: Callable[[Mapping[ParamName, ValueLiteral]], bool],
    ) -> Lattice:
        """Like a product over many params, but prunes branches via `ok(partial)`.
        Useful when constraints would invalidate many cartesian combinations.
        """
        norm_space = { _to_param(k): list(v) for k, v in space.items() }
        return self._append(_ConstrainedProductOp(norm_space, ok, self._conflict))

    # ——— iteration / utilities ———
    def __iter__(self) -> Iterator[SubstitutionMap]:
        upstream: Iterable[SubstitutionMap] = ()  # ignored by _SeedOp
        for op in self._ops:
            upstream = op.apply(upstream)
        yield from upstream

    def variants(self) -> Iterator[SubstitutionMap]:
        """Alias for iter(self)."""
        return iter(self)

    def to_list(self, limit: int | None = None) -> list[SubstitutionMap]:
        out: list[SubstitutionMap] = []
        for i, subs in enumerate(self):
            out.append(subs)
            if limit is not None and i + 1 >= limit:
                break
        return out

    def estimated_cardinality(self) -> int | None:
        """Multiply known factors; returns None if any factor is unknown.

        This is only an estimate — filters, derives, or constrained ops can
        change the actual count.
        """
        total = 1
        for op in self._ops:
            f = op.cardinality_factor()
            if f is None:
                return None
            total *= f
        return total

    def exact_cardinality(self, limit: int | None = None) -> int:
        """Count by iterating. If *limit* is provided, stop once reached.
        Returns the number of yielded variants (≤ limit when given)."""
        count = 0
        for count, _ in enumerate(self, start=1):
            if limit is not None and count >= limit:
                break
        return count

    def to_dataframe(self): # type: ignore
        """Materialize variants into a pandas DataFrame.
        Requires pandas; imported lazily to avoid a hard dependency.
        """
        try:
            import pandas as pd
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                "pandas is required for to_dataframe(); pip install pandas"
            ) from e
        rows = [dict(m) for m in self]
        return pd.DataFrame.from_records(rows)

    # TODO(zac): Define TaskLattice version string somewhere
    def iter_with_ids(self, *, salt: str = "tasklattice-v1") -> Iterator[tuple[str, SubstitutionMap]]:
        """Yield `(variant_id, mapping)` where the id is a stable hash of the
        canonicalized mapping plus a *salt* (to allow versioning).
        Keys/values are canonicalized via sorted items and JSON with repr fallback.
        """
        for m in self:
            # Canonicalize to a JSON string; fall back to repr for non-JSONables.
            try:
                items = [(repr(k), m[k]) for k in sorted(m.keys(), key=repr)]
                payload = json.dumps(items, default=repr, separators=(",", ":"))
            except Exception:
                payload = repr(tuple(sorted((repr(k), repr(v)) for k, v in m.items())))
            h = hashlib.blake2b(digest_size=16)
            h.update(salt.encode("utf-8"))
            h.update(payload.encode("utf-8"))
            yield (h.hexdigest(), m)

    # ——— configuration knobs ———
    def with_conflict_policy(self, policy: ConflictPolicy) -> Lattice:
        """Return a copy with a different merge policy for subsequent ops."""
        return Lattice._from_ops(self._ops, conflict=policy)

    # ——— introspection ———
    def explain(self, *, file: IO[str] | None = None) -> None:
        """Print a human-readable summary of the pipeline and its ops."""
        out = file or sys.stdout
        print("Lattice pipeline:", file=out)
        est = 1
        unknown = False
        for i, op in enumerate(self._ops):
            f = op.cardinality_factor()
            if f is None:
                unknown = True
            else:
                est *= f
            cf = "?" if f is None else str(f)
            print(f"  [{i:02d}] {op.brief()}  ×{cf}  {op.details()}", file=out)
        total = "?" if unknown else str(est)
        print(f"Estimated cardinality: {total}", file=out)

