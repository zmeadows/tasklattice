"""
tasklattice.runners.local
=========================


LocalRunner executes a RunMaterialized via a subprocess on the current host.

Design recap
------------
- Construct with `launch=...` (LaunchSpec | factory | "str cmd" | ["argv"]).
- submit(run):
    * computes effective LaunchSpec (cwd relative/None → resolved under run_dir),
    * validates (common + backend),
    * writes `_tl/run.json` (queued → running),
    * EITHER spawns immediately (if capacity allows) OR enqueues,
    * returns a RunHandle that may be in QUEUED state.
- Single monitor thread:
    * polls all active runs,
    * dispatches queued runs when capacity frees up,
    * enforces Resources.time_limit_s (TERM then KILL with grace),
    * writes timeout events and final terminal state to `_tl/run.json`,
    * removes finished runs and cleans per-run locks.

All run.json writes after spawn are serialized with a per-run Lock to avoid races.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
import warnings
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from tasklattice.constants import (
    RUNSTATE_SCHEMA,
    default_stderr_path,
    default_stdout_path,
    meta_dir,
    runstate_path,
)
from tasklattice.platform import platform
from tasklattice.run.materialize import RunMaterialized
from tasklattice.run.state import (
    TERMINAL_STATES,
    RunStatus,
    append_runstate_event,
    read_runstate,
    spec_to_jsonable,
    update_runstate,
    write_runstate,
)
from tasklattice.runners.base import (
    LaunchSpec,
    LaunchSpecFactory,
    RunHandle,
    Runner,
    UserLaunchInput,
    ensure_launch_factory,
    validate_spec_common,
)
from tasklattice.utils.fs_utils import ensure_parent_dirs
from tasklattice.utils.json_utils import json_load
from tasklattice.utils.time_utils import now_iso

# -----------------------------------------------------------------------------
# Small utilities (json i/o, timestamps)
# -----------------------------------------------------------------------------


def _as_status(x: Any) -> RunStatus | None:
    if isinstance(x, RunStatus):
        return x
    if isinstance(x, str):
        try:
            return RunStatus(x)
        except ValueError:
            return None
    return None


def _resolve_max_parallel(setting: int | Literal["auto", "unbounded"]) -> int | None:
    if setting == "auto":
        n = os.cpu_count() or 1
        return max(1, n - 1)  # leave a core free
    elif setting == "unbounded":
        return None  # no cap
    elif setting <= 0:
        raise ValueError("max_parallel must be > 0, or 'auto'/'unbounded'")

    return setting


_DEFAULT_GRACE_PERIOD = 5.0


def _terminate_with_grace(
    proc_or_pid: int | subprocess.Popen[bytes],
    *,
    grace_s: float | None = _DEFAULT_GRACE_PERIOD,
    force: bool = False,
) -> None:
    # If we only have a PID, avoid signaling unless we can prove it's alive now.
    if isinstance(proc_or_pid, int):
        alive = platform.pid_alive(proc_or_pid)
        if alive is not True:
            return  # Unknown or already dead → don't risk killing a reused PID.

    # Soft first
    platform.terminate_tree_by(proc_or_pid, mode="soft")

    if grace_s is None:
        grace_s = _DEFAULT_GRACE_PERIOD

    # Wait for exit (prefer handle-based wait when possible)
    if grace_s > 0:
        if isinstance(proc_or_pid, subprocess.Popen):
            try:
                proc_or_pid.wait(timeout=grace_s)
                return
            except Exception:
                pass
        else:
            pid = int(proc_or_pid)
            deadline = time.monotonic() + grace_s
            while time.monotonic() < deadline:
                alive = platform.pid_alive(pid)
                if alive is False:
                    return
                time.sleep(0.1)

    # Hard if still around or caller insists
    still_alive = (
        (proc_or_pid.poll() is None)
        if isinstance(proc_or_pid, subprocess.Popen)
        else (platform.pid_alive(int(proc_or_pid)) is True)
    )

    if force or still_alive:
        platform.terminate_tree_by(proc_or_pid, mode="hard")


# -----------------------------------------------------------------------------
# RunHandle impl (monitor updates the metadata; handle supports QUEUED state)
# -----------------------------------------------------------------------------


@dataclass
class _LocalRunHandle(RunHandle):
    _runner: LocalRunner
    _run_id: str
    _run_dir: Path
    _proc: subprocess.Popen[bytes] | None = None
    _stdout: Path | None = None
    _stderr: Path | None = None
    _cancel_requested: bool = False
    _timed_out: bool = False
    _started_evt: threading.Event = field(default_factory=threading.Event)
    _finished_evt: threading.Event = field(default_factory=threading.Event)

    def run_id(self) -> str:
        return self._run_id

    def external_id(self) -> str | None:
        if self._proc is not None:
            return str(self._proc.pid)
        doc = read_runstate(self._run_dir)
        eid = doc.get("external_id")
        return str(eid) if eid is not None else None

    def status(self) -> RunStatus:
        # Live handle path
        if self._proc is not None:
            rc = self._proc.poll()
            if rc is None:
                return RunStatus.RUNNING
            if self._timed_out:
                return RunStatus.TIMED_OUT
            if self._cancel_requested:
                return RunStatus.CANCELLED
            return RunStatus.SUCCEEDED if rc == 0 else RunStatus.FAILED

        # Passive path: read run.json
        doc = read_runstate(self._run_dir)
        if not doc:
            return RunStatus.CANCELLED if self._cancel_requested else RunStatus.FAILED
        state = _as_status(doc.get("state"))

        if state == RunStatus.RUNNING:
            pid_val = doc.get("external_id")
            try:
                pid = int(pid_val) if pid_val is not None else None
            except Exception:
                pid = None
            if pid is not None and platform.pid_alive(pid) is False:
                self._runner._finalize_unknown_exit(
                    self._run_dir, state=RunStatus.FAILED, reason="pid_not_found"
                )
                doc = read_runstate(self._run_dir) or {"state": RunStatus.FAILED}
                state = _as_status(doc.get("state")) or RunStatus.FAILED

        try:
            return cast(RunStatus, state)
        except Exception:
            return RunStatus.FAILED

    def wait(self, timeout_s: float | None = None) -> RunStatus:
        # Live handle: rely on monitor events set by runner.
        if self._proc is not None:
            if timeout_s is None:
                self._finished_evt.wait()
                return self.status()
            else:
                if not self._finished_evt.wait(timeout_s):
                    return self.status()
                return self.status()

        # Passive/attached: poll run.json until terminal or timeout.
        start = time.monotonic()
        term = {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT}
        while True:
            st = self.status()
            if st in term:
                self._finished_evt.set()
                return st
            if timeout_s is not None and (time.monotonic() - start) >= timeout_s:
                return st
            time.sleep(0.2)

    def cancel(
        self, force: bool = False, grace_s: float | None = None, reason: str | None = None
    ) -> None:
        """
        Best-effort cancellation.
        - Queued: remove from runner queue and mark CANCELLED.
        - Running (live): signal via runner under lock.
        - Running (attached): kill by PID/PGID and finalize.
        """
        _ = reason  # TODO: plumb into events
        self._cancel_requested = True

        if self._proc is None:
            doc = read_runstate(self._run_dir)
            state = _as_status(doc.get("state"))
            if state == RunStatus.RUNNING:
                self._runner._cancel_attached(
                    self._run_dir, handle=self, grace_s=grace_s, force=force
                )
                return
            # queued or unknown
            self._runner._cancel_queued(self._run_dir, handle=self)
            return

        # Live running
        self._runner._cancel_running(self._run_dir, handle=self, grace_s=grace_s, force=force)

    def return_code(self) -> int | None:
        return None if self._proc is None else self._proc.returncode

    def stdout_path(self) -> Path | None:
        return self._stdout

    def stderr_path(self) -> Path | None:
        return self._stderr


@dataclass(slots=True)
class _RunRecordCommon:
    run_id: str
    run_dir: Path
    handle: _LocalRunHandle
    stdout_path: Path
    stderr_path: Path
    lock: threading.Lock  # serialize run.json access for THIS run


@dataclass(slots=True)
class _ActiveRunRecord(_RunRecordCommon):
    """Internal record the monitor uses to manage a single active run."""

    deadline_monotonic: float | None  # None => no timeout


@dataclass(slots=True)
class _PendingRunRecord(_RunRecordCommon):
    """A run that is materialized, validated, and queued but not yet started."""

    spec: LaunchSpec


class LocalRunner(Runner):
    """
    Execute a RunMaterialized via a local subprocess, with optional concurrency cap.

    Args:
        launch: UserLaunchInput for how to start each run.
        max_parallel: "auto" (default) caps to max(1, cpu_count-1), "unbounded" removes the cap,
                      or a positive integer for a fixed cap.
        name: Runner name for metadata.
    """

    name: str

    def __init__(
        self,
        launch: UserLaunchInput,
        *,
        max_parallel: int | Literal["auto", "unbounded"] = "auto",
        name: str = "local",
    ) -> None:
        self.name = name
        self._launch_factory: LaunchSpecFactory = ensure_launch_factory(launch)

        # Concurrency limit
        self._max_parallel = _resolve_max_parallel(max_parallel)

        # Active run registry (run_dir -> record)
        self._active: dict[Path, _ActiveRunRecord] = {}
        # Pending FIFO queue (run_dir order preserved)
        self._pending: deque[_PendingRunRecord] = deque()
        # Single runner-level lock for both active and pending (keeps ordering stable)
        self._active_lock = threading.Lock()

        # Condition to wake the monitor when queue/active set changes
        self._cond = threading.Condition(self._active_lock)

        # Per-run locks (by run_dir) for run.json
        self._locks: dict[Path, threading.Lock] = {}
        self._locks_lock = threading.Lock()

        # Single monitor thread
        self._stop_event = threading.Event()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name=f"LocalRunnerMonitor[{self.name}]",
            daemon=True,
        )
        self._monitor_thread.start()

    # ---- public Runner API ---------------------------------------------------

    def effective_spec(self, run: RunMaterialized) -> LaunchSpec:
        return self._launch_factory(run)

    def validate_spec(self, spec: LaunchSpec, *, run_dir: Path) -> None:
        """
        Backend-specific checks for LocalRunner, after defaults/normalization.
        """
        # Warn: GPUs are ignored locally (no enforcement)
        if spec.resources.gpus:
            warnings.warn("LocalRunner ignores `resources.gpus`; continuing anyway.", stacklevel=2)

        # Check cmd[0] plausibility against resolved cwd
        cwd_abs = run_dir if spec.cwd is None else (run_dir / spec.cwd)
        cmd0 = spec.cmd[0]
        p = Path(cmd0)
        if p.is_absolute() and p.exists():
            return
        if (cwd_abs / cmd0).exists():
            return
        if shutil.which(cmd0):
            return
        warnings.warn(
            f"Executable '{cmd0}' not found (cwd={cwd_abs}); process may fail to start.",
            stacklevel=2,
        )

    def submit(self, run: RunMaterialized) -> RunHandle:
        """
        Create (or enqueue) a run, write 'queued' state, maybe spawn immediately,
        and return a handle (which may be queued or running).
        """
        run_dir: Path = run.run_dir.path
        run_id = str(getattr(run, "run_id", None) or run_dir.name)

        # Compute effective spec and normalize defaults.
        base_spec = self.effective_spec(run)

        # Enforce cwd policy (spec.cwd is None or RELATIVE). Resolve absolute later at spawn.
        stdout_p = base_spec.stdout_path or default_stdout_path(run_dir)
        stderr_p = base_spec.stderr_path or default_stderr_path(run_dir)
        meta_dir(run_dir).mkdir(parents=True, exist_ok=True)

        # Per-run lock (shared with monitor)
        run_lock = self._get_run_lock(run_dir)

        # Determine attempt & write "queued" atomically
        with run_lock:
            attempt = 1
            prior = read_runstate(run_dir)
            if prior and isinstance(prior.get("attempt"), int):
                attempt = prior["attempt"] + 1

            # Truncate logs fresh each submission
            ensure_parent_dirs(stdout_p)
            ensure_parent_dirs(stderr_p)
            open(stdout_p, "wb").close()
            open(stderr_p, "wb").close()

            # Build effective spec with normalized paths kept relative in spec.cwd
            effective_spec = LaunchSpec(
                cmd=list(base_spec.cmd),
                env=base_spec.env,
                cwd=(base_spec.cwd if base_spec.cwd is None else Path(base_spec.cwd)),
                stdout_path=stdout_p,
                stderr_path=stderr_p,
                resources=base_spec.resources,
                backend_opts=base_spec.backend_opts,
            )

            # Validate (common + backend)
            validate_spec_common(effective_spec, run_dir=run_dir)
            self.validate_spec(effective_spec, run_dir=run_dir)

            # Record queued state
            payload = {
                "schema": RUNSTATE_SCHEMA,
                "runner": self.name,
                "run_id": run_id,
                "attempt": attempt,
                "state": RunStatus.QUEUED,
                "submitted_at": now_iso(),
                "started_at": None,
                "finished_at": None,
                "external_id": None,
                "return_code": None,
                "launch_spec": spec_to_jsonable(effective_spec, run_dir=run_dir),
                "events": [],
            }
            write_runstate(run_dir, payload)
            append_runstate_event(run_dir, state=RunStatus.QUEUED, reason="submit")

        # Construct handle
        handle = _LocalRunHandle(self, run_id, run_dir, None, stdout_p, stderr_p)

        # Try to start immediately if capacity allows; otherwise enqueue.
        pending = _PendingRunRecord(
            run_id=run_id,
            run_dir=run_dir,
            spec=effective_spec,
            stdout_path=stdout_p,
            stderr_path=stderr_p,
            lock=run_lock,
            handle=handle,
        )

        with self._active_lock:
            if self._has_capacity_locked():
                # spawn now (inside the same lock to keep capacity consistent)
                try:
                    self._spawn_from_pending_locked(pending)
                except Exception as exc:
                    # Mark FAILED with finished_at and a spawn event, then re-raise.
                    with run_lock:
                        update_runstate(
                            run_dir,
                            {
                                "state": RunStatus.FAILED,
                                "finished_at": now_iso(),
                                "return_code": None,
                            },
                        )

                        append_runstate_event(
                            run_dir,
                            state=RunStatus.FAILED,
                            reason=f"spawn failed: {exc!s}",
                        )
                    raise
            else:
                # No capacity → enqueue
                self._pending.append(pending)
            # Wake monitor to reconsider capacity/queue
            self._cond.notify_all()

        return handle

    def attach(self, run: RunMaterialized) -> RunHandle | None:
        """Attach to an existing run directory.

        Returns a passive handle (no Popen). If the run is terminal, the handle's
        finished event is set. If the run is RUNNING but the PID is missing and
        we're on POSIX, auto-finalize to FAILED (pid_not_found).
        returns None for QUEUED.
        """
        run_dir: Path = run.run_dir.path
        doc = json_load(runstate_path(run_dir))
        if not doc:
            return None

        # Only attach if this run belongs to this runner name.
        runner_name = doc.get("runner")
        if runner_name and runner_name != self.name:
            return None

        # Derive stdout/stderr paths (from launch_spec or defaults)
        ls = doc.get("launch_spec") or {}
        try:
            raw_stdout = ls.get("stdout_path")
            stdout_p = (
                Path(raw_stdout) if isinstance(raw_stdout, str) else default_stdout_path(run_dir)
            )
        except Exception:
            stdout_p = default_stdout_path(run_dir)
        try:
            raw_stderr = ls.get("stderr_path")
            stderr_p = (
                Path(raw_stderr) if isinstance(raw_stderr, str) else default_stderr_path(run_dir)
            )
        except Exception:
            stderr_p = default_stderr_path(run_dir)

        run_id = str(doc.get("run_id") or run_dir.name)
        handle = _LocalRunHandle(self, run_id, run_dir, None, stdout_p, stderr_p)

        state = _as_status(doc.get("state"))
        if state in {
            RunStatus.SUCCEEDED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.TIMED_OUT,
        }:
            handle._started_evt.set()
            handle._finished_evt.set()
            return handle

        if state == RunStatus.RUNNING:
            pid_val = doc.get("external_id")
            try:
                pid = int(pid_val) if pid_val is not None else None
            except Exception:
                pid = None
            if pid is not None and platform.pid_alive(pid) is False:
                self._finalize_unknown_exit(run_dir, state=RunStatus.FAILED, reason="pid_not_found")
                handle._started_evt.set()
                handle._finished_evt.set()
                return handle
            # Otherwise, passive RUNNING.
            handle._started_evt.set()
            return handle

        if state == RunStatus.QUEUED:
            # Can't reconstruct a queued in-memory item → refuse attach.
            return None

        # Unknown state → refuse
        return None

    def close(self) -> None:
        """Stop the monitor thread. We don't mutate per-run state here."""
        self._stop_event.set()
        # Wake the monitor so it can exit promptly
        with self._active_lock:
            self._cond.notify_all()
        self._monitor_thread.join(timeout=2.0)

    # ---- helpers: PID liveness / termination / finalize ----------------------

    def _finalize_unknown_exit(self, run_dir: Path, *, state: str, reason: str) -> None:
        """Idempotently flip a non-terminal run.json into a terminal state.

        Used when we detect a stale RUNNING state but the PID is gone, or after
        a PID-based termination where we cannot retrieve a return code.
        """

        with self._get_run_lock(run_dir):
            doc = read_runstate(run_dir)

            cur = _as_status(doc.get("state"))
            if cur in TERMINAL_STATES:
                return

            doc["state"] = state
            doc["finished_at"] = now_iso()
            # keep existing return_code if set; else None
            if "return_code" not in doc:
                doc["return_code"] = None
            # advisory flags
            doc["finalized_by_attach"] = True
            doc["reason"] = reason

            write_runstate(run_dir, doc)
            append_runstate_event(run_dir, state=state, reason=reason)

    def _cancel_attached(
        self, run_dir: Path, *, handle: _LocalRunHandle, grace_s: float | None = None, force: bool
    ) -> None:
        """Cancel a RUNNING attached run via PID/PGID and finalize to CANCELLED."""
        lock = self._get_run_lock(run_dir)
        with lock:
            doc = json_load(runstate_path(run_dir)) or {}

            state = _as_status(doc.get("state"))
            if state in TERMINAL_STATES:
                return

            if state != RunStatus.RUNNING:
                # Not running; treat as queued or unknown → mark cancelled.
                self._finalize_unknown_exit(
                    run_dir, state=RunStatus.CANCELLED, reason="user_cancel_nonrunning"
                )
                handle._finished_evt.set()
                return

            # RUNNING
            pid_val = doc.get("external_id")
            try:
                pid = int(pid_val) if pid_val is not None else None
            except Exception:
                pid = None

        if pid is not None:
            _terminate_with_grace(pid, grace_s=grace_s, force=force)

        # Regardless of platform/liveness certainty, mark CANCELLED.
        self._finalize_unknown_exit(run_dir, state=RunStatus.CANCELLED, reason="user_cancel")
        handle._finished_evt.set()

    # ---- internal: cancellation of runs ------------------------------

    def _cancel_queued(self, run_dir: Path, *, handle: _LocalRunHandle) -> None:
        # TODO: since handle stores run_dir, do we need both run_dir and handle args?
        with self._active_lock:
            # Remove from pending if present
            idx = None
            for i, item in enumerate(self._pending):
                if item.run_dir == run_dir:
                    idx = i
                    break

            if idx is None:
                # It might have just started; nothing to do here.
                # The running cancel path will handle it.
                return

            record = self._pending[idx]
            del self._pending[idx]

        # Mark cancelled in run.json
        with record.lock:
            update_runstate(
                run_dir,
                {"state": RunStatus.CANCELLED, "finished_at": now_iso(), "return_code": None},
            )

            append_runstate_event(
                run_dir, state=RunStatus.CANCELLED, reason="cancelled while queued"
            )

        handle._finished_evt.set()

        # Drop the per-run lock since the run was never started
        with self._locks_lock:
            self._locks.pop(run_dir, None)

        # Wake monitor in case it was waiting for capacity/queue changes
        with self._active_lock:
            self._cond.notify_all()

    def _cancel_running(
        self,
        run_dir: Path,
        *,
        handle: _LocalRunHandle,
        grace_s: float | None = None,
        force: bool = False,
    ) -> None:
        """Best-effort cancellation for a running process under the runner lock."""
        with self._active_lock:
            rec = self._active.get(run_dir)
            if rec is None or rec.handle._proc is None:
                # No longer running (finished or race); nothing to do.
                return
            handle._cancel_requested = True
            proc = rec.handle._proc
            try:
                _terminate_with_grace(proc, grace_s=grace_s, force=force)
            finally:
                # Wake the monitor so it can notice state changes promptly.
                self._cond.notify_all()

    # ---- internal: lock registry --------------------------------------------

    def _get_run_lock(self, run_dir: Path) -> threading.Lock:
        """
        Return a stable per-run lock. Resubmits and the monitor use the same lock
        to serialize run.json updates.
        """
        with self._locks_lock:
            lock = self._locks.get(run_dir)
            if lock is None:
                lock = threading.Lock()
                self._locks[run_dir] = lock
            return lock

    def _has_capacity_locked(self) -> bool:
        if self._max_parallel is None:
            return True  # unbounded
        return len(self._active) < self._max_parallel

    # ---- internal: monitor thread -------------------------------------------

    def _monitor_loop(self) -> None:
        """
        Poll all active runs, enforce timeouts, finalize metadata when processes exit,
        and dispatch queued runs when capacity frees up. All writes to run.json after
        spawn happen here under each run's lock to avoid races.
        """
        while not self._stop_event.is_set():
            # 1) Finalize finished runs and enforce timeouts
            with self._active_lock:
                items = list(self._active.items())

            now = time.monotonic()
            to_remove: list[Path] = []

            for run_dir, rec in items:
                with rec.lock:
                    proc = rec.handle._proc
                    if proc is None:
                        # shouldn't happen for active records
                        continue

                    # Enforce wall-clock timeout
                    if (
                        rec.deadline_monotonic is not None
                        and proc.poll() is None
                        and now >= rec.deadline_monotonic
                    ):
                        rec.handle._timed_out = True
                        _terminate_with_grace(proc)
                        append_runstate_event(run_dir, state=RunStatus.TIMED_OUT, reason="timeout")
                        rec.deadline_monotonic = None  # prevent repeated signaling

                    # Finalization on process exit
                    rc = proc.poll()
                    if rc is not None:
                        finished_at = now_iso()
                        if rec.handle._timed_out:
                            final_state = RunStatus.TIMED_OUT
                        elif rec.handle._cancel_requested:
                            final_state = RunStatus.CANCELLED
                        else:
                            final_state = RunStatus.SUCCEEDED if rc == 0 else RunStatus.FAILED

                        update_runstate(
                            run_dir,
                            {
                                "state": final_state,
                                "finished_at": finished_at,
                                "return_code": rc,
                            },
                        )

                        append_runstate_event(run_dir, state=final_state, reason="process exited")

                        rec.handle._finished_evt.set()
                        to_remove.append(run_dir)

            if to_remove:
                with self._active_lock:
                    for rd in to_remove:
                        self._active.pop(rd, None)
                # Clean up locks for finalized runs to avoid unbounded growth
                with self._locks_lock:
                    for rd in to_remove:
                        self._locks.pop(rd, None)

            # 2) Dispatch from pending queue if capacity allows
            with self._active_lock:
                while self._has_capacity_locked() and self._pending:
                    item = self._pending.popleft()
                    try:
                        self._spawn_from_pending_locked(item)
                    except Exception as exc:
                        # Mark FAILED and continue to next pending
                        with item.lock:
                            update_runstate(
                                item.run_dir,
                                {
                                    "state": RunStatus.FAILED,
                                    "finished_at": now_iso(),
                                    "return_code": None,
                                },
                            )

                            append_runstate_event(
                                item.run_dir,
                                state=RunStatus.FAILED,
                                reason=f"spawn failed: {exc!s}",
                            )

                            item.handle._finished_evt.set()
                            # don't re-raise from monitor loop

            # 3) Sleep until something changes or a deadline approaches.
            # If nothing is active or pending, wait indefinitely for a notification.
            with self._cond:
                if self._stop_event.is_set():
                    return

                # Compute a reasonable timeout: next deadline or a small poll window
                timeout = None

                # The condition uses the same lock; we already hold it while waiting.
                if self._active or self._pending:
                    # Maximum polling interval to observe exits promptly
                    max_poll = 0.5
                    timeout = max_poll

                    # If there are deadlines, wake sooner
                    now2 = time.monotonic()
                    next_deadline = None

                    for rec2 in self._active.values():
                        if rec2.deadline_monotonic is not None:
                            if next_deadline is None or rec2.deadline_monotonic < next_deadline:
                                next_deadline = rec2.deadline_monotonic

                    if next_deadline is not None:
                        delta = max(0.0, next_deadline - now2)
                        timeout = min(timeout, delta)

                self._cond.wait(timeout=timeout)

    def _spawn_from_pending_locked(self, record: _PendingRunRecord) -> None:
        """
        Spawn a queued run NOW (caller must hold _active_lock). On success,
        writes 'running', registers into _active, and updates the handle.
        On failure, raises; caller is responsible for marking FAILED state.
        """
        run_dir = record.run_dir
        spec = record.spec
        run_lock = record.lock

        # Compute absolute cwd under run_dir
        cwd_abs = run_dir if spec.cwd is None else (run_dir / spec.cwd)

        # Prepare environment
        env = os.environ.copy()
        if spec.env:
            env.update(spec.env)

        popen_kwargs: dict[str, Any] = {}
        platform.configure_popen_group(popen_kwargs)

        proc = subprocess.Popen(
            list(spec.cmd),
            cwd=str(cwd_abs),
            env=env,
            stdout=open(record.stdout_path, "ab", buffering=0),
            stderr=open(record.stderr_path, "ab", buffering=0),
            text=False,
            encoding=None,
            errors=None,
            **popen_kwargs,
        )
        proc = cast(subprocess.Popen[bytes], proc)

        # Mark running
        with run_lock:
            update_runstate(
                run_dir,
                {"state": RunStatus.RUNNING, "started_at": now_iso(), "external_id": str(proc.pid)},
            )

            append_runstate_event(
                run_dir, state=RunStatus.RUNNING, reason=f"spawned pid {proc.pid}"
            )

        # Update handle & register
        record.handle._proc = proc
        record.handle._stdout = record.stdout_path
        record.handle._stderr = record.stderr_path
        record.handle._started_evt.set()

        deadline: float | None = None
        tl = spec.resources.time_limit_s
        if tl is not None and tl > 0:
            deadline = time.monotonic() + float(tl)

        rec = _ActiveRunRecord(
            run_id=record.run_id,
            run_dir=run_dir,
            handle=record.handle,
            stdout_path=record.stdout_path,
            stderr_path=record.stderr_path,
            deadline_monotonic=deadline,
            lock=run_lock,
        )
        self._active[run_dir] = rec
