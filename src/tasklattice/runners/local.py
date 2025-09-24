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

import json
import os
import shutil
import signal
import subprocess
import threading
import time
import warnings
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Literal, Optional, cast

from tasklattice.constants import (
    RUNSTATE_SCHEMA,
    meta_dir,
    runstate_path,
)
from tasklattice.constants import (
    stderr_path as default_stderr_path,
)
from tasklattice.constants import (
    stdout_path as default_stdout_path,
)
from tasklattice.materialize import RunMaterialized
from tasklattice.runners.base import (
    LaunchSpec,
    LaunchSpecFactory,
    RunHandle,
    Runner,
    RunStatus,
    TERMINAL_STATES,
    UserLaunchInput,
    ensure_launch_factory,
    validate_spec_common,
)

# -----------------------------------------------------------------------------
# Small utilities (json i/o, timestamps)
# -----------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ensure_parent_dirs(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def _json_load(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as f:
            data: Any = json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        # Corrupt or unreadable; treat as missing.
        return None

    if isinstance(data, dict):
        # JSON object keys are strings per spec; cast is safe here.
        return cast(dict[str, Any], data)

    # If it wasn't a JSON object (e.g., list/str), ignore it.
    return None

def _json_atomic_write(path: Path, payload: dict[str, Any]) -> None:
    _ensure_parent_dirs(path)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)

def _spec_to_jsonable(spec: LaunchSpec, *, run_dir: Path) -> dict[str, Any]:
    """
    JSON-friendly view of the effective LaunchSpec for provenance.
    We keep cwd relative if provided, otherwise we render run_dir (absolute).
    """
    return {
        "cmd": list(spec.cmd),
        "env": dict(spec.env) if spec.env is not None else None,
        "cwd": str(spec.cwd) if spec.cwd is not None else str(run_dir),
        "stdout_path": str(spec.stdout_path) if spec.stdout_path else str(default_stdout_path(run_dir)),
        "stderr_path": str(spec.stderr_path) if spec.stderr_path else str(default_stderr_path(run_dir)),
        "resources": {
            "cpus": spec.resources.cpus,
            "gpus": spec.resources.gpus,
            "mem_mb": spec.resources.mem_mb,
            "time_limit_s": spec.resources.time_limit_s,
        },
        "backend_opts": dict(spec.backend_opts),
    }

def _append_event(run_dir: Path, lock: threading.Lock, *, state: str, reason: str) -> None:
    """
    Single path to append an event to run.json. If/when we add trimming, do it here.
    """
    with lock:
        path = runstate_path(run_dir)
        doc = _json_load(path) or {}
        ev = list(doc.get("events", []))
        ev.append({"timestamp": _now_iso(), "state": state, "reason": reason})
        # TODO: If we ever want to trim: ev = ev[-MAX_EVENTS:]
        doc["events"] = ev
        _json_atomic_write(path, doc)

def _resolve_max_parallel(setting: int | Literal["auto", "unbounded"]) -> int | None:
    if setting == "auto":
        n = os.cpu_count() or 1
        return max(1, n - 1)  # leave a core free
    elif setting == "unbounded":
        return None  # no cap
    elif setting <= 0:
        raise ValueError("max_parallel must be > 0, or 'auto'/'unbounded'")

    return setting


# -----------------------------------------------------------------------------
# RunHandle impl (monitor updates the metadata; handle supports QUEUED state)
# -----------------------------------------------------------------------------

@dataclass
class _LocalRunHandle(RunHandle):
    _runner: LocalRunner
    _run_id: str
    _run_dir: Path
    _proc: Optional[subprocess.Popen[bytes]] = None
    _stdout: Path | None = None
    _stderr: Path | None = None
    _cancel_requested: bool = False
    _timed_out: bool = False
    _started_evt: threading.Event = field(default_factory=threading.Event)
    _finished_evt: threading.Event = field(default_factory=threading.Event)

    def run_id(self) -> str: return self._run_id
    def external_id(self) -> str | None:
        return None if self._proc is None else str(self._proc.pid)

    def status(self) -> RunStatus:
        # If we have a live subprocess, prefer that as the source of truth.
        if self._proc is not None:
            rc = self._proc.poll()
            if rc is None:
                return RunStatus.RUNNING
            if self._timed_out:
                return RunStatus.TIMED_OUT
            if self._cancel_requested:
                return RunStatus.CANCELLED
            return RunStatus.SUCCEEDED if rc == 0 else RunStatus.FAILED

        # Passive/attached handle fallback: read run.json if available.
        try:
            path = runstate_path(self._run_dir)
            doc = _json_load(path) or {}
            state_raw = doc.get("state")
            if state_raw is None:
                # Unknown → assume queued unless a cancel was requested.
                return RunStatus.CANCELLED if self._cancel_requested else RunStatus.QUEUED
            return RunStatus(state_raw)
        except Exception:
            # On any error, be conservative.
            return RunStatus.CANCELLED if self._cancel_requested else RunStatus.QUEUED

    def wait(self, timeout_s: float | None = None) -> RunStatus:
        # If we're managing a subprocess, we can use the event (set by the monitor)
        # which will be triggered when the process exits or the run transitions terminal.
        if self._proc is not None:
            if timeout_s is None:
                self._finished_evt.wait()
            else:
                if not self._finished_evt.wait(timeout_s):
                    return self.status()
            return self.status()

        # Passive/attached handle: poll run.json until we observe a terminal state.
        # Keep this simple and conservative (no background threads).
        end = None if timeout_s is None else (time.monotonic() + max(0.0, float(timeout_s)))
        while True:
            st = self.status()
            if st in TERMINAL_STATES:
                return st
            if end is not None and time.monotonic() >= end:
                return st
            time.sleep(0.2)

    def cancel(self, force: bool = False, reason: str | None = None) -> None:
        """
        Best-effort cancellation for queued or running runs.
        - If queued: remove from runner queue and mark CANCELLED.
        - If running: signal process group (POSIX) or process (Windows) via runner under lock.
        """

        # TODO: propagate the reason to the run.json events list
        _ = reason

        self._cancel_requested = True
        if self._proc is None:
            # queued → ask runner to cancel from queue
            self._runner._cancel_queued(self._run_dir, handle=self)
            return

        # running → ask runner to cancel under its lock
        self._runner._cancel_running(self._run_dir, handle=self, force=force)

    def return_code(self) -> int | None:
        return None if self._proc is None else self._proc.returncode

    def stdout_path(self) -> Path | None: return self._stdout
    def stderr_path(self) -> Path | None: return self._stderr


# -----------------------------------------------------------------------------
# LocalRunner monitor, records, and queue
# -----------------------------------------------------------------------------

@dataclass
class _RunRecord:
    """Internal record the monitor uses to manage a single active run."""
    run_id: str
    run_dir: Path
    handle: _LocalRunHandle
    stdout_path: Path
    stderr_path: Path
    lock: threading.Lock                 # serialize run.json access for THIS run
    deadline_monotonic: Optional[float]  # None => no timeout

@dataclass
class _PendingItem:
    """A run that is materialized, validated, and queued but not yet started."""
    run_id: str
    run_dir: Path
    handle: _LocalRunHandle
    stdout_path: Path
    stderr_path: Path
    lock: threading.Lock
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
        self._active: dict[Path, _RunRecord] = {}
        # Pending FIFO queue (run_dir order preserved)
        self._pending: Deque[_PendingItem] = deque()
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
        lock = self._get_run_lock(run_dir)

        # Determine attempt & write "queued" atomically
        with lock:
            attempt = 1
            prior = _json_load(runstate_path(run_dir))
            if prior and isinstance(prior.get("attempt"), int):
                attempt = prior["attempt"] + 1

            # Truncate logs fresh each submission
            _ensure_parent_dirs(stdout_p)
            _ensure_parent_dirs(stderr_p)
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
            queued_at = _now_iso()
            state_path = runstate_path(run_dir)
            payload = {
                "schema": RUNSTATE_SCHEMA,
                "runner": self.name,
                "run_id": run_id,
                "attempt": attempt,
                "state": RunStatus.QUEUED,
                "submitted_at": queued_at,
                "started_at": None,
                "finished_at": None,
                "external_id": None,
                "return_code": None,
                "launch_spec": _spec_to_jsonable(effective_spec, run_dir=run_dir),
                "events": [],
            }
            _json_atomic_write(state_path, payload)
            _append_event(run_dir, lock, state=RunStatus.QUEUED, reason="submit")

        # Construct handle
        handle = _LocalRunHandle(self, run_id, run_dir, None, stdout_p, stderr_p)

        # Try to start immediately if capacity allows; otherwise enqueue.
        pending = _PendingItem(
            run_id=run_id,
            run_dir=run_dir,
            spec=effective_spec,
            stdout_path=stdout_p,
            stderr_path=stderr_p,
            lock=lock,
            handle=handle,
        )

        with self._active_lock:
            if self._has_capacity_locked():
                # spawn now (inside the same lock to keep capacity consistent)
                try:
                    self._spawn_from_pending_locked(pending)
                except Exception as exc:
                    # Mark FAILED with finished_at and a spawn event, then re-raise.
                    with lock:
                        path = runstate_path(run_dir)
                        doc = _json_load(path) or {}
                        doc["state"] = RunStatus.FAILED
                        doc["finished_at"] = _now_iso()
                        doc["return_code"] = None
                        _json_atomic_write(path, doc)
                        _append_event(run_dir, lock, state=RunStatus.FAILED, reason=f"spawn failed: {exc!s}")
                    raise
            else:
                # No capacity → enqueue
                self._pending.append(pending)
            # Wake monitor to reconsider capacity/queue
            self._cond.notify_all()
        

        return handle

    def attach(self, run: RunMaterialized) -> RunHandle | None:
        """Best-effort attach to an existing local run.

        Rules (initial implementation):
        - Only attach to runs that were created by *this* runner name.
        - We do not reconstruct the in-memory queue; queued runs are not attachable.
        - For running or finished runs, we return a passive handle that reads
          ``_tl/run.json`` for status and exposes stdout/stderr paths.
        - We *do not* re-parent or track the OS process in the monitor thread yet.
          (No reliable cross-process ``Popen`` attach exists in the stdlib.)
        """
        run_dir: Path = run.run_dir.path
        run_id = str(getattr(run, "run_id", None) or run_dir.name)

        # Read run.json (if missing or malformed, we can't attach)
        state_path = runstate_path(run_dir)
        doc = _json_load(state_path)
        if not doc:
            return None

        # Must be the same runner name that wrote the state, otherwise bail.
        runner_name = doc.get("runner")
        if runner_name and str(runner_name) != self.name:
            return None

        # Determine stdout/stderr paths from saved spec, else fall back to defaults.
        try:
            ls = doc.get("launch_spec") or {}
        except Exception:
            ls = {}
        stdout_p = Path(ls.get("stdout_path") or default_stdout_path(run_dir))
        stderr_p = Path(ls.get("stderr_path") or default_stderr_path(run_dir))

        # Construct a passive handle (no subprocess attached).
        handle = _LocalRunHandle(self, run_id, run_dir, None, stdout_p, stderr_p)

        # If the run is already terminal, mark events so wait() returns promptly.
        st = doc.get("state")
        st = RunStatus(st) if st is not None else RunStatus.QUEUED

        if st in TERMINAL_STATES:
            handle._started_evt.set()
            handle._finished_evt.set()
        elif st is RunStatus.RUNNING:
            # Best-effort: mark started so client code can distinguish from queued.
            handle._started_evt.set()
        # QUEUED → leave both events unset.

        # Optionally verify the PID is alive for RUNNING; purely informational.
        # We don't currently adopt the process into the monitor thread.
        if st is RunStatus.RUNNING:
            pid_s = doc.get("external_id")
            try:
                pid = int(pid_s) if pid_s is not None else None
            except Exception:
                pid = None
            if pid is not None and not self._pid_alive(pid):
                # Process seems gone but run.json wasn't finalized. We leave it as-is
                # (conservative) and let a future orchestrator/repair step decide.
                pass

        return handle

    def close(self) -> None:
        """Stop the monitor thread. We don't mutate per-run state here."""
        self._stop_event.set()
        # Wake the monitor so it can exit promptly
        with self._active_lock:
            self._cond.notify_all()
        self._monitor_thread.join(timeout=2.0)

    # ---- internal: cancellation of runs ------------------------------

    def _cancel_queued(self, run_dir: Path, *, handle: _LocalRunHandle) -> None:
        with self._active_lock:
            # Remove from pending if present
            idx = None
            for i, item in enumerate(self._pending):
                if item.run_dir == run_dir:
                    idx = i
                    break
            if idx is None:
                # It might have just started; nothing to do here — the running cancel path will handle it.
                return
            item = self._pending[idx]
            del self._pending[idx]

        # Mark cancelled in run.json
        with item.lock:
            path = runstate_path(run_dir)
            doc = _json_load(path) or {}
            doc["state"] = RunStatus.CANCELLED
            doc["finished_at"] = _now_iso()
            doc["return_code"] = None
            _json_atomic_write(path, doc)
            _append_event(run_dir, item.lock, state=RunStatus.CANCELLED, reason="cancelled while queued")
        handle._finished_evt.set()
        # Drop the per-run lock since the run was never started
        with self._locks_lock:
            self._locks.pop(run_dir, None)
        # Wake monitor in case it was waiting for capacity/queue changes
        with self._active_lock:
            self._cond.notify_all()

    def _cancel_running(self, run_dir: Path, *, handle: _LocalRunHandle, force: bool = False) -> None:
        """Best-effort cancellation for a running process under the runner lock."""
        with self._active_lock:
            rec = self._active.get(run_dir)
            if rec is None or rec.handle._proc is None:
                # No longer running (finished or race); nothing to do.
                return
            handle._cancel_requested = True
            proc = rec.handle._proc
            try:
                if os.name == "posix":
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    except Exception:
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                    if force:
                        try:
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        except Exception:
                            try:
                                proc.kill()
                            except Exception:
                                pass
                else:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    if force:
                        try:
                            proc.kill()
                        except Exception:
                            pass
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

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        """Best-effort check whether a PID is alive on this host.

        POSIX: uses os.kill(pid, 0).
        Windows: os.kill(pid, 0) often raises PermissionError for foreign-session processes;
        we treat PermissionError as "probably alive".
        """
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            # Unknown → assume alive to avoid false negatives.
            return True


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
                proc = rec.handle._proc
                if proc is None:
                    # shouldn't happen for active records
                    continue

                # Enforce wall-clock timeout
                if rec.deadline_monotonic is not None and proc.poll() is None and now >= rec.deadline_monotonic:
                    rec.handle._timed_out = True
                    self._signal_timeout(proc)
                    _append_event(run_dir, rec.lock, state=RunStatus.TIMED_OUT, reason="timeout")
                    rec.deadline_monotonic = None  # prevent repeated signaling

                # Finalization on process exit
                rc = proc.poll()
                if rc is not None:
                    finished_at = _now_iso()
                    if rec.handle._timed_out:
                        final_state = RunStatus.TIMED_OUT
                    elif rec.handle._cancel_requested:
                        final_state = RunStatus.CANCELLED
                    else:
                        final_state = RunStatus.SUCCEEDED if rc == 0 else RunStatus.FAILED

                    with rec.lock:
                        path = runstate_path(run_dir)
                        doc = _json_load(path) or {}
                        doc.update({
                            "state": final_state,
                            "finished_at": finished_at,
                            "return_code": rc,
                        })
                        _json_atomic_write(path, doc)
                        _append_event(run_dir, rec.lock, state=final_state, reason="process exited")

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
                            path = runstate_path(item.run_dir)
                            doc = _json_load(path) or {}
                            doc["state"] = RunStatus.FAILED
                            doc["finished_at"] = _now_iso()
                            doc["return_code"] = None
                            _json_atomic_write(path, doc)
                            _append_event(item.run_dir, item.lock, state=RunStatus.FAILED, reason=f"spawn failed: {exc!s}")
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

    def _spawn_from_pending_locked(self, item: _PendingItem) -> None:
        """
        Spawn a queued run NOW (caller must hold _active_lock). On success,
        writes 'running', registers into _active, and updates the handle.
        On failure, raises; caller is responsible for marking FAILED state.
        """
        run_dir = item.run_dir
        spec = item.spec
        lock = item.lock

        # Compute absolute cwd under run_dir
        cwd_abs = run_dir if spec.cwd is None else (run_dir / spec.cwd)

        # Prepare environment
        env = os.environ.copy()
        if spec.env:
            env.update(spec.env)

        popen_kwargs: dict[str, Any] = {}
        if os.name == "posix":
            popen_kwargs["preexec_fn"] = os.setsid
            popen_kwargs["start_new_session"] = True
        elif os.name == "nt":
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]

        proc = subprocess.Popen(
            list(spec.cmd),
            cwd=str(cwd_abs),
            env=env,
            stdout=open(item.stdout_path, "ab", buffering=0),
            stderr=open(item.stderr_path, "ab", buffering=0),
            text=False,
            encoding=None,
            errors=None,
            **popen_kwargs,
        )
        proc = cast(subprocess.Popen[bytes], proc)

        # Mark running
        with lock:
            started_at = _now_iso()
            path = runstate_path(run_dir)
            doc = _json_load(path) or {}
            doc["state"] = RunStatus.RUNNING
            doc["started_at"] = started_at
            doc["external_id"] = str(proc.pid)
            _json_atomic_write(path, doc)
            _append_event(run_dir, lock, state=RunStatus.RUNNING, reason=f"spawned pid {proc.pid}")

        # Update handle & register
        item.handle._proc = proc
        item.handle._stdout = item.stdout_path
        item.handle._stderr = item.stderr_path
        item.handle._started_evt.set()

        deadline: Optional[float] = None
        tl = spec.resources.time_limit_s
        if tl is not None and tl > 0:
            deadline = time.monotonic() + float(tl)

        rec = _RunRecord(
            run_id=item.run_id,
            run_dir=run_dir,
            handle=item.handle,
            stdout_path=item.stdout_path,
            stderr_path=item.stderr_path,
            deadline_monotonic=deadline,
            lock=lock,
        )
        self._active[run_dir] = rec

    @staticmethod
    def _signal_timeout(proc: subprocess.Popen[bytes]) -> None:
        """
        Timeout handling: graceful TERM then forceful KILL after a short grace.
        """
        if os.name == "posix":
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        else:
            # try a soft break first, works if console + new process group
            try:
                proc.send_signal(signal.CTRL_BREAK_EVENT) # type: ignore[attr-defined]
            except Exception:
                pass

            try:
                proc.terminate()
            except Exception:
                pass

            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except Exception:
                    pass
