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

def _as_status(x: Any) -> RunStatus | None:
    if isinstance(x, RunStatus):
        return x
    if isinstance(x, str):
        try:
            return RunStatus(x)
        except ValueError:
            return None
    return None

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

    def run_id(self) -> str:
        return self._run_id

    def external_id(self) -> str | None:
        if self._proc is not None:
            return str(self._proc.pid)
        doc = _json_load(runstate_path(self._run_dir)) or {}
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
        doc = _json_load(runstate_path(self._run_dir))
        if not doc:
            return RunStatus.CANCELLED if self._cancel_requested else RunStatus.FAILED
        state = _as_status(doc.get("state"))

        # Auto-finalize stale RUNNING on POSIX if pid is gone
        if state == RunStatus.RUNNING and os.name == "posix":
            pid_val = doc.get("external_id")
            try:
                pid = int(pid_val) if pid_val is not None else None
            except Exception:
                pid = None
            if pid is not None and not self._runner._pid_alive(pid):
                self._runner._finalize_unknown_exit(self._run_dir, state=RunStatus.FAILED, reason="pid_not_found")
                doc = _json_load(runstate_path(self._run_dir)) or {"state": RunStatus.FAILED}
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

    def cancel(self, force: bool = False, reason: str | None = None) -> None:
        """
        Best-effort cancellation.
        - Queued: remove from runner queue and mark CANCELLED.
        - Running (live): signal via runner under lock.
        - Running (attached): kill by PID/PGID and finalize.
        """
        _ = reason  # TODO: plumb into events
        self._cancel_requested = True

        if self._proc is None:
            doc = _json_load(runstate_path(self._run_dir)) or {}
            state = _as_status(doc.get("state"))
            if state == RunStatus.RUNNING:
                self._runner._cancel_attached(self._run_dir, force=force, handle=self)
                return
            # queued or unknown
            self._runner._cancel_queued(self._run_dir, handle=self)
            return

        # Live running
        self._runner._cancel_running(self._run_dir, handle=self, force=force)

    def return_code(self) -> int | None:
        return None if self._proc is None else self._proc.returncode

    def stdout_path(self) -> Path | None:
        return self._stdout

    def stderr_path(self) -> Path | None:
        return self._stderr
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
        """Attach to an existing run directory.

        Returns a passive handle (no Popen). If the run is terminal, the handle's
        finished event is set. If the run is RUNNING but the PID is missing and
        we're on POSIX, auto-finalize to FAILED (pid_not_found).
        """
        run_dir: Path = run.run_dir.path
        doc = _json_load(runstate_path(run_dir))
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
            stdout_p = Path(raw_stdout) if isinstance(raw_stdout, str) else default_stdout_path(run_dir)
        except Exception:
            stdout_p = default_stdout_path(run_dir)
        try:
            raw_stderr = ls.get("stderr_path")
            stderr_p = Path(raw_stderr) if isinstance(raw_stderr, str) else default_stderr_path(run_dir)
        except Exception:
            stderr_p = default_stderr_path(run_dir)

        run_id = str(doc.get("run_id") or run_dir.name)
        handle = _LocalRunHandle(self, run_id, run_dir, None, stdout_p, stderr_p)

        state = _as_status(doc.get("state"))
        if state in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT}:
            handle._started_evt.set()
            handle._finished_evt.set()
            return handle

        if state == RunStatus.RUNNING:
            # POSIX: check liveness and auto-finalize if gone.
            pid_val = doc.get("external_id")
            try:
                pid = int(pid_val) if pid_val is not None else None
            except Exception:
                pid = None
            if pid is not None and os.name == "posix" and not self._pid_alive(pid):
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

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        """Best-effort check. POSIX: os.kill(pid, 0). Windows: conservative True.

        Returning True on Windows avoids auto-finalizing based on a potentially
        incorrect liveness check. Explicit cancel() still works via taskkill.
        """
        if os.name == "posix":
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return False
            except PermissionError:
                # Process exists but is not ours.
                return True
            else:
                return True
        else:
            # Conservative: assume alive/unknown.
            return True

    def _finalize_unknown_exit(self, run_dir: Path, *, state: str, reason: str) -> None:
        """Idempotently flip a non-terminal run.json into a terminal state.

        Used when we detect a stale RUNNING state but the PID is gone, or after
        a PID-based termination where we cannot retrieve a return code.
        """
        lock = self._get_run_lock(run_dir)
        with lock:
            path = runstate_path(run_dir)
            doc = _json_load(path) or {}
            cur = _as_status(doc.get("state"))
            if cur in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT}:
                return
            doc["state"] = state
            doc["finished_at"] = _now_iso()
            # keep existing return_code if set; else None
            if "return_code" not in doc:
                doc["return_code"] = None
            # advisory flags
            doc["finalized_by_attach"] = True
            doc["reason"] = reason
            _json_atomic_write(path, doc)
            _append_event(run_dir, lock, state=state, reason=reason)

    def _terminate_pid(self, *, pid: int, pgid: int | None, force: bool, grace_s: float = 5.0) -> None:
        """Send termination to pid/pgid, escalate if needed.

        POSIX: prefer process group if available. Windows: use taskkill.
        """
        if os.name == "posix":
            try:
                if pgid is not None:
                    os.killpg(pgid, signal.SIGTERM)
                else:
                    os.kill(pid, signal.SIGTERM)
            except Exception:
                pass

            # poll up to grace
            deadline = time.monotonic() + float(grace_s)
            while time.monotonic() < deadline:
                if not self._pid_alive(pid):
                    break
                time.sleep(0.1)

            if self._pid_alive(pid):
                # escalate
                try:
                    if pgid is not None:
                        os.killpg(pgid, signal.SIGKILL)
                    else:
                        os.kill(pid, signal.SIGKILL)
                except Exception:
                    pass
        else:
            # Windows: best-effort. /T kills children; first try gentle, then force.
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
                )
            except Exception:
                pass
            # Always issue a forced kill as a follow-up if force=True,
            # or unconditionally since we can't reliably poll liveness.
            if force:
                try:
                    subprocess.run(
                        ["taskkill", "/PID", str(pid), "/T", "/F"],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
                    )
                except Exception:
                    pass

    def _cancel_attached(self, run_dir: Path, *, force: bool, handle: "_LocalRunHandle") -> None:
        """Cancel a RUNNING attached run via PID/PGID and finalize to CANCELLED."""
        lock = self._get_run_lock(run_dir)
        with lock:
            doc = _json_load(runstate_path(run_dir)) or {}
            state = _as_status(doc.get("state"))
            if state in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT}:
                return
            if state != RunStatus.RUNNING:
                # Not running; treat as queued or unknown → mark cancelled.
                self._finalize_unknown_exit(run_dir, state=RunStatus.CANCELLED, reason="user_cancel_nonrunning")
                handle._finished_evt.set()
                return
            # RUNNING
            pid_val = doc.get("external_id")
            try:
                pid = int(pid_val) if pid_val is not None else None
            except Exception:
                pid = None
            pgid_val = doc.get("pgid")
            try:
                pgid = int(pgid_val) if pgid_val is not None else None
            except Exception:
                pgid = None

        if pid is not None:
            self._terminate_pid(pid=pid, pgid=pgid, force=force)
        # Regardless of platform/liveness certainty, mark CANCELLED.
        self._finalize_unknown_exit(run_dir, state=RunStatus.CANCELLED, reason="user_cancel")
        handle._finished_evt.set()
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
            # Record process group id (POSIX) to enable group termination
            pgid = None
            if os.name == "posix":
                try:
                    pgid = os.getpgid(proc.pid)
                except Exception:
                    pgid = None
            doc["pgid"] = pgid
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
