import subprocess
import time
from typing import Any, Protocol


class PlatformOps(Protocol):
    name: str  # "posix" | "nt"

    def configure_popen_group(self, popen_kwargs: dict[str, Any]) -> None: ...
    def pid_alive(self, pid: int) -> bool: ...
    def terminate_pid_tree(self, pid: int, *, force: bool) -> None: ...
    def terminate_process_tree(self, proc: subprocess.Popen[bytes], *, force: bool) -> None: ...
    def soft_terminate(self, proc: subprocess.Popen[bytes]) -> None: ...
    def hard_kill(self, proc: subprocess.Popen[bytes]) -> None: ...

    def graceful_kill(self, proc: subprocess.Popen[bytes], *, grace_seconds: float = 5.0) -> None:
        """
        Timeout handling: graceful TERM then forceful KILL after a short grace.
        """
        self.soft_terminate(proc)

        try:
            proc.wait(timeout=max(0.3, grace_seconds))
        except Exception:
            self.hard_kill(proc)

    def graceful_kill_pid(self, pid: int, *, force: bool = False, grace_seconds: float = 5.0) -> None:
        """
        Attempt to gracefully terminate the process tree rooted at `pid`,
        then force-kill if it hasn't exited within `grace_seconds`.
        """
        # 1) Ask nicely
        self.terminate_pid_tree(pid, force=False)

        # 2) Wait up to grace_seconds for the PID to disappear
        deadline = time.monotonic() + max(0.3, grace_seconds)
        while time.monotonic() < deadline:
            if not self.pid_alive(pid):
                return
            time.sleep(0.1)

        # 3) Escalate
        if self.pid_alive(pid) or force:
            self.terminate_pid_tree(pid, force=True)
