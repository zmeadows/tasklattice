from __future__ import annotations

import signal
import subprocess
from typing import Any, ClassVar, Literal

from .base import PlatformOps, TerminationMode


class _Win(PlatformOps):
    name: ClassVar[Literal["posix", "nt"]] = "nt"

    def configure_popen_group(self, popen_kwargs: dict[str, Any]) -> None:
        new_group_flag = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        existing = int(popen_kwargs.get("creationflags", 0))
        popen_kwargs["creationflags"] = existing | new_group_flag

    def pid_alive(self, pid: int) -> bool | None:
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:
            return None

        out = (result.stdout or "").strip()
        if not out:
            return None
        if "No tasks are running" in out:
            return False
        return True if str(pid) in out else None

    def _taskkill(self, pid: int, *, force: bool) -> None:
        try:
            args: list[str] = ["taskkill", "/PID", str(pid), "/T"]
            if force:
                args.append("/F")
            subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        except Exception:
            pass

    def terminate_tree_by(self, proc_or_pid: int | subprocess.Popen[Any], mode: TerminationMode) -> None:
        if isinstance(proc_or_pid, subprocess.Popen):
            if mode == "soft":
                try:
                    proc_or_pid.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
                    return
                except Exception:
                    # fall through to taskkill
                    pass
            pid = proc_or_pid.pid
        else:
            pid = int(proc_or_pid)

        if mode == "soft":
            self._taskkill(pid, force=False)
        else:
            self._taskkill(pid, force=True)


platform_impl: PlatformOps = _Win()

