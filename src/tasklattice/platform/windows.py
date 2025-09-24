import signal
import subprocess
from typing import Any

from tasklattice.platform.base import PlatformOps


class _Win(PlatformOps):
    name = "nt"

    def configure_popen_group(self, popen_kwargs: dict[str, Any]) -> None:
        creation_flag: int = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        popen_kwargs["creationflags"] = creation_flag

    def pid_alive(self, pid: int) -> bool:
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:
            return True
        stdout = result.stdout or ""
        if "No tasks are running" in stdout:
            return False
        return str(pid) in stdout

    def terminate_pid_tree(self, pid: int, *, force: bool) -> None:
        try:
            args: list[str] = ["taskkill", "/PID", str(pid), "/T"]
            if force:
                args.append("/F")
            subprocess.run(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except Exception:
            pass

    def terminate_process_tree(self, proc: subprocess.Popen[bytes], *, force: bool) -> None:
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass
        if force:
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def soft_terminate(self, proc: subprocess.Popen[bytes]) -> None:
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass

    def hard_kill(self, proc: subprocess.Popen[bytes]) -> None:
        try:
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


platform_impl = _Win()
