import os
import signal
import subprocess
from typing import Any

from tasklattice.platform.base import PlatformOps


class _Posix(PlatformOps):
    name = "posix"

    def configure_popen_group(self, popen_kwargs: dict[str, Any]) -> None:
        popen_kwargs["preexec_fn"] = os.setsid
        popen_kwargs["start_new_session"] = True

    def pid_alive(self, pid: int) -> bool: 
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def terminate_pid_tree(self, pid: int, *, force: bool) -> None:
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
            if force:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
        except Exception:
            pass

    def terminate_process_tree(self, proc: subprocess.Popen[bytes], *, force: bool) -> None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            if force:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass
            if force:
                try:
                    proc.kill()
                except Exception:
                    pass

    def soft_terminate(self, proc: subprocess.Popen[bytes]) -> None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass

    def hard_kill(self, proc: subprocess.Popen[bytes]) -> None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

platform_impl = _Posix()

