from __future__ import annotations

from typing import Any, ClassVar, Literal
import os
import signal
import subprocess

from .base import PlatformOps, TerminationMode


class _Posix(PlatformOps):
    name: ClassVar[Literal["posix","nt"]] = "posix"

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

    def _send_group_signal(self, target_pid: int, sig: int) -> bool:
        try:
            pgid = os.getpgid(target_pid)
        except Exception:
            pgid = None

        sent = False
        if pgid is not None:
            try:
                os.killpg(pgid, sig)
                sent = True
            except Exception:
                pass

        if not sent:
            try:
                os.kill(target_pid, sig)
                sent = True
            except Exception:
                pass

        return sent

    def terminate_tree_by(self, proc_or_pid: int | subprocess.Popen[Any], mode: TerminationMode) -> None:
        target_pid = proc_or_pid.pid if isinstance(proc_or_pid, subprocess.Popen) else int(proc_or_pid)
        sig = signal.SIGTERM if mode == "soft" else signal.SIGKILL

        try:
            self._send_group_signal(target_pid, sig)
        except Exception:
            if isinstance(proc_or_pid, subprocess.Popen):
                try:
                    (proc_or_pid.terminate() if mode == "soft" else proc_or_pid.kill())
                except Exception:
                    pass


platform_impl: PlatformOps = _Posix()

