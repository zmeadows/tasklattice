from __future__ import annotations
from typing import Any, ClassVar, Literal, Protocol, overload
import subprocess

TerminationMode = Literal["soft", "hard"]

class PlatformOps(Protocol):
    name: ClassVar[Literal["posix", "nt"]]

    def configure_popen_group(self, popen_kwargs: dict[str, Any]) -> None: ...

    def pid_alive(self, pid: int) -> bool | None: ...

    @overload
    def terminate_tree_by(self, proc_or_pid: int, mode: TerminationMode) -> None: ...
    @overload
    def terminate_tree_by(self, proc_or_pid: subprocess.Popen[Any], mode: TerminationMode) -> None: ...
    def terminate_tree_by(self, proc_or_pid: int | subprocess.Popen[Any], mode: TerminationMode) -> None: ...

