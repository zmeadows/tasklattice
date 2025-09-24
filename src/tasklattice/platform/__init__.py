import importlib
import os
from typing import Final

from tasklattice.platform.base import PlatformOps

_mod = {"posix": ".posix", "nt": ".windows"}.get(os.name, ".posix")
platform: Final[PlatformOps] = importlib.import_module(_mod, __name__).platform_impl

