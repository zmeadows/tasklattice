import os
import tempfile
from pathlib import Path


def ensure_parent_dirs(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def write_bytes_atomic(path: Path, out: bytes) -> None:
    ensure_parent_dirs(path)

    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))

    try:
        with os.fdopen(fd, "wb") as f:
            f.write(out)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)

        # Make the rename durable
        try:
            dfd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
    except Exception:
        # Best-effort cleanup
        try:
            os.unlink(tmp_name)
        except Exception:
            pass
        raise
