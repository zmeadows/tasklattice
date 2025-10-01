from pathlib import Path


def ensure_parent_dirs(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
