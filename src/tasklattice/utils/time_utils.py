from datetime import UTC, datetime


def now_iso() -> str:
    return datetime.now(UTC).isoformat()
