"""Save and load the collector's resume point (the time_us of the newest event on disk)."""

import json
import os
from pathlib import Path

from bluesky_stream_analytics.fs import fsync_directory


def load_cursor(path: Path) -> int | None:
    """Return the saved time_us, or None if there is none. Raises ValueError if the file is unreadable."""
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None

    try:
        return int(json.loads(text)["time_us"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Cursor file {path} is unreadable: {text!r}") from exc


def save_cursor(path: Path, time_us: int) -> None:
    """Atomically save time_us (write to a temp file, then rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")

    with open(tmp_path, "w") as f:
        json.dump({"time_us": time_us}, f)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, path)
    fsync_directory(path.parent)

