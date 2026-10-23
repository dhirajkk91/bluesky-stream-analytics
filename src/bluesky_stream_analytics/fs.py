"""Filesystem helpers for crash-safe writes."""

import os
from pathlib import Path


def fsync_directory(directory: Path) -> None:
    """Flush a directory to disk so a create or rename in it survives a crash."""
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
