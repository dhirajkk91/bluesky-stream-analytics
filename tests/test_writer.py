import json
from datetime import UTC, datetime

import pytest
from compression import zstd

from bluesky_stream_analytics.writer import (
    HourlyFileWriter,
    finalize_open_file,
    recover_open_files,
    truncate_partial_last_line,
)

# 2026-10-23 13:59:59 UTC, one second before an hour boundary.
BEFORE_BOUNDARY_US = int(datetime(2026, 10, 23, 13, 59, 59, tzinfo=UTC).timestamp() * 1_000_000)
AFTER_BOUNDARY_US = BEFORE_BOUNDARY_US + 2_000_000


def event(time_us: int) -> str:
    return json.dumps({"did": "did:plc:test", "time_us": time_us, "kind": "commit"}, separators=(",", ":"))


def finished_files(root):
    return sorted((root / "events").rglob("*.jsonl.zst"))


def read_lines(path) -> list[str]:
    with zstd.open(path, "rt") as f:
        return f.read().splitlines()


def test_close_produces_compressed_file_with_every_event(tmp_path):
    writer = HourlyFileWriter(tmp_path)
    raws = [event(BEFORE_BOUNDARY_US - i) for i in range(3, 0, -1)]
    for raw in raws:
        writer.write(raw, json.loads(raw)["time_us"])

    assert writer.close() == BEFORE_BOUNDARY_US - 1

    [path] = finished_files(tmp_path)
    assert path.parent.as_posix().endswith("events/date=2026-10-23/hour=13")
    assert read_lines(path) == raws
    assert not list(tmp_path.rglob("*.open"))


def test_rolls_over_to_a_new_file_at_the_hour_boundary(tmp_path):
    writer = HourlyFileWriter(tmp_path)
    writer.write(event(BEFORE_BOUNDARY_US), BEFORE_BOUNDARY_US)
    writer.write(event(AFTER_BOUNDARY_US), AFTER_BOUNDARY_US)

    # The 13:00 file is finished as soon as the first 14:00 event arrives.
    assert [p.parent.name for p in finished_files(tmp_path)] == ["hour=13"]

    writer.close()
    files = finished_files(tmp_path)
    assert [p.parent.name for p in files] == ["hour=13", "hour=14"]
    assert read_lines(files[1]) == [event(AFTER_BOUNDARY_US)]


def test_event_from_an_earlier_hour_goes_into_the_current_file(tmp_path):
    writer = HourlyFileWriter(tmp_path)
    writer.write(event(AFTER_BOUNDARY_US), AFTER_BOUNDARY_US)
    writer.write(event(BEFORE_BOUNDARY_US), BEFORE_BOUNDARY_US)
    writer.close()

    [path] = finished_files(tmp_path)
    assert path.parent.name == "hour=14"
    assert len(read_lines(path)) == 2


def test_sync_returns_newest_written_time_us(tmp_path):
    writer = HourlyFileWriter(tmp_path)
    assert writer.sync() is None
    writer.write(event(BEFORE_BOUNDARY_US), BEFORE_BOUNDARY_US)
    assert writer.sync() == BEFORE_BOUNDARY_US


def test_multiline_json_is_stored_on_one_line(tmp_path):
    writer = HourlyFileWriter(tmp_path)
    writer.write('{\n "time_us": 1\n}', BEFORE_BOUNDARY_US)
    writer.close()
    assert read_lines(finished_files(tmp_path)[0]) == ['{"time_us":1}']


def test_two_writers_never_share_a_file(tmp_path):
    for _ in range(2):
        writer = HourlyFileWriter(tmp_path)
        writer.write(event(BEFORE_BOUNDARY_US), BEFORE_BOUNDARY_US)
        writer.close()
    assert len(finished_files(tmp_path)) == 2


def test_finalize_refuses_to_overwrite_a_finished_file(tmp_path):
    open_path = tmp_path / "a.jsonl.open"
    open_path.write_text("x\n")
    (tmp_path / "a.jsonl.zst").write_bytes(b"already here")
    with pytest.raises(FileExistsError):
        finalize_open_file(open_path)


def test_truncate_partial_last_line(tmp_path):
    path = tmp_path / "f"
    path.write_bytes(b"one\ntwo\nthr")
    assert truncate_partial_last_line(path) == 3
    assert path.read_bytes() == b"one\ntwo\n"
    assert truncate_partial_last_line(path) == 0


def test_truncate_handles_lines_longer_than_one_read_chunk(tmp_path):
    path = tmp_path / "f"
    path.write_bytes(b"a" * 200_000 + b"\n" + b"b" * 150_000)
    assert truncate_partial_last_line(path) == 150_000
    assert path.stat().st_size == 200_001


def test_recovery_keeps_every_complete_line_after_a_crash(tmp_path):
    writer = HourlyFileWriter(tmp_path)
    raws = [event(BEFORE_BOUNDARY_US + i) for i in range(5)]
    for raw in raws:
        writer.write(raw, json.loads(raw)["time_us"])
    writer.sync()
    # Simulate a crash in the middle of writing the next event.
    writer._file.write(b'{"did":"did:plc:te')
    writer._file.flush()

    recovery = recover_open_files(tmp_path)
    [recovered] = recovery.files
    assert read_lines(recovered) == raws
    assert recovery.newest_time_us == BEFORE_BOUNDARY_US + 4
    assert not list(tmp_path.rglob("*.open"))


def test_recovery_after_crash_between_compress_and_delete(tmp_path):
    writer = HourlyFileWriter(tmp_path)
    writer.write(event(BEFORE_BOUNDARY_US), BEFORE_BOUNDARY_US)
    writer.close()
    [final_path] = finished_files(tmp_path)
    leftover = final_path.with_name(final_path.name.replace(".jsonl.zst", ".jsonl.open"))
    leftover.write_text(event(BEFORE_BOUNDARY_US) + "\n")

    assert recover_open_files(tmp_path).files == []
    assert not leftover.exists()
    assert read_lines(final_path) == [event(BEFORE_BOUNDARY_US)]


def test_recovery_removes_incomplete_compression_and_empty_files(tmp_path):
    directory = tmp_path / "events" / "date=2026-10-23" / "hour=13"
    directory.mkdir(parents=True)
    (directory / "a.jsonl.zst.tmp").write_bytes(b"partial")
    (directory / "b.jsonl.open").write_bytes(b"")

    recovery = recover_open_files(tmp_path)
    assert recovery.files == []
    assert recovery.newest_time_us is None
    assert list(directory.iterdir()) == []
