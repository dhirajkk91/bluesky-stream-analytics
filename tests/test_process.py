"""End-to-end tests of loading the event store, against a table in a local DuckDB database."""

import pytest

from sample_events import delete, follow, identity, like, time_us, write_hour_file

from bluesky_stream_analytics.event_store import COLUMNS
from bluesky_stream_analytics.process import connect, count_lines, pending_files, process_all

TABLE = "events"


@pytest.fixture
def con():
    con = connect(threads=2, memory_limit="1GB", extension_dir=None)
    con.execute(f"CREATE TABLE {TABLE} ({COLUMNS})")
    yield con
    con.close()


def table_rows(con):
    return con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]


def likes(hour, count, start=0, minute=0):
    return [like(f"did:plc:{i}", f"3l{i}", time_us(hour, minute, i % 60, i)) for i in range(start, start + count)]


def test_loads_events_and_removes_the_files(con, tmp_path):
    write_hour_file(tmp_path, 10, "a", likes(10, 50) + ["not json"])
    write_hour_file(tmp_path, 11, "b", [follow("did:plc:x", "3f", time_us(11)), identity("did:plc:x", 7, time_us(11))])

    [result] = process_all(con, TABLE, tmp_path, batch_files=6)

    assert (result.files, result.lines, result.unreadable, result.added) == (2, 53, 1, 52)
    assert table_rows(con) == 52
    assert pending_files(tmp_path) == []
    assert list((tmp_path / "uploaded").iterdir()) == []


def test_running_again_on_the_same_events_adds_nothing(con, tmp_path):
    events = likes(10, 30)
    write_hour_file(tmp_path, 10, "a", events)
    process_all(con, TABLE, tmp_path, batch_files=6)

    write_hour_file(tmp_path, 10, "a", events)
    [result] = process_all(con, TABLE, tmp_path, batch_files=6)

    assert (result.already_in_table, result.added) == (30, 0)
    assert table_rows(con) == 30


def test_repeats_inside_a_batch_are_stored_once(con, tmp_path):
    events = likes(10, 20)
    write_hour_file(tmp_path, 10, "a", events + events[-5:])

    [result] = process_all(con, TABLE, tmp_path, batch_files=6)

    assert (result.duplicates_in_batch, result.added) == (5, 20)


def test_rewind_across_the_hour_change_is_deduplicated_against_the_earlier_hour(con, tmp_path):
    # The restart's 5-second rewind lands the end of hour 09 again, in hour 10's file.
    end_of_nine = [like(f"did:plc:late{i}", f"3l{i}", time_us(9, 59, 55 + i)) for i in range(5)]
    write_hour_file(tmp_path, 9, "a", likes(9, 40) + end_of_nine)
    process_all(con, TABLE, tmp_path, batch_files=6)

    write_hour_file(tmp_path, 10, "b", end_of_nine + likes(10, 40, start=100))
    [result] = process_all(con, TABLE, tmp_path, batch_files=6)

    assert (result.already_in_table, result.added) == (5, 40)
    assert table_rows(con) == 85


def test_delete_is_a_separate_event_from_the_create(con, tmp_path):
    write_hour_file(tmp_path, 10, "a", [like("did:plc:a", "3l", time_us(10)), delete("did:plc:a", "app.bsky.feed.like", "3l", time_us(10, 5))])

    process_all(con, TABLE, tmp_path, batch_files=6)

    ops = [r[0] for r in con.execute(f"SELECT operation FROM {TABLE} ORDER BY event_time").fetchall()]
    assert ops == ["create", "delete"]


def test_files_are_loaded_in_batches_oldest_first(con, tmp_path):
    for hour in (12, 10, 11):
        write_hour_file(tmp_path, hour, f"f{hour}", likes(hour, 10, start=hour * 100))

    results = process_all(con, TABLE, tmp_path, batch_files=1)

    assert [r.added for r in results] == [10, 10, 10]
    sources = [r[0] for r in con.execute(f"SELECT DISTINCT source_file FROM {TABLE} ORDER BY source_file").fetchall()]
    assert sources == [f"events/date=2026-10-24/hour={h}/f{h}.jsonl.zst" for h in (10, 11, 12)]


def test_count_lines_streams_compressed_files(tmp_path):
    a = write_hour_file(tmp_path, 10, "a", ["x"] * 3)
    b = write_hour_file(tmp_path, 11, "b", ["y"] * 4)
    assert count_lines([a, b]) == 7
