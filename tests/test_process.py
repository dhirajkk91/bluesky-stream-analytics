"""End-to-end tests of loading the event store, against a local Iceberg catalog."""

from pathlib import Path

import pytest

from sample_events import delete, follow, identity, like, time_us, write_hour_file

from bluesky_stream_analytics.process import EVENTS_TABLE, pending_files, process_all


@pytest.fixture
def lake(spark):
    """A fresh, empty events table."""
    if not spark.has_iceberg:
        pytest.skip("Iceberg runtime jar not in .jars/")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bluesky_store")
    spark.sql(f"DROP TABLE IF EXISTS {EVENTS_TABLE} PURGE")
    return spark


def table_rows(spark):
    return spark.table(EVENTS_TABLE).count()


def likes(hour, count, start=0, minute=0):
    return [like(f"did:plc:{i}", f"3l{i}", time_us(hour, minute, i % 60, i)) for i in range(start, start + count)]


def test_loads_events_and_removes_the_files(lake, tmp_path):
    write_hour_file(tmp_path, 10, "a", likes(10, 50) + ["not json"])
    write_hour_file(tmp_path, 11, "b", [follow("did:plc:x", "3f", time_us(11)), identity("did:plc:x", 7, time_us(11))])

    [result] = process_all(lake, tmp_path, batch_files=6)

    assert (result.files, result.lines, result.unreadable, result.added) == (2, 53, 1, 52)
    assert table_rows(lake) == 52
    assert pending_files(tmp_path) == []
    assert list((tmp_path / "uploaded").iterdir()) == []


def test_running_again_on_the_same_events_adds_nothing(lake, tmp_path):
    events = likes(10, 30)
    write_hour_file(tmp_path, 10, "a", events)
    process_all(lake, tmp_path, batch_files=6)

    write_hour_file(tmp_path, 10, "a", events)
    [result] = process_all(lake, tmp_path, batch_files=6)

    assert (result.already_in_table, result.added) == (30, 0)
    assert table_rows(lake) == 30


def test_repeats_inside_a_batch_are_stored_once(lake, tmp_path):
    events = likes(10, 20)
    write_hour_file(tmp_path, 10, "a", events + events[-5:])

    [result] = process_all(lake, tmp_path, batch_files=6)

    assert (result.duplicates_in_batch, result.added) == (5, 20)


def test_rewind_across_the_hour_change_is_deduplicated_against_the_earlier_hour(lake, tmp_path):
    # The restart's 5-second rewind lands the end of hour 09 again, in hour 10's file.
    end_of_nine = [like(f"did:plc:late{i}", f"3l{i}", time_us(9, 59, 55 + i)) for i in range(5)]
    write_hour_file(tmp_path, 9, "a", likes(9, 40) + end_of_nine)
    process_all(lake, tmp_path, batch_files=6)

    write_hour_file(tmp_path, 10, "b", end_of_nine + likes(10, 40))
    [result] = process_all(lake, tmp_path, batch_files=6)

    assert (result.already_in_table, result.added) == (5, 40)
    assert table_rows(lake) == 85


def test_delete_is_a_separate_event_from_the_create(lake, tmp_path):
    write_hour_file(tmp_path, 10, "a", [like("did:plc:a", "3l", time_us(10)), delete("did:plc:a", "app.bsky.feed.like", "3l", time_us(10, 5))])

    process_all(lake, tmp_path, batch_files=6)

    ops = [r.operation for r in lake.sql(f"SELECT operation FROM {EVENTS_TABLE} ORDER BY time_us").collect()]
    assert ops == ["create", "delete"]


def test_each_batch_is_its_own_commit(lake, tmp_path):
    for hour in (10, 11, 12):
        write_hour_file(tmp_path, hour, f"f{hour}", likes(hour, 10))

    results = process_all(lake, tmp_path, batch_files=1)

    assert [r.added for r in results] == [10, 10, 10]
    snapshots = lake.sql(f"SELECT count(*) AS n FROM {EVENTS_TABLE}.snapshots").first().n
    assert snapshots == 3


def test_rows_record_their_source_file(lake, tmp_path):
    write_hour_file(tmp_path, 10, "1792886498967668_d812497f", likes(10, 3))

    process_all(lake, tmp_path, batch_files=6)

    sources = {r.source_file for r in lake.table(EVENTS_TABLE).select("source_file").collect()}
    assert sources == {"events/date=2026-10-24/hour=10/1792886498967668_d812497f.jsonl.zst"}


def test_table_is_partitioned_by_event_hour(lake, tmp_path):
    write_hour_file(tmp_path, 10, "a", likes(9, 5, minute=59) + likes(10, 5, start=5))

    process_all(lake, tmp_path, batch_files=6)

    partitions = lake.sql(f"SELECT count(*) AS n FROM {EVENTS_TABLE}.partitions").first().n
    assert partitions == 2


def test_merge_only_reads_the_hours_in_the_batch(lake, tmp_path):
    write_hour_file(tmp_path, 8, "a", likes(8, 20))
    process_all(lake, tmp_path, batch_files=6)

    # Remove hour 08's data files. A merge that read them would fail.
    for row in lake.sql(f"SELECT file_path FROM {EVENTS_TABLE}.files").collect():
        Path(row.file_path.removeprefix("file:")).unlink()

    write_hour_file(tmp_path, 10, "b", likes(10, 20))
    [result] = process_all(lake, tmp_path, batch_files=6)

    assert result.added == 20
