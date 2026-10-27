"""Tests of the projections: the fold, incremental reads by sequence number, and the views, against local files."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from sample_events import account, delete, edit_post, follow, identity, like, post, tid, time_us, write_hour_file

from bluesky_stream_analytics import projections
from bluesky_stream_analytics.event_store import COLUMNS, stage_batch
from bluesky_stream_analytics.process import connect
from bluesky_stream_analytics.projections import (
    create_tables,
    fold_files,
    last_sequence,
    new_files_by_sequence,
    project_all,
)

SCHEMA = "bluesky_store"
VIEWS = Path(__file__).parents[1] / "sql" / "views"
LIKE = "app.bsky.feed.like"
POST = "app.bsky.feed.post"


LIKED_AT = datetime(2026, 10, 24, 10, 0, 0, 123456, tzinfo=UTC)
OLD = datetime(2025, 3, 1, tzinfo=UTC)
K1, K2, K3 = tid(LIKED_AT), tid(LIKED_AT, 1), tid(LIKED_AT, 2)


@pytest.fixture
def con():
    con = connect(threads=2, memory_limit="1GB", extension_dir=None)
    con.execute(f"CREATE SCHEMA {SCHEMA}")
    create_tables(con, SCHEMA)
    con.execute(f"CREATE TABLE {SCHEMA}.events ({COLUMNS})")
    for view in ("current_records.sql", "accounts.sql"):
        con.execute((VIEWS / view).read_text())
    yield con
    con.close()


class Store:
    """A stand-in for the event store: each commit is one Parquet file written from real-shaped events."""

    def __init__(self, con, tmp_path):
        self.con = con
        self.tmp_path = tmp_path
        self.commits: list[tuple[str, str, int]] = []

    def commit(self, lines: list[str]) -> str:
        sequence = len(self.commits) + 1
        raw = write_hour_file(self.tmp_path / "raw", 10, f"c{sequence}", lines)
        stage_batch(self.con, [str(raw)])
        path = self.tmp_path / f"store-{sequence}.parquet"
        self.con.execute(f"COPY batch TO '{path}' (FORMAT parquet)")
        self.con.execute(f"INSERT INTO {SCHEMA}.events SELECT * FROM batch")
        self.con.execute("DROP TABLE batch")
        self.commits.append(("DATA", str(path), sequence))
        return str(path)

    def fold(self, *sequences: int) -> None:
        """Fold the given commits, in the order given, as if each were a run."""
        for sequence in sequences:
            _, path, _ = self.commits[sequence - 1]
            fold_files(self.con, SCHEMA, [path], sequence - 1, sequence)


@pytest.fixture
def store(con, tmp_path):
    return Store(con, tmp_path)


def us(when: datetime) -> int:
    return int(when.timestamp() * 1_000_000)


def current(con, collection=None):
    where = f"WHERE collection = '{collection}'" if collection else ""
    return con.execute(f"SELECT did, collection, rkey FROM {SCHEMA}.current_records {where} ORDER BY ALL").fetchall()


def test_tid_decodes_to_the_creation_time(con):
    con.execute(projections.TID_MACROS)
    seen = datetime(2026, 10, 24, 12, tzinfo=UTC)
    rows = con.execute(
        "SELECT epoch_us(tid_time(k, make_timestamptz(?))) FROM (VALUES (?), (?), (?), (?)) t(k)",
        [us(seen), K1, tid(OLD, 1023), "self", tid(datetime(2026, 11, 1, tzinfo=UTC))],
    ).fetchall()
    assert rows == [(us(LIKED_AT),), (us(OLD),), (None,), (None,)]  # not a TID, and from the future


def test_new_files_are_grouped_by_commit_and_only_after_the_last_run():
    files = [("DATA", "a", 1), ("DATA", "c", 3), ("DATA", "b", 2), ("DATA", "d", 3)]
    assert new_files_by_sequence(files, 0) == [(1, ["a"]), (2, ["b"]), (3, ["c", "d"])]
    assert new_files_by_sequence(files, 2) == [(3, ["c", "d"])]
    assert new_files_by_sequence(files, 3) == []


def test_commits_that_only_rewrite_rows_are_skipped():
    metadata = {
        "snapshots": [
            {"sequence-number": 1, "summary": {"operation": "append"}},
            {"sequence-number": 2, "summary": {"operation": "overwrite"}},
            {"sequence-number": 3, "summary": {"operation": "replace"}},
            {"sequence-number": 4, "summary": {"operation": "append"}},
        ]
    }
    files = [("DATA", "a", 1), ("DATA", "redacted", 2), ("DATA", "merged", 3), ("DATA", "d", 4)]

    assert projections.rewrite_sequences(metadata) == {2, 3}
    assert new_files_by_sequence(files, 0, {2, 3}) == [(1, ["a"]), (4, ["d"])]


def test_position_deletes_from_redaction_are_skipped_but_equality_deletes_stop_the_run():
    files = [("DATA", "a", 1), ("POSITION_DELETES", "x", 2), ("DATA", "b", 2)]
    assert new_files_by_sequence(files, 0) == [(1, ["a"]), (2, ["b"])]
    with pytest.raises(ValueError, match="equality delete files"):
        new_files_by_sequence([("DATA", "a", 1), ("EQUALITY_DELETES", "x", 2)], 0)


def test_creates_become_current_records(con, store):
    store.commit([like("did:a", K1, time_us(10)), follow("did:a", K2, time_us(10)), post("did:b", K3, time_us(10))])
    store.fold(1)

    assert current(con) == [
        ("did:a", "app.bsky.feed.like", K1),
        ("did:a", "app.bsky.graph.follow", K2),
        ("did:b", "app.bsky.feed.post", K3),
    ]
    row = con.execute(
        f"SELECT epoch_us(record_time), subject_did, subject_collection, subject_rkey FROM {SCHEMA}.current_records WHERE rkey = ?",
        [K1],
    ).fetchone()
    assert row == (us(LIKED_AT), "did:plc:author", POST, "3post")


def test_a_delete_hides_the_record_and_keeps_only_its_key(con, store):
    store.commit([like("did:a", K1, time_us(10), rev="3rev1"), like("did:a", K2, time_us(10), rev="3rev1")])
    store.commit([delete("did:a", LIKE, K1, time_us(11), rev="3rev2")])
    store.fold(1, 2)

    assert current(con, LIKE) == [("did:a", LIKE, K2)]
    markers = con.execute(
        f"SELECT did, collection, rkey, rev, epoch_us(record_time), epoch_us(event_time) FROM {SCHEMA}.record_deletes"
    ).fetchall()
    assert markers == [("did:a", LIKE, K1, "3rev2", us(LIKED_AT), time_us(11))]
    assert [c[0] for c in con.execute(f"DESCRIBE {SCHEMA}.record_deletes").fetchall()] == [
        "did", "collection", "rkey", "rev", "record_time", "event_time"
    ]


def test_a_delete_folded_before_its_create_still_wins(con, store):
    store.commit([like("did:a", K1, time_us(10), rev="3rev1")])
    store.commit([delete("did:a", LIKE, K1, time_us(11), rev="3rev2")])
    store.fold(2, 1)

    assert current(con) == []


def test_folding_the_same_commit_twice_changes_nothing(con, store):
    store.commit([like("did:a", K1, time_us(10)), post("did:b", K2, time_us(10))])
    store.commit([delete("did:b", POST, K2, time_us(11))])
    store.fold(1, 2)
    before = current(con)

    store.fold(1, 2, 1)

    assert current(con) == before == [("did:a", LIKE, K1)]
    # Commit 1 has been folded three times: its two records are appended each time, and the view ignores the copies.
    assert con.execute(f"SELECT count(*) FROM {SCHEMA}.records").fetchone()[0] == 6


def test_an_edit_replaces_the_post_whatever_the_order(con, store):
    store.commit([post("did:a", K1, time_us(10), langs=("en",), rev="3rev1")])
    store.commit([edit_post("did:a", K1, time_us(11), langs=("ja",), rev="3rev2")])
    store.fold(2, 1)

    assert con.execute(f"SELECT langs, rev FROM {SCHEMA}.current_records").fetchall() == [(["ja"], "3rev2")]


def test_a_record_created_again_after_a_delete_is_current(con, store):
    # Record keys can be reused: the newer create outranks the delete by rev.
    store.commit([post("did:a", "self", time_us(10), rev="3rev1")])
    store.commit([delete("did:a", POST, "self", time_us(11), rev="3rev2")])
    store.commit([post("did:a", "self", time_us(12), langs=("de",), rev="3rev3")])
    store.fold(3, 2, 1)

    assert con.execute(f"SELECT rkey, rev, langs FROM {SCHEMA}.current_records").fetchall() == [("self", "3rev3", ["de"])]


def test_a_delete_of_a_record_never_seen_is_only_a_marker(con, store):
    store.commit([delete("did:a", LIKE, tid(OLD), time_us(10))])
    store.fold(1)

    assert current(con) == []
    assert con.execute(f"SELECT rkey, epoch_us(record_time) FROM {SCHEMA}.record_deletes").fetchall() == [(tid(OLD), us(OLD))]


def test_other_record_types_and_account_events_are_not_projected(con, store):
    other = like("did:a", K2, time_us(10)).replace(LIKE, "place.stream.livestream")
    store.commit([other, identity("did:a", 5, time_us(10)), account("did:a", 6, time_us(10)), like("did:a", K1, time_us(10))])
    store.fold(1)

    assert current(con) == [("did:a", LIKE, K1)]


def test_project_all_folds_only_new_commits_and_logs_each(con, store, monkeypatch):
    monkeypatch.setattr(projections, "store_files", lambda con, table: store.commits)
    store.commit([like("did:a", K1, time_us(10))])
    store.commit([like("did:a", K2, time_us(10))])

    first = project_all(con, SCHEMA, f"{SCHEMA}.events")
    store.commit([delete("did:a", LIKE, K1, time_us(11))])
    second = project_all(con, SCHEMA, f"{SCHEMA}.events")
    third = project_all(con, SCHEMA, f"{SCHEMA}.events")

    assert [r.sequence for r in first] == [1, 2]
    assert [(r.sequence, r.versions, r.deletes) for r in second] == [(3, 0, 1)]
    assert third == []
    assert last_sequence(con, SCHEMA) == 3
    assert current(con) == [("did:a", LIKE, K2)]
    runs = con.execute(
        f"SELECT from_sequence, to_sequence, files, events_folded, epoch_us(oldest_delete_time) FROM {SCHEMA}.projection_runs"
    ).fetchall()
    assert runs == [(0, 1, 1, 1, None), (1, 2, 1, 1, None), (2, 3, 1, 1, time_us(11))]


def test_accounts_view_has_the_latest_handle_and_status(con, store):
    store.commit([
        identity("did:a", 10, time_us(10), handle="old.bsky.social"),
        identity("did:a", 12, time_us(11), handle="new.bsky.social"),
        account("did:a", 11, time_us(10), active=True),
        account("did:a", 13, time_us(12), active=False, status="deactivated"),
        identity("did:b", 14, time_us(10), handle="b.bsky.social"),
    ])

    rows = con.execute(f"SELECT did, handle, active, status FROM {SCHEMA}.accounts ORDER BY did").fetchall()
    assert rows == [("did:a", "new.bsky.social", False, "deactivated"), ("did:b", "b.bsky.social", None, None)]
