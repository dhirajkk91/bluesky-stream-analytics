"""Tests of the handle lookups: batching, retries and pacing against a fake API, and the incremental run against local files."""

import email.message
import urllib.error
import urllib.parse

import pytest

from sample_events import identity, like, time_us, write_hour_file

from bluesky_stream_analytics import handles
from bluesky_stream_analytics.event_store import COLUMNS, stage_batch
from bluesky_stream_analytics.handles import HandleLookup, LookupFailed, create_tables, last_sequence, resolve_all
from bluesky_stream_analytics.process import connect

SCHEMA = "bluesky_store"


class FakeApi:
    """Answers getProfiles from a dict of known handles; can fail on chosen calls."""

    def __init__(self, known: dict[str, str], failures: dict[int, urllib.error.HTTPError] | None = None):
        self.known = known
        self.failures = failures or {}
        self.requests: list[list[str]] = []

    def __call__(self, url: str) -> dict:
        actors = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["actors"]
        self.requests.append(actors)
        if len(self.requests) in self.failures:
            raise self.failures[len(self.requests)]
        if any(not a.startswith("did:") for a in actors):
            raise http_error(400)
        return {"profiles": [{"did": a, "handle": self.known[a]} for a in actors if a in self.known]}


def http_error(code: int, retry_after: str | None = None) -> urllib.error.HTTPError:
    headers = email.message.Message()
    if retry_after:
        headers["Retry-After"] = retry_after
    return urllib.error.HTTPError("https://api", code, "error", headers, None)


def lookup_with(api: FakeApi) -> tuple[HandleLookup, list[float]]:
    sleeps: list[float] = []
    return HandleLookup(fetch=api, min_interval=0, sleep=sleeps.append), sleeps


def test_accounts_are_looked_up_25_at_a_time():
    dids = [f"did:plc:{i}" for i in range(60)]
    api = FakeApi({d: f"h{i}.bsky.social" for i, d in enumerate(dids)})
    lookup, _ = lookup_with(api)

    result = lookup.lookup(dids + dids[:5])  # repeats are looked up once

    assert [len(r) for r in api.requests] == [25, 25, 10]
    assert result["did:plc:59"] == "h59.bsky.social"
    assert lookup.calls == 3


def test_accounts_the_api_doesnt_return_have_no_handle():
    lookup, _ = lookup_with(FakeApi({"did:plc:a": "a.bsky.social"}))
    assert lookup.lookup(["did:plc:a", "did:plc:gone"]) == {"did:plc:a": "a.bsky.social", "did:plc:gone": None}


def test_rate_limits_and_server_errors_are_retried_after_the_wait_asked_for():
    api = FakeApi({"did:plc:a": "a.bsky.social"}, {1: http_error(429, "7"), 2: http_error(503)})
    lookup, sleeps = lookup_with(api)

    assert lookup.lookup(["did:plc:a"]) == {"did:plc:a": "a.bsky.social"}
    assert sleeps == [7.0, 2.0]


def test_a_lookup_that_keeps_failing_stops_the_run():
    api = FakeApi({}, {n: http_error(502) for n in range(1, 10)})
    lookup, _ = lookup_with(api)
    with pytest.raises(LookupFailed):
        lookup.lookup(["did:plc:a"])


def test_a_bad_account_id_doesnt_lose_the_rest_of_its_batch():
    lookup, _ = lookup_with(FakeApi({"did:plc:a": "a.bsky.social"}))
    assert lookup.lookup(["did:plc:a", "not-a-did"]) == {"did:plc:a": "a.bsky.social", "not-a-did": None}


def test_calls_are_spaced_out():
    clock = iter([0.0, 0.0, 0.1, 0.5, 0.6, 1.0])
    sleeps: list[float] = []
    lookup = HandleLookup(fetch=FakeApi({}), min_interval=0.5, sleep=sleeps.append, clock=lambda: next(clock))

    lookup.lookup([f"did:plc:{i}" for i in range(75)])

    assert sleeps == [pytest.approx(0.4), pytest.approx(0.4)]


@pytest.fixture
def con():
    con = connect(threads=2, memory_limit="1GB", extension_dir=None)
    con.execute(f"CREATE SCHEMA {SCHEMA}")
    con.execute(f"CREATE TABLE {SCHEMA}.events ({COLUMNS})")
    create_tables(con, SCHEMA)
    yield con
    con.close()


def store_commit(con, tmp_path, commits: list, lines: list[str]) -> None:
    sequence = len(commits) + 1
    raw = write_hour_file(tmp_path / "raw", 10, f"c{sequence}", lines)
    stage_batch(con, [str(raw)])
    path = tmp_path / f"store-{sequence}.parquet"
    con.execute(f"COPY batch TO '{path}' (FORMAT parquet)")
    con.execute("DROP TABLE batch")
    commits.append(("DATA", str(path), sequence))


def test_each_new_commit_is_looked_up_once_with_the_newest_identity_event_per_account(con, tmp_path, monkeypatch):
    commits: list = []
    monkeypatch.setattr(handles, "store_files", lambda con, table: commits)
    api = FakeApi({"did:plc:a": "new.bsky.social"})
    lookup, _ = lookup_with(api)

    store_commit(con, tmp_path, commits, [
        identity("did:plc:a", 10, time_us(10), handle=None),
        identity("did:plc:a", 12, time_us(11), handle=None),
        identity("did:plc:gone", 11, time_us(10), handle=None),
        like("did:plc:a", "3k", time_us(10)),
    ])
    first = resolve_all(con, SCHEMA, f"{SCHEMA}.events", lookup)
    store_commit(con, tmp_path, commits, [like("did:plc:b", "3l", time_us(12))])
    second = resolve_all(con, SCHEMA, f"{SCHEMA}.events", lookup)
    third = resolve_all(con, SCHEMA, f"{SCHEMA}.events", lookup)

    assert [(r.identity_events, r.accounts, r.found, r.api_calls) for r in first] == [(3, 2, 1, 1)]
    assert [(r.identity_events, r.accounts, r.api_calls) for r in second] == [(0, 0, 0)]
    assert third == []
    assert last_sequence(con, SCHEMA) == 2
    rows = con.execute(
        f"SELECT did, epoch_us(identity_time), identity_seq, handle, found FROM {SCHEMA}.account_handles ORDER BY did"
    ).fetchall()
    assert rows == [("did:plc:a", time_us(11), 12, "new.bsky.social", True), ("did:plc:gone", time_us(10), 11, None, False)]
