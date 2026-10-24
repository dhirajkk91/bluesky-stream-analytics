"""End-to-end tests of the collector against a local fake Jetstream server."""

import asyncio
import json
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import pytest
from compression import zstd
from websockets.asyncio.server import serve

from bluesky_stream_analytics import collect
from bluesky_stream_analytics.collect import Collector, backoff_delay, build_url
from bluesky_stream_analytics.cursor import load_cursor

# 1,000 events, one every 10 ms, spanning the 13:00 -> 14:00 UTC boundary.
START_US = int(datetime(2026, 10, 23, 13, 59, 55, tzinfo=UTC).timestamp() * 1_000_000)
EVENTS = [
    json.dumps({"did": f"did:plc:{i}", "time_us": START_US + i * 10_000, "kind": "commit"})
    for i in range(1_000)
]
LAST_US = START_US + 999 * 10_000


class FakeJetstream:
    def __init__(self, disconnect_after: int | None = None) -> None:
        self.disconnect_after = disconnect_after
        self.cursors_requested: list[int | None] = []

    async def handler(self, ws) -> None:
        query = parse_qs(urlparse(ws.request.path).query)
        cursor = int(query["cursor"][0]) if "cursor" in query else None
        self.cursors_requested.append(cursor)

        sent = 0
        for raw in EVENTS:
            if cursor is not None and json.loads(raw)["time_us"] < cursor:
                continue
            await ws.send(raw)
            sent += 1
            if self.disconnect_after is not None and sent == self.disconnect_after:
                self.disconnect_after = None  # only drop the first connection
                return
        await ws.wait_closed()  # stay connected, like a quiet live stream


@pytest.fixture(autouse=True)
def fast_timings(monkeypatch):
    monkeypatch.setattr(collect, "SYNC_INTERVAL_SECONDS", 0.05)
    monkeypatch.setattr(collect, "backoff_delay", lambda attempt: 0.05)


async def run_until(collector: Collector, time_us: int) -> None:
    """Run the collector until it has written time_us, then stop it."""
    task = asyncio.create_task(collector.run())
    while (collector.writer.last_written_time_us or 0) < time_us:
        assert not task.done(), task.exception()
        await asyncio.sleep(0.01)
    collector.stop()
    await asyncio.wait_for(task, timeout=5)


def collected_time_us(data_dir) -> list[int]:
    result = []
    for path in sorted((data_dir / "events").rglob("*.jsonl.zst")):
        with zstd.open(path, "rt") as f:
            result.extend(json.loads(line)["time_us"] for line in f)
    return result


async def serve_fake(fake: FakeJetstream):
    return await serve(fake.handler, "127.0.0.1", 0)


def url_of(server) -> str:
    port = server.sockets[0].getsockname()[1]
    return f"ws://127.0.0.1:{port}/subscribe"


def test_build_url_rewinds_from_the_cursor():
    assert build_url("wss://x/subscribe", None) == "wss://x/subscribe"
    assert build_url("wss://x/subscribe", 10_000_000) == "wss://x/subscribe?cursor=5000000"


@pytest.mark.parametrize("attempt", range(10))
def test_backoff_delay_stays_within_bounds(attempt):
    ceiling = min(collect.MAX_BACKOFF_SECONDS, 2**attempt)
    assert ceiling / 2 <= backoff_delay(attempt) <= ceiling


def test_collects_every_event_and_saves_cursor(tmp_path):
    async def scenario():
        fake = FakeJetstream()
        async with await serve_fake(fake) as server:
            await run_until(Collector(tmp_path, url_of(server)), LAST_US)

    asyncio.run(scenario())

    expected = [json.loads(raw)["time_us"] for raw in EVENTS]
    assert collected_time_us(tmp_path) == expected
    assert load_cursor(tmp_path / "state" / "cursor.json") == LAST_US
    hours = sorted({p.parent.name for p in (tmp_path / "events").rglob("*.jsonl.zst")})
    assert hours == ["hour=13", "hour=14"]


def test_reconnects_after_a_dropped_connection_without_gaps(tmp_path):
    async def scenario():
        fake = FakeJetstream(disconnect_after=300)
        async with await serve_fake(fake) as server:
            await run_until(Collector(tmp_path, url_of(server)), LAST_US)
        return fake

    fake = asyncio.run(scenario())

    collected = collected_time_us(tmp_path)
    assert set(collected) == {json.loads(raw)["time_us"] for raw in EVENTS}
    assert len(collected) > len(EVENTS)  # the rewind replays a few events
    third_hundredth_event_us = START_US + 299 * 10_000
    assert fake.cursors_requested == [None, third_hundredth_event_us - collect.REWIND_SECONDS * 1_000_000]


def test_restart_resumes_from_saved_cursor_with_rewind(tmp_path):
    first_stop_us = START_US + 499 * 10_000

    async def scenario():
        fake = FakeJetstream()
        async with await serve_fake(fake) as server:
            await run_until(Collector(tmp_path, url_of(server)), first_stop_us)
            saved = load_cursor(tmp_path / "state" / "cursor.json")
            await run_until(Collector(tmp_path, url_of(server)), LAST_US)
        return fake, saved

    fake, saved = asyncio.run(scenario())

    assert saved >= first_stop_us
    assert fake.cursors_requested[1] == saved - collect.REWIND_SECONDS * 1_000_000
    collected = collected_time_us(tmp_path)
    assert set(collected) == {json.loads(raw)["time_us"] for raw in EVENTS}


def test_resumes_from_recovered_data_when_newer_than_cursor(tmp_path):
    from bluesky_stream_analytics.cursor import save_cursor
    from bluesky_stream_analytics.writer import HourlyFileWriter

    # A previous run wrote 200 events but crashed before saving a cursor past event 50.
    writer = HourlyFileWriter(tmp_path)
    for raw in EVENTS[:200]:
        writer.write(raw, json.loads(raw)["time_us"])
    writer.sync()
    save_cursor(tmp_path / "state" / "cursor.json", START_US + 50 * 10_000)

    async def scenario():
        fake = FakeJetstream()
        async with await serve_fake(fake) as server:
            await run_until(Collector(tmp_path, url_of(server)), LAST_US)
        return fake

    fake = asyncio.run(scenario())

    newest_recovered = START_US + 199 * 10_000
    assert fake.cursors_requested == [newest_recovered - collect.REWIND_SECONDS * 1_000_000]
    assert set(collected_time_us(tmp_path)) == {json.loads(raw)["time_us"] for raw in EVENTS}


def test_failed_compression_stops_the_collector_and_saves_the_cursor(tmp_path, monkeypatch):
    from bluesky_stream_analytics import writer as writer_module

    def broken_finalize(path):
        raise OSError("disk full")

    monkeypatch.setattr(writer_module, "finalize_open_file", broken_finalize)

    async def scenario():
        fake = FakeJetstream()
        async with await serve_fake(fake) as server:
            collector = Collector(tmp_path, url_of(server))
            with pytest.raises(OSError, match="disk full"):
                await asyncio.wait_for(collector.run(), timeout=5)
            return collector

    collector = asyncio.run(scenario())

    saved = load_cursor(tmp_path / "state" / "cursor.json")
    assert saved == collector.writer.last_synced_time_us
    assert saved >= START_US + 500 * 10_000  # past the hour boundary
    assert list(tmp_path.rglob("*.jsonl.open"))  # left for recovery on the next start
