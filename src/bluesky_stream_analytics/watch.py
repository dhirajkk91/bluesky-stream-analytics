"""Stream live events from Bluesky's Jetstream and print periodic summaries.

Jetstream is a public WebSocket service that pushes every public action on the
Bluesky network (posts, likes, reposts, follows, profile changes, deletions) as
one JSON message per event. This module opens a single connection, labels each
event as it arrives, and every SUMMARY_INTERVAL_SECONDS prints how many events
of each type were seen in that interval.

Nothing is stored; this is a live view of the stream.

Usage:
    uv run watch           # print a summary every 10 seconds
    uv run watch --show    # also print a label for every event

Example output:
    Connected to wss://jetstream2.us-east.bsky.network/subscribe
    10.0s | 1,234 events | 123.4/s | like create 600, post create 200, ...
"""

import argparse
import asyncio
import json
import time
from collections import Counter

from websockets.asyncio.client import connect

JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
SUMMARY_INTERVAL_SECONDS = 10


def describe(event: dict) -> str:
    """Return a short, human-readable label for a Jetstream event.

    Commit events (a record was created, updated or deleted) are labelled with
    the record type and the operation. Identity and account events are
    labelled with their kind.

    Bluesky's own record types (collections under "app.bsky.") are shortened
    to their last segment, e.g. "app.bsky.feed.like" becomes "like". Record
    types from other apps on the network keep their full name, because their
    last segment alone (e.g. "vote" or "entry") would be ambiguous.

    Args:
        event: One decoded Jetstream message.

    Returns:
        A label such as:
            "like create"      a new like
            "post delete"      a deleted post
            "profile update"   an edited profile
            "identity"         a handle or identity change
            "account"          an account status change
    """
    kind = event.get("kind", "unknown")
    if kind != "commit":
        return kind

    commit = event["commit"]
    collection = commit["collection"]
    if collection.startswith("app.bsky."):
        collection = collection.rsplit(".", 1)[-1]
    return f"{collection} {commit['operation']}"


def summarize(counts: Counter[str], seconds: float) -> str:
    """Format the event counts for one interval as a single summary line.

    Args:
        counts: Number of events seen per label during the interval.
        seconds: Length of the interval in seconds.

    Returns:
        The interval length, total events, events per second, and the five
        most common labels, for example:
            "10.0s | 1,234 events | 123.4/s | like create 600, post create 200, ..."
    """
    total = sum(counts.values())
    top = ", ".join(f"{label} {n:,}" for label, n in counts.most_common(5))
    return f"{seconds:.1f}s | {total:,} events | {total / seconds:,.1f}/s | {top}"


async def watch(show_events: bool) -> None:
    """Connect to Jetstream and print a summary line every interval.

    Runs until the connection closes or the process is interrupted. Counts are
    reset after each summary, so every line describes only its own interval.
    time.monotonic() is used for timing because, unlike the wall clock, it
    never jumps if the system time is adjusted.

    Args:
        show_events: If True, also print the label of every event as it arrives.
    """
    counts: Counter[str] = Counter()
    window_start = time.monotonic()

    async with connect(JETSTREAM_URL) as ws:
        print(f"Connected to {JETSTREAM_URL}")

        async for message in ws:
            label = describe(json.loads(message))
            counts[label] += 1
            if show_events:
                print(label)

            elapsed = time.monotonic() - window_start
            if elapsed >= SUMMARY_INTERVAL_SECONDS:
                print(summarize(counts, elapsed))
                counts.clear()
                window_start = time.monotonic()


def main() -> None:
    """Command-line entry point. Stops cleanly on Ctrl+C."""
    parser = argparse.ArgumentParser(description="Stream live Bluesky events from Jetstream.")
    parser.add_argument("--show", action="store_true", help="print a label for every event")
    args = parser.parse_args()

    try:
        asyncio.run(watch(show_events=args.show))
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
