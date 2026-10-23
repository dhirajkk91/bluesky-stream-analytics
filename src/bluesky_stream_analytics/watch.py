"""Live view of the Jetstream feed: prints a count of each event type every 10 seconds. Nothing is saved."""

import argparse
import asyncio
import json
import time
from collections import Counter

from websockets.asyncio.client import connect

JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
SUMMARY_INTERVAL_SECONDS = 10


def describe(event: dict) -> str:
    """
    Label an event, e.g. "like create", "post delete" or "identity".

    Bluesky's own record types are shortened ("app.bsky.feed.like" -> "like");
    other apps' record types keep their full name.
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
    """Format one interval's counts as a single line: total, rate and the top five labels."""
    total = sum(counts.values())
    top = ", ".join(f"{label} {n:,}" for label, n in counts.most_common(5))
    return f"{seconds:.1f}s | {total:,} events | {total / seconds:,.1f}/s | {top}"


async def watch(show_events: bool) -> None:
    """Connect to Jetstream and print a summary every SUMMARY_INTERVAL_SECONDS."""
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
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Stream live Bluesky events from Jetstream.")
    parser.add_argument("--show", action="store_true", help="print a label for every event")
    args = parser.parse_args()

    try:
        asyncio.run(watch(show_events=args.show))
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
