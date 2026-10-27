"""Builders for Jetstream events shaped like the real feed, and a helper to write them as a finished hourly file."""

import json
from datetime import UTC, datetime
from pathlib import Path

from compression import zstd

TID_ALPHABET = "234567abcdefghijklmnopqrstuvwxyz"


def time_us(hour: int, minute: int = 0, second: int = 0, micro: int = 0) -> int:
    return int(datetime(2026, 10, 24, hour, minute, second, tzinfo=UTC).timestamp()) * 1_000_000 + micro


def _commit(did, ts, collection, operation, rkey, rev, record=None, cid=None):
    commit = {"rev": rev, "operation": operation, "collection": collection, "rkey": rkey}
    if record is not None:
        commit["record"] = {"$type": collection, "createdAt": "2026-10-24T10:00:00.000Z", **record}
        commit["cid"] = cid or f"bafyrei{rkey}"
    return json.dumps({"did": did, "time_us": ts, "kind": "commit", "commit": commit})


def like(did, rkey, ts, post_author="did:plc:author", post_rkey="3post", rev="3rev1"):
    subject = {"cid": "bafyreipost", "uri": f"at://{post_author}/app.bsky.feed.post/{post_rkey}"}
    return _commit(did, ts, "app.bsky.feed.like", "create", rkey, rev, {"subject": subject})


def repost(did, rkey, ts, post_author="did:plc:author", post_rkey="3post", rev="3rev1"):
    subject = {"cid": "bafyreipost", "uri": f"at://{post_author}/app.bsky.feed.post/{post_rkey}"}
    return _commit(did, ts, "app.bsky.feed.repost", "create", rkey, rev, {"subject": subject})


def follow(did, rkey, ts, subject="did:plc:followed", rev="3rev1"):
    return _commit(did, ts, "app.bsky.graph.follow", "create", rkey, rev, {"subject": subject})


def post(did, rkey, ts, text="hello", langs=("en",), reply_to=None, rev="3rev1"):
    record = {"text": text, "langs": list(langs)}
    if reply_to:
        record["reply"] = {
            "parent": {"cid": "bafyreiparent", "uri": reply_to},
            "root": {"cid": "bafyreiroot", "uri": reply_to},
        }
    return _commit(did, ts, "app.bsky.feed.post", "create", rkey, rev, record)


def edit_post(did, rkey, ts, text="edited", langs=("en",), rev="3rev2"):
    return _commit(did, ts, "app.bsky.feed.post", "update", rkey, rev, {"text": text, "langs": list(langs)})


def profile(did, ts, rev="3rev1"):
    return _commit(did, ts, "app.bsky.actor.profile", "create", "self", rev, {"displayName": "Someone"})


def delete(did, collection, rkey, ts, rev="3rev2"):
    return _commit(did, ts, collection, "delete", rkey, rev)


def identity(did, seq, ts, handle="someone.bsky.social"):
    body = {"did": did, "handle": handle, "seq": seq, "time": "2026-10-24T10:00:00.000Z"}
    return json.dumps({"did": did, "time_us": ts, "kind": "identity", "identity": body})


def account(did, seq, ts, active=True, status=None):
    body = {"active": active, "did": did, "seq": seq, "time": "2026-10-24T10:00:00.000Z"}
    if status:
        body["status"] = status
    return json.dumps({"did": did, "time_us": ts, "kind": "account", "account": body})


def write_hour_file(data_dir: Path, hour: int, name: str, lines: list[str], folder: str = "uploaded") -> Path:
    """Write lines as a finished .jsonl.zst file, where the uploader leaves it for processing."""
    path = data_dir / folder / "events" / "date=2026-10-24" / f"hour={hour:02d}" / f"{name}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(zstd.compress("".join(line + "\n" for line in lines).encode()))
    return path


def tid(when: datetime, clock: int = 0) -> str:
    """Encode a time as a TID record key: microseconds shifted past a 10-bit clock id, base32-sortable."""
    n = (int(when.timestamp() * 1_000_000) << 10) | clock
    return "".join(TID_ALPHABET[(n >> (5 * i)) & 31] for i in reversed(range(13)))


def at_us(when: datetime) -> int:
    return int(when.timestamp() * 1_000_000)
