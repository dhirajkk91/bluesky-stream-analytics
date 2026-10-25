from datetime import date

from bluesky_stream_analytics.fetch_raw import fetch, local_path, raw_prefix

BUCKET = "test-bucket"


class FakeS3:
    """In-memory stand-in for listing and downloading objects."""

    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects
        self.downloads: list[str] = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        assert Bucket == BUCKET
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        # Two pages, to exercise pagination.
        half = len(keys) // 2
        for chunk in (keys[:half], keys[half:]):
            yield {"Contents": [{"Key": k, "Size": len(self.objects[k])} for k in chunk]}

    def download_file(self, bucket, key, filename):
        self.downloads.append(key)
        with open(filename, "wb") as f:
            f.write(self.objects[key])


def test_raw_prefix_for_a_day_and_an_hour():
    assert raw_prefix(date(2026, 10, 24)) == "raw/events/date=2026-10-24/"
    assert raw_prefix(date(2026, 10, 24), 7) == "raw/events/date=2026-10-24/hour=07/"


def test_local_path_mirrors_the_key_under_uploaded(tmp_path):
    key = "raw/events/date=2026-10-24/hour=07/a.jsonl.zst"
    assert local_path(tmp_path, key) == tmp_path / "uploaded/events/date=2026-10-24/hour=07/a.jsonl.zst"


def test_fetch_downloads_finished_files_under_the_prefix(tmp_path):
    s3 = FakeS3(
        {
            "raw/events/date=2026-10-24/hour=07/a.jsonl.zst": b"a",
            "raw/events/date=2026-10-24/hour=08/b.jsonl.zst": b"b",
            "raw/events/date=2026-10-24/hour=08/notes.txt": b"x",
            "raw/events/date=2026-10-25/hour=00/c.jsonl.zst": b"c",
        }
    )

    assert fetch(s3, BUCKET, tmp_path, raw_prefix(date(2026, 10, 24))) == 2
    assert local_path(tmp_path, "raw/events/date=2026-10-24/hour=08/b.jsonl.zst").read_bytes() == b"b"
    assert not list(tmp_path.rglob("*.part"))


def test_fetch_skips_files_already_waiting(tmp_path):
    key = "raw/events/date=2026-10-24/hour=07/a.jsonl.zst"
    existing = local_path(tmp_path, key)
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"already here")
    s3 = FakeS3({key: b"from s3"})

    assert fetch(s3, BUCKET, tmp_path, raw_prefix(date(2026, 10, 24))) == 0
    assert s3.downloads == []
    assert existing.read_bytes() == b"already here"
