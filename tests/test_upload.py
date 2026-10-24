import base64
import hashlib

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import ANY, Stubber

from bluesky_stream_analytics.upload import (
    finished_files,
    object_key,
    sha256_base64,
    upload_all,
    upload_file,
)

BUCKET = "test-bucket"


class FakeS3:
    """In-memory stand-in for the S3 client: verifies checksums and honours IfNoneMatch."""

    def __init__(self, fail_keys=()):
        self.objects: dict[str, bytes] = {}
        self.fail_keys = set(fail_keys)

    def put_object(self, *, Bucket, Key, Body, ChecksumSHA256, IfNoneMatch):
        assert Bucket == BUCKET and IfNoneMatch == "*"
        if Key in self.fail_keys:
            raise _client_error("AccessDenied", 403)
        data = Body.read()
        if base64.b64encode(hashlib.sha256(data).digest()).decode() != ChecksumSHA256:
            raise _client_error("BadDigest", 400)
        if Key in self.objects:
            raise _client_error("PreconditionFailed", 412)
        self.objects[Key] = data


def _client_error(code: str, status: int) -> ClientError:
    return ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, "PutObject")


def make_file(data_dir, name, content=b'{"time_us":1}\n', hour="hour=16"):
    path = data_dir / "events" / "date=2026-10-24" / hour / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_finished_files_skips_open_and_temporary_files(tmp_path):
    done = make_file(tmp_path, "a.jsonl.zst")
    make_file(tmp_path, "b.jsonl.open")
    make_file(tmp_path, "c.jsonl.zst.tmp")
    assert finished_files(tmp_path) == [done]


def test_finished_files_with_no_data_yet(tmp_path):
    assert finished_files(tmp_path) == []


def test_object_key_keeps_partition_path(tmp_path):
    path = make_file(tmp_path, "a.jsonl.zst")
    assert object_key(tmp_path, path) == "raw/events/date=2026-10-24/hour=16/a.jsonl.zst"


def test_sha256_base64(tmp_path):
    path = tmp_path / "f"
    path.write_bytes(b"abc")
    assert sha256_base64(path) == "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="


def test_upload_stores_exact_bytes_and_deletes_local_copy(tmp_path):
    s3 = FakeS3()
    path = make_file(tmp_path, "a.jsonl.zst", b"raw bytes")

    assert upload_file(s3, BUCKET, tmp_path, path) is True
    assert s3.objects == {"raw/events/date=2026-10-24/hour=16/a.jsonl.zst": b"raw bytes"}
    assert not path.exists()


def test_existing_object_is_not_overwritten_and_local_copy_is_removed(tmp_path):
    s3 = FakeS3()
    key = "raw/events/date=2026-10-24/hour=16/a.jsonl.zst"
    s3.objects[key] = b"uploaded earlier"
    path = make_file(tmp_path, "a.jsonl.zst", b"uploaded earlier")

    assert upload_file(s3, BUCKET, tmp_path, path) is False
    assert s3.objects[key] == b"uploaded earlier"
    assert not path.exists()


def test_failed_upload_keeps_local_copy(tmp_path):
    path = make_file(tmp_path, "a.jsonl.zst")
    s3 = FakeS3(fail_keys={object_key(tmp_path, path)})

    with pytest.raises(ClientError):
        upload_file(s3, BUCKET, tmp_path, path)
    assert path.exists()


def test_upload_all_continues_past_failures_and_counts_them(tmp_path):
    ok_1 = make_file(tmp_path, "a.jsonl.zst", hour="hour=15")
    bad = make_file(tmp_path, "b.jsonl.zst", hour="hour=16")
    ok_2 = make_file(tmp_path, "c.jsonl.zst", hour="hour=17")
    s3 = FakeS3(fail_keys={object_key(tmp_path, bad)})

    assert upload_all(s3, BUCKET, tmp_path) == 1
    assert len(s3.objects) == 2
    assert bad.exists() and not ok_1.exists() and not ok_2.exists()


def test_request_is_valid_for_the_real_s3_api(tmp_path):
    """Checks the parameter names and values against botocore's S3 model, without a network call."""
    path = make_file(tmp_path, "a.jsonl.zst", b"abc")
    client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x")

    with Stubber(client) as stub:
        stub.add_response(
            "put_object",
            {},
            {
                "Bucket": BUCKET,
                "Key": "raw/events/date=2026-10-24/hour=16/a.jsonl.zst",
                "Body": ANY,
                "ChecksumSHA256": "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=",
                "IfNoneMatch": "*",
            },
        )
        assert upload_file(client, BUCKET, tmp_path, path) is True


def test_real_client_412_is_treated_as_already_uploaded(tmp_path):
    path = make_file(tmp_path, "a.jsonl.zst")
    client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x")

    with Stubber(client) as stub:
        stub.add_client_error("put_object", service_error_code="PreconditionFailed", http_status_code=412)
        assert upload_file(client, BUCKET, tmp_path, path) is False
    assert not path.exists()
