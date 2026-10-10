"""Failure-injection tests for the ingest _SUCCESS marker boundary.

Uses an in-memory fake S3 client (no moto/network/MinIO) to drive the real
ingestion/kaggle_ingest.py logic. Proves the marker is written last and that an
upload crash leaves no marker, while a forced re-run overwrites it.
"""

import sys
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from ingestion import kaggle_ingest

MARKER_KEY = "ecommerce_events/_SUCCESS"
CSV_KEY = "ecommerce_events/ingested_date={}/events.csv"


class FakeS3Client:
    """Minimal in-memory stand-in for a boto3 S3 client."""

    def __init__(self):
        self.objects = {}
        self.upload_calls = 0
        self.put_calls = 0

    def list_buckets(self):
        return {"Buckets": [{"Name": "raw"}]}

    def head_object(self, Bucket, Key):
        if Key in self.objects:
            return {"Key": Key}
        raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")

    def upload_file(self, Filename, Bucket, Key):
        self.objects[Key] = Path(Filename).read_bytes()
        self.upload_calls += 1

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[Key] = Body
        self.put_calls += 1
        return {"Key": Key}


def _uploaded_csv_keys(client):
    return [key for key in client.objects if key.endswith("events.csv")]


@pytest.fixture()
def fake_client(monkeypatch):
    client = FakeS3Client()
    monkeypatch.setattr(kaggle_ingest, "get_minio_client", lambda: client)
    monkeypatch.setattr(kaggle_ingest, "test_minio_connection", lambda c, log: True)
    monkeypatch.delenv("FAIL_AFTER_INGEST_UPLOAD", raising=False)
    return client


@pytest.fixture()
def data_dir(tmp_path):
    (tmp_path / "events.csv").write_text("event_id\ne1\n", encoding="utf-8")
    return tmp_path


def _run_main(monkeypatch, data_dir, force=True):
    argv = ["kaggle_ingest.py", "--data-dir", str(data_dir)]
    if force:
        argv.append("--force")
    monkeypatch.setattr(sys, "argv", argv)
    kaggle_ingest.main()


def test_marker_helper_reports_absent_then_present(fake_client):
    absent, path = kaggle_ingest.check_already_ingested_minio(fake_client, logger=None)
    assert absent is False
    assert path.endswith(MARKER_KEY)

    fake_client.objects[MARKER_KEY] = b"{}"
    present, _ = kaggle_ingest.check_already_ingested_minio(fake_client, logger=None)
    assert present is True


def test_marker_is_written_after_upload(fake_client, data_dir, monkeypatch):
    _run_main(monkeypatch, data_dir)

    assert _uploaded_csv_keys(fake_client), "expected CSV objects to be uploaded"
    assert MARKER_KEY in fake_client.objects


def test_crash_before_marker_leaves_no_marker_then_recovery_writes_it(
    fake_client, data_dir, monkeypatch
):
    monkeypatch.setenv("FAIL_AFTER_INGEST_UPLOAD", "true")

    with pytest.raises(RuntimeError):
        _run_main(monkeypatch, data_dir)

    assert _uploaded_csv_keys(fake_client), "upload should have completed before the crash"
    assert MARKER_KEY not in fake_client.objects, "marker must not exist after a pre-marker crash"

    monkeypatch.delenv("FAIL_AFTER_INGEST_UPLOAD")
    _run_main(monkeypatch, data_dir)

    assert MARKER_KEY in fake_client.objects
    assert fake_client.put_calls == 1, "marker should be written exactly once on recovery"


def test_existing_marker_skips_reen_upload_without_force(fake_client, data_dir, monkeypatch):
    _run_main(monkeypatch, data_dir)
    uploads_after_first = fake_client.upload_calls

    _run_main(monkeypatch, data_dir, force=False)

    assert fake_client.upload_calls == uploads_after_first, "no re-upload when marker exists and --force is absent"
    assert MARKER_KEY in fake_client.objects


def test_force_reupload_overwrites_marker(fake_client, data_dir, monkeypatch):
    _run_main(monkeypatch, data_dir)
    uploads_after_first = fake_client.upload_calls
    puts_after_first = fake_client.put_calls

    _run_main(monkeypatch, data_dir, force=True)

    assert fake_client.upload_calls == uploads_after_first + 1
    assert fake_client.put_calls == puts_after_first + 1
    assert list(fake_client.objects).count(MARKER_KEY) == 1
