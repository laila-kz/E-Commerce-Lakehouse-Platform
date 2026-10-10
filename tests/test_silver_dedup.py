import logging

import pytest

from spark_jobs.silver_transform import (
    ensure_silver_table_exists,
    merge_into_silver,
    verify_merge_success,
    within_batch_dedup,
)


SILVER_COLUMNS = [
    "event_key",
    "user_id",
    "event_type",
    "product_id",
    "user_session",
    "event_time",
    "event_date",
]


def _silver_row(event_key, event_date="2023-01-02"):
    return (event_key, 1, "view", 100, "sess-x", "2023-01-02 00:00:00", event_date)


def _dedup_df(spark, rows):
    return spark.createDataFrame(
        rows,
        ["ingested_at", "source_file", "event_key"],
    )


@pytest.fixture()
def logger():
    log = logging.getLogger("test_silver_dedup")
    log.propagate = False
    log.setLevel(logging.INFO)
    if not log.handlers:
        log.addHandler(logging.NullHandler())
    return log


def test_within_batch_dedup_keeps_earliest_ingest_then_source(spark, logger):
    df = _dedup_df(
        spark,
        [
            (5, "b.parquet", "k1"),
            (3, "z.parquet", "k1"),
            (3, "a.parquet", "k1"),
        ],
    )

    rows = within_batch_dedup(df, logger).collect()

    assert len(rows) == 1
    assert rows[0]["event_key"] == "k1"
    assert rows[0]["ingested_at"] == 3
    assert rows[0]["source_file"] == "a.parquet"


def test_replay_of_same_batch_inserts_zero_rows(spark, logger, tmp_path):
    silver_path = str(tmp_path / "silver")
    target = spark.createDataFrame([_silver_row("k1")], SILVER_COLUMNS)
    target.write.format("delta").mode("overwrite").partitionBy("event_date").save(silver_path)

    replay = spark.createDataFrame([_silver_row("k1")], SILVER_COLUMNS)

    expected_new_keys = merge_into_silver(spark, silver_path, replay, logger)
    assert expected_new_keys == 0

    verification = verify_merge_success(spark, silver_path, expected_new_keys, logger)
    assert verification["operation"] == "MERGE"
    assert verification["verified_metric"] == 0
    assert verification["verified"] is True


def test_first_run_empty_target_writes_deduped_count(spark, logger, tmp_path):
    silver_path = str(tmp_path / "silver")
    raw = spark.createDataFrame(
        [
            (10, "b.parquet", "k1", "2023-01-02"),
            (10, "a.parquet", "k1", "2023-01-02"),
            (11, "c.parquet", "k2", "2023-01-02"),
        ],
        ["ingested_at", "source_file", "event_key", "event_date"],
    )

    deduped = within_batch_dedup(raw, logger)
    expected = deduped.count()
    assert expected == 2

    table_exists, created_new = ensure_silver_table_exists(spark, silver_path, deduped, logger)
    assert created_new is True

    assert spark.read.format("delta").load(silver_path).count() == expected

    verification = verify_merge_success(spark, silver_path, expected, logger)
    assert verification["operation"] in ["WRITE", "CREATE TABLE AS SELECT"]
    assert verification["verified"] is True


def test_verify_reports_failure_on_insert_count_mismatch(spark, logger, tmp_path):
    silver_path = str(tmp_path / "silver")
    target = spark.createDataFrame([_silver_row("k1")], SILVER_COLUMNS)
    target.write.format("delta").mode("overwrite").partitionBy("event_date").save(silver_path)

    source = spark.createDataFrame([_silver_row("k2")], SILVER_COLUMNS)

    expected = merge_into_silver(spark, silver_path, source, logger)
    assert expected == 1

    matched = verify_merge_success(spark, silver_path, expected, logger)
    assert matched["verified"] is True

    mismatched = verify_merge_success(spark, silver_path, expected + 1, logger)
    assert mismatched["verified"] is False
    assert mismatched["verified_metric"] != mismatched["expected_rows"]


def test_within_batch_dedup_null_ordering_prefers_null(spark, logger):
    df = _dedup_df(
        spark,
        [
            (5, "b.parquet", "k1"),
            (None, "a.parquet", "k1"),
        ],
    )

    rows = within_batch_dedup(df, logger).collect()

    assert len(rows) == 1
    assert rows[0]["event_key"] == "k1"
    assert rows[0]["ingested_at"] is None

    df_tie = _dedup_df(
        spark,
        [
            (5, "b.parquet", "k2"),
            (5, None, "k2"),
        ],
    )

    tie_rows = within_batch_dedup(df_tie, logger).collect()

    assert len(tie_rows) == 1
    assert tie_rows[0]["event_key"] == "k2"
    assert tie_rows[0]["source_file"] is None
