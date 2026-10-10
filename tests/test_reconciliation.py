import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from checks import run_reconciliation as rc


def _seed_gold(tmp_path, fact_events_rows=2):
    """Create a Gold-layer DuckDB database whose row/revenue counts satisfy checks 1-3.
    Checks 4-5 hit MinIO directly and will therefore be SKIPped in this env."""
    db = tmp_path / "rec.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE stg_events (id BIGINT, event_type VARCHAR, event_time TIMESTAMP)")
    con.execute(
        "INSERT INTO stg_events VALUES "
        "(1, 'view', TIMESTAMP '2024-01-01 10:00:00'),"
        "(2, 'purchase', TIMESTAMP '2024-01-01 10:05:00')"
    )
    con.execute("CREATE TABLE fact_events (id BIGINT, event_type VARCHAR)")
    for i in range(fact_events_rows):
        con.execute("INSERT INTO fact_events VALUES (?, 'x')", [i])
    con.execute("CREATE TABLE fact_purchases (id BIGINT, revenue DOUBLE)")
    con.execute("INSERT INTO fact_purchases VALUES (10, 750.00)")
    con.execute("CREATE TABLE mart_daily_summary (event_date DATE, total_revenue DOUBLE)")
    con.execute(
        "INSERT INTO mart_daily_summary VALUES (DATE '2024-01-01', 750.00)"
    )
    con.close()
    return db


def _empty_db(tmp_path):
    db = tmp_path / "rec.duckdb"
    duckdb.connect(str(db)).close()
    return db


def _run(monkeypatch, tmp_path, db, *extra_args):
    monkeypatch.setenv("DUCKDB_PATH", str(db))
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["run_reconciliation", *extra_args])
    return rc.main()


def test_gold_passes_but_skips_exit_1_strict(monkeypatch, tmp_path, capsys):
    db = _seed_gold(tmp_path)
    code = _run(monkeypatch, tmp_path, db)
    assert code == 1
    out = capsys.readouterr().out
    assert "[SKIP]" in out
    assert "raw_to_bronze_row_parity" in out
    assert "bronze_to_silver_row_parity" in out
    assert (tmp_path / "audit").is_dir()


def test_allow_skips_returns_0_when_only_infra_skipped(monkeypatch, tmp_path):
    db = _seed_gold(tmp_path)
    assert _run(monkeypatch, tmp_path, db, "--allow-skips") == 0


def test_drift_fails_even_with_allow_skips(monkeypatch, tmp_path):
    db = _seed_gold(tmp_path, fact_events_rows=3)
    assert _run(monkeypatch, tmp_path, db, "--allow-skips") == 1


def test_no_tables_all_skips_strict(monkeypatch, tmp_path):
    db = _empty_db(tmp_path)
    assert _run(monkeypatch, tmp_path, db) == 1
    assert _run(monkeypatch, tmp_path, db, "--allow-skips") == 0


def test_safe_run_reason_is_recorded():
    con = duckdb.connect(":memory:")
    run = rc._safe_run(con, "test", "SELECT * FROM missing_table")
    assert run["ok"] is False
    assert run["reason"]
    assert "missing_table" in run["reason"].upper() or "missing" in run["reason"].lower()
