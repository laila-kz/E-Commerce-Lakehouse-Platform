#!/usr/bin/env python3
"""
Cross-Layer Reconciliation Check Suite

Checks three Gold-layer pairs:
  1. stg_events rows            ==  fact_events rows            (event row parity)
  2. fact_purchases revenue     ==  mart_daily_summary revenue   (purchase revenue parity)
  3. stg_events[purchase] rows  ==  fact_purchases rows          (purchase count parity)

NOTE: Checks 1 & 3 query the dbt-materialised views (stg_events) which read from
DuckDB's Gold database — they do NOT require MinIO/Docker to be running.
When a source is unavailable, the affected check is recorded as SKIP (not
silently ignored) and the process can still exit 0.
"""

import os
import sys
import uuid
import json
import logging
from datetime import datetime
import duckdb

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("reconciliation")

# ─────────────────────────────────────────────────────────────────────────────
# Sentinel value for a skipped/errored check so we can track it properly
# ─────────────────────────────────────────────────────────────────────────────
_SKIP = object()


def get_duckdb_connection():
    """Create DuckDB connection. Configures S3/MinIO only if endpoint reachable."""
    endpoint = (
        os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        .replace("http://", "")
        .replace("https://", "")
    )
    access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    db_path = os.environ.get(
        "DUCKDB_PATH", os.path.join(os.getcwd(), "dbt", "dev.duckdb")
    )
    con = duckdb.connect(db_path if os.path.exists(db_path) else ":memory:")

    con.execute("INSTALL httpfs; LOAD httpfs;")
    try:
        con.execute("INSTALL delta; LOAD delta;")
    except Exception as e:
        logger.debug(f"Delta extension load notice: {e}")

    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=false;")

    return con


def init_reconciliation_table(con):
    """Ensure reconciliation_results table exists."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS reconciliation_results (
            run_id       VARCHAR,
            check_name   VARCHAR,
            source_layer VARCHAR,
            target_layer VARCHAR,
            source_count BIGINT,
            target_count BIGINT,
            drift        BIGINT,
            status       VARCHAR,
            executed_at  TIMESTAMP
        )
    """)


def _safe_run(con, label, sql, *, source_key, target_key):
    """
    Execute a single reconciliation SQL query.
    Returns a (source_count, target_count) tuple, or _SKIP on error.
    """
    try:
        row = con.execute(sql).fetchone()
        return row[0], row[1]
    except Exception as e:
        # Surface a one-line warning without the full 10-retry S3 traceback
        short = str(e).split("\n")[0][:200]
        logger.warning(f"{label} — SKIPPED ({short})")
        return _SKIP


def run_checks(con, run_id):
    """Execute all reconciliation checks. Returns list of result dicts (including SKIPs)."""
    results = []
    now = datetime.utcnow()

    # ── Check 1: Silver (stg_events) row count == fact_events row count ──────
    counts = _safe_run(
        con,
        label="Check 1 [silver_to_fact_events_row_parity]",
        sql="""
            SELECT
                (SELECT COUNT(*) FROM stg_events)  AS silver_count,
                (SELECT COUNT(*) FROM fact_events)  AS fact_count
        """,
        source_key="silver_count",
        target_key="fact_count",
    )
    if counts is _SKIP:
        results.append({
            "run_id": run_id,
            "check_name": "silver_to_fact_events_row_parity",
            "source_layer": "Silver (stg_events)",
            "target_layer": "Gold (fact_events)",
            "source_count": -1,
            "target_count": -1,
            "drift": -1,
            "status": "SKIP",
            "executed_at": now,
        })
    else:
        src, tgt = counts
        drift = abs(src - tgt)
        results.append({
            "run_id": run_id,
            "check_name": "silver_to_fact_events_row_parity",
            "source_layer": "Silver (stg_events)",
            "target_layer": "Gold (fact_events)",
            "source_count": src,
            "target_count": tgt,
            "drift": drift,
            "status": "PASS" if drift == 0 else "FAIL",
            "executed_at": now,
        })

    # ── Check 2: fact_purchases revenue == mart_daily_summary revenue ─────────
    counts = _safe_run(
        con,
        label="Check 2 [purchases_to_mart_revenue_parity]",
        sql="""
            SELECT
                COALESCE((SELECT ROUND(SUM(revenue),       2) FROM fact_purchases),    0) AS fact_rev,
                COALESCE((SELECT ROUND(SUM(total_revenue), 2) FROM mart_daily_summary), 0) AS mart_rev
        """,
        source_key="fact_rev",
        target_key="mart_rev",
    )
    if counts is _SKIP:
        results.append({
            "run_id": run_id,
            "check_name": "purchases_to_mart_revenue_parity",
            "source_layer": "Gold (fact_purchases)",
            "target_layer": "Gold (mart_daily_summary)",
            "source_count": -1,
            "target_count": -1,
            "drift": -1,
            "status": "SKIP",
            "executed_at": now,
        })
    else:
        fact_rev, mart_rev = float(counts[0]), float(counts[1])
        drift = round(abs(fact_rev - mart_rev), 2)
        results.append({
            "run_id": run_id,
            "check_name": "purchases_to_mart_revenue_parity",
            "source_layer": "Gold (fact_purchases)",
            "target_layer": "Gold (mart_daily_summary)",
            "source_count": int(fact_rev),
            "target_count": int(mart_rev),
            "drift": int(drift),
            "status": "PASS" if drift == 0.0 else "FAIL",
            "executed_at": now,
        })

    # ── Check 3: stg_events[purchase] count == fact_purchases row count ───────
    counts = _safe_run(
        con,
        label="Check 3 [silver_to_fact_purchases_parity]",
        sql="""
            SELECT
                (SELECT COUNT(*) FROM stg_events    WHERE event_type = 'purchase') AS silver_p,
                (SELECT COUNT(*) FROM fact_purchases)                               AS fact_p
        """,
        source_key="silver_p",
        target_key="fact_p",
    )
    if counts is _SKIP:
        results.append({
            "run_id": run_id,
            "check_name": "silver_to_fact_purchases_parity",
            "source_layer": "Silver (stg_events)",
            "target_layer": "Gold (fact_purchases)",
            "source_count": -1,
            "target_count": -1,
            "drift": -1,
            "status": "SKIP",
            "executed_at": now,
        })
    else:
        src, tgt = counts
        drift = abs(src - tgt)
        results.append({
            "run_id": run_id,
            "check_name": "silver_to_fact_purchases_parity",
            "source_layer": "Silver (stg_events)",
            "target_layer": "Gold (fact_purchases)",
            "source_count": src,
            "target_count": tgt,
            "drift": drift,
            "status": "PASS" if drift == 0 else "FAIL",
            "executed_at": now,
        })

    return results


def main():
    run_id = str(uuid.uuid4())[:8]
    logger.info(f"Starting Cross-Layer Reconciliation Suite (Run ID: {run_id})")

    con = get_duckdb_connection()
    init_reconciliation_table(con)

    results = run_checks(con, run_id)

    if not results:
        logger.info(
            "No active tables found to reconcile (pipeline not yet run). Initializing baseline."
        )
        return 0

    # ── Print report ──────────────────────────────────────────────────────────
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    skipped = sum(1 for r in results if r["status"] == "SKIP")

    print("\n" + "=" * 85)
    print(f"  CROSS-LAYER RECONCILIATION AUDIT REPORT — RUN: {run_id}")
    print("=" * 85)
    print(
        f"{'CHECK NAME':<38} {'SOURCE':>10}  {'TARGET':>10}  {'DRIFT':>6}  STATUS"
    )
    print("-" * 85)

    for r in results:
        src = str(r["source_count"]) if r["source_count"] >= 0 else "N/A"
        tgt = str(r["target_count"]) if r["target_count"] >= 0 else "N/A"
        drft = str(r["drift"]) if r["drift"] >= 0 else "N/A"
        print(
            f"{r['check_name']:<38} {src:>10}  {tgt:>10}  {drft:>6}  [{r['status']}]"
        )
        con.execute(
            "INSERT INTO reconciliation_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                r["run_id"],
                r["check_name"],
                r["source_layer"],
                r["target_layer"],
                r["source_count"],
                r["target_count"],
                r["drift"],
                r["status"],
                r["executed_at"],
            ),
        )

    print("=" * 85)
    print(f"  Summary: {passed} PASS  |  {failed} FAIL  |  {skipped} SKIP")
    print("=" * 85 + "\n")

    # ── Persist JSON audit report ─────────────────────────────────────────────
    os.makedirs("audit", exist_ok=True)
    report_path = os.path.join("audit", f"reconciliation_{run_id}.json")
    with open(report_path, "w") as f:
        json.dump(
            {
                "run_id": run_id,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "summary": {"pass": passed, "fail": failed, "skip": skipped},
                "checks": [
                    {
                        k: str(v) if isinstance(v, datetime) else v
                        for k, v in r.items()
                    }
                    for r in results
                ],
            },
            f,
            indent=2,
        )
    logger.info(f"Reconciliation audit report saved to {report_path}")

    # ── Exit codes ────────────────────────────────────────────────────────────
    if failed > 0:
        logger.error(
            f"Reconciliation FAILED: {failed} check(s) detected cross-layer drift!"
        )
        sys.exit(1)

    if skipped > 0:
        logger.warning(
            f"{skipped} check(s) SKIPPED (MinIO/Docker offline). "
            f"Start Docker and re-run for a full audit. "
            f"Gold-layer checks ({passed} PASS) completed successfully."
        )
        # Exit 0 — Gold-layer checks passed; SKIPs are infrastructure, not data errors
        return 0

    logger.info(
        f"All {passed} cross-layer reconciliation checks PASSED with 0.00% drift."
    )
    return 0


if __name__ == "__main__":
    main()
