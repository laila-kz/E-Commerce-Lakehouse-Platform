#!/usr/bin/env python3
"""
Cross-Layer Reconciliation Check Suite

Checks cross-layer row/revenue parity:

  1. stg_events rows            ==  fact_events rows            (Silver -> Gold row parity)
  2. fact_purchases revenue     ==  mart_daily_summary revenue   (Gold revenue parity)
  3. stg_events[purchase] rows  ==  fact_purchases rows          (Gold purchase-count parity)
  4. raw CSV rows               ==  bronze Delta rows            (Raw -> Bronze row parity)
  5. bronze Delta rows          ==  stg_events rows              (Bronze -> Silver row parity)

Checks 1-3 query the dbt-materialised views (stg_events, fact_*) which read from
DuckDB's Gold database — they do NOT require MinIO/Docker to be running.
Checks 4-5 attempt to read Raw/Bronze directly via DuckDB's httpfs/delta
extensions; when MinIO is offline (or the extensions are unavailable) the check
is recorded as SKIP with the reason it was skipped — it is never silently
dropped from the report.

Exit semantics:
  * any FAIL            -> exit 1
  * any SKIP            -> exit 1 UNLESS --allow-skips is passed (checks are
      infrastructure-dependent, not data errors, so skipping is explicit)
  * all PASS            -> exit 0
"""

import os
import sys
import uuid
import json
import logging
import argparse
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


def _safe_run(con, label, sql):
    """
    Execute a single reconciliation SQL query.
    Returns {"ok": True, "source": ..., "target": ...} on success, or
    {"ok": False, "reason": "<one-line error>"} when the check cannot run
    (missing tables, MinIO offline, extensions unavailable).
    """
    try:
        row = con.execute(sql).fetchone()
        return {"ok": True, "source": row[0], "target": row[1], "reason": None}
    except Exception as e:
        # Surface a one-line warning without the full 10-retry S3 traceback
        short = str(e).split("\n")[0][:200]
        logger.warning(f"{label} — SKIPPED ({short})")
        return {"ok": False, "source": -1, "target": -1, "reason": short}


def _int_result(run_id, now, run, name, source_layer, target_layer):
    """Build a result record from an integer-parity _safe_run outcome."""
    if not run["ok"]:
        return {
            "run_id": run_id,
            "check_name": name,
            "source_layer": source_layer,
            "target_layer": target_layer,
            "source_count": -1,
            "target_count": -1,
            "drift": -1,
            "status": "SKIP",
            "reason": run["reason"],
            "executed_at": now,
        }
    src, tgt = int(run["source"]), int(run["target"])
    drift = abs(src - tgt)
    return {
        "run_id": run_id,
        "check_name": name,
        "source_layer": source_layer,
        "target_layer": target_layer,
        "source_count": src,
        "target_count": tgt,
        "drift": drift,
        "status": "PASS" if drift == 0 else "FAIL",
        "reason": None,
        "executed_at": now,
    }


def run_checks(con, run_id):
    """Execute all reconciliation checks. Returns list of result dicts (including SKIPs)."""
    results = []
    now = datetime.utcnow()

    # ── Check 1: Silver (stg_events) row count == fact_events row count ──────
    run = _safe_run(
        con,
        label="Check 1 [silver_to_fact_events_row_parity]",
        sql="""
            SELECT
                (SELECT COUNT(*) FROM stg_events)  AS silver_count,
                (SELECT COUNT(*) FROM fact_events)  AS fact_count
        """,
    )
    results.append(_int_result(
        run_id, now, run,
        name="silver_to_fact_events_row_parity",
        source_layer="Silver (stg_events)",
        target_layer="Gold (fact_events)",
    ))

    # ── Check 2: fact_purchases revenue == mart_daily_summary revenue ─────────
    run = _safe_run(
        con,
        label="Check 2 [purchases_to_mart_revenue_parity]",
        sql="""
            SELECT
                COALESCE((SELECT ROUND(SUM(revenue),       2) FROM fact_purchases),    0) AS fact_rev,
                COALESCE((SELECT ROUND(SUM(total_revenue), 2) FROM mart_daily_summary), 0) AS mart_rev
        """,
    )
    if not run["ok"]:
        results.append({
            "run_id": run_id,
            "check_name": "purchases_to_mart_revenue_parity",
            "source_layer": "Gold (fact_purchases)",
            "target_layer": "Gold (mart_daily_summary)",
            "source_count": -1,
            "target_count": -1,
            "drift": -1,
            "status": "SKIP",
            "reason": run["reason"],
            "executed_at": now,
        })
    else:
        fact_rev, mart_rev = float(run["source"]), float(run["target"])
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
            "reason": None,
            "executed_at": now,
        })

    # ── Check 3: stg_events[purchase] count == fact_purchases row count ───────
    run = _safe_run(
        con,
        label="Check 3 [silver_to_fact_purchases_parity]",
        sql="""
            SELECT
                (SELECT COUNT(*) FROM stg_events    WHERE event_type = 'purchase') AS silver_p,
                (SELECT COUNT(*) FROM fact_purchases)                               AS fact_p
        """,
    )
    results.append(_int_result(
        run_id, now, run,
        name="silver_to_fact_purchases_parity",
        source_layer="Silver (stg_events)",
        target_layer="Gold (fact_purchases)",
    ))

    # ── Check 4: raw CSV rows == bronze Delta rows (requires MinIO) ───────────
    run = _safe_run(
        con,
        label="Check 4 [raw_to_bronze_row_parity]",
        sql="""
            SELECT
                (SELECT COUNT(*) FROM read_csv_auto(
                    's3a://raw/ecommerce_events/*',
                    union_by_name=true, ignore_errors=true))                       AS raw_count,
                (SELECT COUNT(*) FROM delta_scan('s3a://bronze/ecommerce_events/')) AS bronze_count
        """,
    )
    results.append(_int_result(
        run_id, now, run,
        name="raw_to_bronze_row_parity",
        source_layer="Raw (raw/ecommerce_events/*)",
        target_layer="Bronze (bronze/ecommerce_events/)",
    ))

    # ── Check 5: bronze Delta rows == stg_events rows (requires MinIO) ────────
    run = _safe_run(
        con,
        label="Check 5 [bronze_to_silver_row_parity]",
        sql="""
            SELECT
                (SELECT COUNT(*) FROM delta_scan('s3a://bronze/ecommerce_events/')) AS bronze_count,
                (SELECT COUNT(*) FROM stg_events)                                   AS silver_count
        """,
    )
    results.append(_int_result(
        run_id, now, run,
        name="bronze_to_silver_row_parity",
        source_layer="Bronze (bronze/ecommerce_events/)",
        target_layer="Silver (stg_events)",
    ))

    return results


def _persist_report(con, results, run_id, passed, failed, skipped):
    """Write reconciliation_results rows and the JSON audit report."""
    for r in results:
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


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Cross-Layer Reconciliation Check Suite")
    parser.add_argument(
        "--allow-skips",
        action="store_true",
        help="Return exit code 0 even when one or more checks were SKIPPED "
             "(e.g. MinIO/Docker offline). Default: any SKIP exits 1 (strict).",
    )
    args = parser.parse_args(argv)

    run_id = str(uuid.uuid4())[:8]
    logger.info(f"Starting Cross-Layer Reconciliation Suite (Run ID: {run_id})")

    con = get_duckdb_connection()
    init_reconciliation_table(con)

    results = run_checks(con, run_id)

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

    print("=" * 85)
    print(f"  Summary: {passed} PASS  |  {failed} FAIL  |  {skipped} SKIP")
    print("=" * 85 + "\n")

    # ── Show exactly which checks were skipped and why ────────────────────────
    for r in results:
        if r["status"] == "SKIP":
            print(f"  [SKIP] {r['check_name']}: {r['reason']}")
    if skipped:
        print("  -> run with --allow-skips to treat infrastructure SKIPs as non-fatal\n")

    _persist_report(con, results, run_id, passed, failed, skipped)

    # ── Exit codes ────────────────────────────────────────────────────────────
    if failed > 0:
        logger.error(
            f"Reconciliation FAILED: {failed} check(s) detected cross-layer drift!"
        )
        return 1

    if skipped > 0:
        if not args.allow_skips:
            logger.error(
                f"{skipped} check(s) were SKIPPED (MinIO/Docker offline or missing tables). "
                f"Fix the environment and re-run, or pass --allow-skips to proceed. "
                f"Gold-layer checks ({passed} PASS) completed successfully."
            )
            return 1
        logger.warning(
            f"{skipped} check(s) SKIPPED (MinIO/Docker offline). "
            f"Start Docker and re-run for a full audit. "
            f"--allow-skips set, continuing with exit 0. "
            f"Gold-layer checks ({passed} PASS) completed successfully."
        )
        return 0

    logger.info(
        f"All {passed} cross-layer reconciliation checks PASSED with 0.00% drift."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())