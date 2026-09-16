#!/usr/bin/env python3
"""
Unified Quality Gate Runner
Runs Raw, Bronze, and Silver quality checks in sequence.
Aggregates results into one clear pass/fail signal.
"""

import argparse
from datetime import datetime
import json
import logging
from pathlib import Path
import subprocess
import sys

# ===== Logging Setup (consistent with other scripts) =====


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "event": getattr(record, "event", record.msg),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if hasattr(record, "extra"):
            log_entry.update(record.extra)
        return json.dumps(log_entry)


def setup_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

# ===== Argument Parsing =====


def parse_args():
    parser = argparse.ArgumentParser(
        description="Unified Quality Gate Runner - Runs Raw, Bronze, Silver checks"
    )
    parser.add_argument(
        "--layer",
        choices=["raw", "bronze", "silver", "all"],
        default="all",
        help="Which layer to run (default: all)"
    )
    parser.add_argument(
        "--report-dir",
        default="./logs",
        help="Directory to write report files (default: ./logs)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level"
    )
    return parser.parse_args()

# ===== Suite Runner Functions =====


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_raw_checks(logger):
    """Run Raw layer quality checks"""
    logger.info("Running Raw checks", extra={"event": "raw_start"})

    cmd = ["python3", str(PROJECT_ROOT / "soda/run_raw_checks.py")]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))

    logger.info(
        "Raw checks complete",
        extra={
            "event": "raw_complete",
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    )

    return {
        "name": "raw",
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "passed": result.returncode == 0
    }


def run_bronze_checks(logger):
    """Run Bronze layer Soda checks"""
    logger.info("Running Bronze checks", extra={"event": "bronze_start"})

    cmd = [
        "python3",
        str(PROJECT_ROOT / "soda/run_soda_scan.py"),
        "s3a://bronze/ecommerce_events/",
        str(PROJECT_ROOT / "soda/checks/bronze_checks.yml"),
        str(PROJECT_ROOT / "soda/configurations/spark_configuration.yml")
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))

    logger.info(
        "Bronze checks complete",
        extra={
            "event": "bronze_complete",
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    )

    return {
        "name": "bronze",
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "passed": result.returncode == 0
    }


def run_silver_checks(logger):
    """Run Silver layer Soda checks"""
    logger.info("Running Silver checks", extra={"event": "silver_start"})

    cmd = ["python3", str(PROJECT_ROOT / "soda/run_silver_scan.py")]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))

    logger.info(
        "Silver checks complete",
        extra={
            "event": "silver_complete",
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    )

    return {
        "name": "silver",
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "passed": result.returncode == 0
    }

# ===== Aggregation Functions =====


def aggregate_results(results):
    """Aggregate individual suite results into overall status"""
    passed_count = sum(1 for r in results if r["passed"])
    failed_count = len(results) - passed_count
    overall_passed = failed_count == 0

    return {
        "overall_status": "PASSED" if overall_passed else "FAILED",
        "overall_exit_code": 0 if overall_passed else 1,
        "summary": {
            "total": len(results),
            "passed": passed_count,
            "failed": failed_count
        },
        "suites": results
    }


def write_report(aggregated, report_dir, logger):
    """Write aggregated report to file"""
    Path(report_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = Path(report_dir) / f"quality_gate_report_{timestamp}.json"

    report_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        **aggregated
    }

    with open(report_path, "w") as f:
        json.dump(report_data, f, indent=2)

    logger.info(
        "Report written",
        extra={
            "event": "report_written",
            "path": str(report_path)
        }
    )

    return report_path

# ===== Main =====


def main():
    args = parse_args()
    logger = setup_logging()
    logger.setLevel(getattr(logging, args.log_level))

    logger.info(
        "Quality gate started",
        extra={
            "event": "gate_start",
            "layer": args.layer,
            "report_dir": args.report_dir
        }
    )

    if args.layer == "all":
        suites = ["raw", "bronze", "silver"]
    else:
        suites = [args.layer]

    # Run each suite
    results = []
    for suite_name in suites:
        runner = {
            "raw": run_raw_checks,
            "bronze": run_bronze_checks,
            "silver": run_silver_checks
        }[suite_name]

        result = runner(logger)
        results.append(result)

    # Aggregate results
    aggregated = aggregate_results(results)

    # Write report
    report_path = write_report(aggregated, args.report_dir, logger)

    # Log final summary
    logger.info(
        "Quality gate complete",
        extra={
            "event": "gate_complete",
            "overall_status": aggregated["overall_status"],
            "passed": aggregated["summary"]["passed"],
            "failed": aggregated["summary"]["failed"],
            "report_path": str(report_path)
        }
    )

    # Print summary to stdout
    print("\n" + "=" * 60)
    print("📊 QUALITY GATE SUMMARY")
    print("=" * 60)
    print(f"Overall Status: {aggregated['overall_status']}")
    print(f"Suites: {aggregated['summary']['passed']}/{aggregated['summary']['total']} passed")
    print(f"Report: {report_path}")
    print("=" * 60)

    for suite in aggregated["suites"]:
        status = "✅ PASSED" if suite["passed"] else "❌ FAILED"
        print(f"{suite['name'].upper():8} {status}")

    print("=" * 60)

    sys.exit(aggregated["overall_exit_code"])


if __name__ == "__main__":
    main()
