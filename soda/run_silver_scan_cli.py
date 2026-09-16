#!/usr/bin/env python3
"""
Soda Core Scan Runner - Silver Layer (CLI approach)
"""
import subprocess
import sys


def run_silver_scan():
    print("=" * 60)
    print("🔍 SODA CORE SILVER QUALITY SCAN")
    print("=" * 60)
    print("📊 Table: s3a://silver/ecommerce_events/")
    print("📝 Checks: soda/checks/silver_checks.yml")
    print("=" * 60)

    # Use CLI directly
    cmd = [
        "soda", "scan",
        "-d", "spark_datasource",
        "-c", "soda/configurations/spark_configuration.yml",
        "-v", "silver_table=s3a://silver/ecommerce_events/",
        "soda/checks/silver_checks.yml"
    ]

    result = subprocess.run(cmd, capture_output=False, text=True)

    print("\n" + "=" * 60)
    print("📊 SCAN COMPLETE")
    print("=" * 60)

    if result.returncode == 0:
        print("✅ ALL SILVER CHECKS PASSED!")
    else:
        print(f"❌ SOME SILVER CHECKS FAILED! Exit code: {result.returncode}")

    return result.returncode


if __name__ == "__main__":
    sys.exit(run_silver_scan())
