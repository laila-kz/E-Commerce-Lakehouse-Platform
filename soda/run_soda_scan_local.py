#!/usr/bin/env python3
"""
Soda Core Scan Runner - Local Machine
"""

import subprocess
import sys


def run_soda_scan():
    print("=" * 70)
    print("🔍 SODA CORE DATA QUALITY SCAN (Local)")
    print("=" * 70)

    # Paths
    checks_file = "soda/checks/bronze_checks_duckdb.yml"
    config_file = "soda/configurations/duckdb_configuration_local.yml"

    # Run the scan
    cmd = [
        "soda", "scan",
        "-d", "duckdb_datasource",
        "-c", config_file,
        "-v", "bronze_table=s3a://bronze/ecommerce_events/",
        checks_file
    ]

    result = subprocess.run(cmd, capture_output=False, text=True)

    print("\n" + "=" * 70)
    print("📊 SCAN COMPLETE")
    print("=" * 70)

    if result.returncode == 0:
        print("✅ ALL CHECKS PASSED!")
    else:
        print(f"❌ SOME CHECKS FAILED! Exit code: {result.returncode}")

    return result.returncode


if __name__ == "__main__":
    sys.exit(run_soda_scan())
