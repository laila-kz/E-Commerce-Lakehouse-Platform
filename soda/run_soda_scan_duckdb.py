#!/usr/bin/env python3
"""
Soda Core Scan Runner using DuckDB
Reads Delta tables directly via DuckDB's delta extension
"""

import subprocess
import sys


def run_soda_scan(table_path, checks_file, configuration_file):
    """
    Run Soda scan using DuckDB
    """
    print("=" * 70)
    print("🔍 SODA CORE DATA QUALITY SCAN (DuckDB)")
    print("=" * 70)
    print(f"📊 Table: {table_path}")
    print(f"📝 Checks: {checks_file}")
    print(f"⚙️  Config: {configuration_file}")
    print("=" * 70)

    # Build the Soda scan command
    cmd = [
        "soda", "scan",
        "-d", "duckdb_datasource",
        "-c", configuration_file,
        "-v", f"bronze_table={table_path}",
        checks_file
    ]

    # Run the scan
    result = subprocess.run(cmd, capture_output=False, text=True)

    print("\n" + "=" * 70)
    print("📊 SCAN COMPLETE")
    print("=" * 70)

    if result.returncode == 0:
        print("✅ ALL CHECKS PASSED!")
    else:
        print("❌ SOME CHECKS FAILED!")
        print(f"Exit code: {result.returncode}")

    return result.returncode


if __name__ == "__main__":
    # Path to your Bronze Delta table (MinIO via S3)
    table_path = "s3a://bronze/ecommerce_events/"

    # Paths to Soda files
    checks_file = "soda/checks/bronze_checks_duckdb.yml"
    configuration_file = "soda/configurations/duckdb_configuration.yml"

    # Run the scan
    exit_code = run_soda_scan(table_path, checks_file, configuration_file)

    sys.exit(exit_code)
