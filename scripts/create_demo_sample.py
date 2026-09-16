#!/usr/bin/env python3
"""
Demo Dataset Generator
Extracts a lightweight sample (e.g. 50,000 rows / ~5MB) from large raw CSV files.
Allows the full pipeline (Ingestion -> Bronze -> Silver -> dbt -> Airflow -> Benchmarks)
to run in under 30 seconds for live video demos without locking local CPU/RAM resources.
"""

from pathlib import Path


def create_sample(source_file, target_file, max_rows=50000):
    source_path = Path(source_file)
    target_path = Path(target_file)

    if not source_path.exists():
        print(f"[SKIP] Source file {source_file} not found.")
        return

    size_gb = source_path.stat().st_size / (1024**3)
    print(f"--> Extracting {max_rows:,} sample rows from {source_path.name} ({size_gb:.2f} GB)...")

    row_count = 0
    with open(source_path, 'r', encoding='utf-8', errors='ignore') as src, \
            open(target_path, 'w', encoding='utf-8') as dst:
        for line in src:
            dst.write(line)
            row_count += 1
            if row_count >= max_rows:
                break

    size_mb = target_path.stat().st_size / (1024**2)
    print(f"[SUCCESS] Created demo sample: {target_path} ({row_count:,} rows, {size_mb:.2f} MB)")


def main():
    data_dir = Path("./data/ecommerce_events")
    sample_dir = Path("./data/demo_sample")
    sample_dir.mkdir(parents=True, exist_ok=True)

    # 1. Sample 2019-Oct.csv
    create_sample(data_dir / "2019-Oct.csv", sample_dir / "2019-Oct.csv", max_rows=50000)

    # 2. Sample 2019-Nov.csv
    create_sample(data_dir / "2019-Nov.csv", sample_dir / "2019-Nov.csv", max_rows=50000)

    print("\n[SUCCESS] Demo sample dataset ready in ./data/demo_sample/")
    print("Run ingestion with --data-dir ./data/demo_sample to execute the pipeline in DEMO MODE in seconds!")


if __name__ == "__main__":
    main()
