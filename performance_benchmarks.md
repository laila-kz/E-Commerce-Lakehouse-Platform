# 🚀 Spark & Delta Lake Performance Optimization Benchmark Report

![Mode: Empirical Baseline](https://img.shields.io/badge/Mode-Empirical_Baseline_Profile-FFA500?style=flat)
**Execution Profile:** `EMPIRICAL_REFERENCE_PROFILE`
**Generated At:** 2026-09-16 15:00:13 UTC
**Target Environment:** Spark on MinIO S3A Data Lakehouse

> **Banner:** `EMPIRICAL_REFERENCE_PROFILE` means these figures are hardcoded fallback values in `spark_jobs/spark_benchmark.py` (fallback data at lines 292-310, emitted by the reference-profile branch at lines 285-313), **not** a live cluster run. Every number and gain in the tables below is a **reference value** only.

---

## 📊 Benchmark Suite 1: Storage Optimization & File Compaction (reference values)

| Storage Strategy | Query Duration (ms) | Records | Performance Gain | Notes |
|------------------|---------------------|---------|------------------|-------|
| **Standard Delta Lake (Default)** | `1420.5 ms` | `500000` | Baseline | Partition pruned Delta read |
| **Delta OPTIMIZE + Z-Order (`user_id`)** | `480.2 ms` | `500000` | **~3.0x Faster** | Coalesced data files & data-skipping via Z-Order min/max stats |

---

## ⚡ Benchmark Suite 2: Join Strategy Benchmarks (`fact_events` ⋈ `dim_product`) (reference values)

| Join Strategy | Execution Time (ms) | Network Shuffle Overhead | Performance Impact |
|---------------|----------------------|--------------------------|--------------------|
| **Sort-Merge Join (Standard)** | `2850.0 ms` | `High (Full network exchange across executors)` | Baseline shuffle overhead |
| **Broadcast Hash Join (`broadcast()`)** | `890.0 ms` | `Zero (Dimension table broadcasted to all workers)` | **~3.2x Faster** |

---

## ⚙️ Benchmark Suite 3: Shuffle Partition Tuning (`spark.sql.shuffle.partitions`) (reference values)

| Partition Strategy | Execution Duration (ms) | Active Partitions | Task Overhead Impact |
|--------------------|-------------------------|-------------------|----------------------|
| **Default Setting (200 Partitions)** | `1950.0 ms` | `200` | Baseline (excessive empty task scheduling) |
| **Tuned Setting (8 Partitions)** | `720.0 ms` | `8` | **Tuned for optimal CPU core saturation** |

---

## 💡 Key Architectural Insights & Production Recommendations

1. **Z-Ordering Datasets by High-Cardinality Filters**: Applying `ZORDER BY (user_id, event_time)` on Silver Delta tables reduces file scanning volume significantly via Delta Lake's data-skipping file statistics.
2. **Dimension Table Broadcasting**: For dimension joins against large event fact tables, wrapping dimension DataFrames with `broadcast()` avoids multi-gigabyte network shuffles.
3. **Partition Sizing**: Adjusting `spark.sql.shuffle.partitions` to match cluster CPU slot counts prevents task scheduling latency on smaller data batches.
