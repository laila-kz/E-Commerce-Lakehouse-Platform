"""
Emit complete OpenLineage metadata to Marquez API
Namespace: ecommerce_lakehouse
Lineage: Raw S3 -> Bronze Delta -> Silver Delta -> dbt Staging
         -> Intermediate -> Dims & Facts -> Gold Marts -> Reconciliation
"""

from datetime import datetime, timezone
import json
import os
import urllib.error
import urllib.request
import uuid

MARQUEZ_URL = os.environ.get("MARQUEZ_URL", "http://localhost:5000/api/v1/lineage")
NAMESPACE = "ecommerce_lakehouse"


def emit_event(job_name, inputs, outputs, event_type="COMPLETE"):
    now = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())

    event = {
        "eventType": event_type,
        "eventTime": now,
        "run": {
            "runId": run_id
        },
        "job": {
            "namespace": NAMESPACE,
            "name": job_name
        },
        "inputs": inputs,
        "outputs": outputs,
        "producer": "https://github.com/OpenLineage/OpenLineage"
    }

    data = json.dumps(event).encode("utf-8")
    req = urllib.request.Request(
        MARQUEZ_URL,
        data=data,
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(req) as response:
            print(f"[LINEAGE] Emitted '{job_name}' ({event_type}) -> Status: {response.status}")
    except urllib.error.URLError as e:
        print(f"[ERROR] Failed to emit lineage for '{job_name}': {e}")


def main():
    print(f"Emitting OpenLineage events to {MARQUEZ_URL} in namespace '{NAMESPACE}'...")

    # Define Schema Facets
    raw_schema = {
        "schema": {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [
                {"name": "event_time", "type": "STRING"},
                {"name": "event_type", "type": "STRING"},
                {"name": "product_id", "type": "LONG"},
                {"name": "category_id", "type": "LONG"},
                {"name": "category_code", "type": "STRING"},
                {"name": "brand", "type": "STRING"},
                {"name": "price", "type": "DOUBLE"},
                {"name": "user_id", "type": "LONG"},
                {"name": "user_session", "type": "STRING"}
            ]
        }
    }

    bronze_schema = {
        "schema": {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [
                {"name": "event_time", "type": "TIMESTAMP"},
                {"name": "event_type", "type": "STRING"},
                {"name": "product_id", "type": "LONG"},
                {"name": "category_id", "type": "LONG"},
                {"name": "category_code", "type": "STRING"},
                {"name": "brand", "type": "STRING"},
                {"name": "price", "type": "DOUBLE"},
                {"name": "user_id", "type": "LONG"},
                {"name": "user_session", "type": "STRING"},
                {"name": "ingested_at", "type": "TIMESTAMP"},
                {"name": "source_file", "type": "STRING"},
                {"name": "pipeline_run_id", "type": "STRING"},
                {"name": "batch_id", "type": "STRING"},
                {"name": "event_date", "type": "DATE"}
            ]
        }
    }

    # Real Silver columns: spark_jobs/silver_transform.py output
    #   category_l1/l2/l3 (L342-353), event_date (L371), event_key (L378-390),
    #   plus lineage columns added in spark_jobs/bronze_transform.py (L265-270).
    silver_schema = {
        "schema": {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [
                {"name": "event_key", "type": "STRING"},
                {"name": "user_id", "type": "BIGINT"},
                {"name": "user_session", "type": "STRING"},
                {"name": "event_type", "type": "STRING"},
                {"name": "product_id", "type": "BIGINT"},
                {"name": "category_id", "type": "BIGINT"},
                {"name": "category_code", "type": "STRING"},
                {"name": "category_l1", "type": "STRING"},
                {"name": "category_l2", "type": "STRING"},
                {"name": "category_l3", "type": "STRING"},
                {"name": "brand", "type": "STRING"},
                {"name": "price", "type": "DOUBLE"},
                {"name": "event_time", "type": "TIMESTAMP"},
                {"name": "event_date", "type": "STRING"},
                {"name": "ingested_at", "type": "TIMESTAMP"},
                {"name": "source_file", "type": "STRING"},
                {"name": "pipeline_run_id", "type": "STRING"},
                {"name": "batch_id", "type": "STRING"}
            ]
        }
    }

    # Real mart columns: dbt/models/marts/mart_daily_summary.sql (combined CTE)
    marts_daily_schema = {
        "schema": {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [
                {"name": "date", "type": "DATE"},
                {"name": "category_l1", "type": "VARCHAR"},
                {"name": "total_events", "type": "BIGINT"},
                {"name": "total_purchases", "type": "BIGINT"},
                {"name": "total_revenue", "type": "DOUBLE"},
                {"name": "active_users", "type": "BIGINT"},
                {"name": "last_recomputed_at", "type": "TIMESTAMP"}
            ]
        }
    }

    # Real mart columns: dbt/models/marts/mart_customer_retention.sql (final CTE)
    marts_retention_schema = {
        "schema": {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [
                {"name": "cohort_month", "type": "DATE"},
                {"name": "months_since_first_purchase", "type": "BIGINT"},
                {"name": "cohort_size", "type": "BIGINT"},
                {"name": "retained_count", "type": "BIGINT"},
                {"name": "retention_rate", "type": "DOUBLE"}
            ]
        }
    }

    # 1. Ingestion: Raw Source -> s3a://raw/ecommerce_events
    emit_event(
        job_name="kaggle_ingest_raw",
        inputs=[{"namespace": "file://data/demo_sample", "name": "2019-Oct.csv", "facets": raw_schema}],
        outputs=[{"namespace": "s3a://raw", "name": "ecommerce_events", "facets": raw_schema}]
    )

    # 2. Bronze Transform: s3a://raw/ecommerce_events -> s3a://bronze/ecommerce_events
    emit_event(
        job_name="pyspark_bronze_transform",
        inputs=[{"namespace": "s3a://raw", "name": "ecommerce_events", "facets": raw_schema}],
        outputs=[{"namespace": "s3a://bronze", "name": "ecommerce_events", "facets": bronze_schema}]
    )

    # 3. Silver Transform: s3a://bronze/ecommerce_events -> s3a://silver/ecommerce_events
    emit_event(
        job_name="pyspark_silver_merge_transform",
        inputs=[{"namespace": "s3a://bronze", "name": "ecommerce_events", "facets": bronze_schema}],
        outputs=[{"namespace": "s3a://silver", "name": "ecommerce_events", "facets": silver_schema}]
    )

    # 4. dbt Staging: s3a://silver/ecommerce_events -> dev.duckdb.stg_events
    emit_event(
        job_name="dbt_stg_events",
        inputs=[{"namespace": "s3a://silver", "name": "ecommerce_events", "facets": silver_schema}],
        outputs=[{"namespace": "duckdb://dbt/dev.duckdb", "name": "stg_events", "facets": silver_schema}]
    )

    # 5. dbt Intermediate: stg_events -> int_sessions, int_events_enriched, int_customer_month_activity
    emit_event(
        job_name="dbt_intermediate_models",
        inputs=[{"namespace": "duckdb://dbt/dev.duckdb", "name": "stg_events", "facets": silver_schema}],
        outputs=[
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_sessions"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_events_enriched"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_customer_month_activity"}
        ]
    )

    # 6. dbt Dims & Facts: int_* -> dim_customer, dim_product, dim_date, fact_events, fact_purchases
    emit_event(
        job_name="dbt_core_dims_and_facts",
        inputs=[
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_sessions"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_events_enriched"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_customer_month_activity"}
        ],
        outputs=[
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "dim_customer"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "dim_product"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "dim_date"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "fact_events"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "fact_purchases"}
        ]
    )

    # 7. dbt Gold Marts: facts & dims -> mart_daily_summary, mart_customer_retention
    emit_event(
        job_name="dbt_gold_marts",
        inputs=[
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "fact_events"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "fact_purchases"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "dim_customer"}
        ],
        outputs=[
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "mart_daily_summary", "facets": marts_daily_schema},
            {
                "namespace": "duckdb://dbt/dev.duckdb",
                "name": "mart_customer_retention",
                "facets": marts_retention_schema
            }
        ]
    )

    # 8. Cross-Layer Reconciliation: Silver vs Gold Marts
    emit_event(
        job_name="cross_layer_reconciliation",
        inputs=[
            {"namespace": "s3a://silver", "name": "ecommerce_events"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "fact_events"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "mart_daily_summary"}
        ],
        outputs=[
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "reconciliation_results"}
        ]
    )

    print("\n[SUCCESS] Successfully populated Marquez with full Lakehouse Lineage & Governance metadata!")


if __name__ == "__main__":
    main()
