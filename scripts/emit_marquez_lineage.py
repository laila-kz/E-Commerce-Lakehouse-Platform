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
                {"name": "product_id", "type": "INT"},
                {"name": "category_id", "type": "BIGINT"},
                {"name": "brand", "type": "STRING"},
                {"name": "price", "type": "FLOAT"},
                {"name": "user_id", "type": "BIGINT"},
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
                {"name": "product_id", "type": "INT"},
                {"name": "category_id", "type": "BIGINT"},
                {"name": "brand", "type": "STRING"},
                {"name": "price", "type": "DOUBLE"},
                {"name": "user_id", "type": "BIGINT"},
                {"name": "user_session", "type": "STRING"},
                {"name": "ingested_at", "type": "TIMESTAMP"}
            ]
        }
    }

    silver_schema = {
        "schema": {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [
                {"name": "event_time", "type": "TIMESTAMP"},
                {"name": "event_type", "type": "STRING"},
                {"name": "product_id", "type": "INT"},
                {"name": "category_id", "type": "BIGINT"},
                {"name": "brand", "type": "STRING"},
                {"name": "price", "type": "DOUBLE"},
                {"name": "user_id", "type": "BIGINT"},
                {"name": "user_session", "type": "STRING"},
                {"name": "event_date", "type": "DATE"},
                {"name": "dedup_hash", "type": "STRING"},
                {"name": "is_valid", "type": "BOOLEAN"},
                {"name": "processed_at", "type": "TIMESTAMP"}
            ]
        }
    }

    marts_daily_schema = {
        "schema": {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [
                {"name": "date", "type": "DATE"},
                {"name": "product_id", "type": "BIGINT"},
                {"name": "customer_key", "type": "VARCHAR"},
                {"name": "active_users", "type": "BIGINT"},
                {"name": "total_views", "type": "BIGINT"},
                {"name": "total_carts", "type": "BIGINT"},
                {"name": "total_purchases", "type": "BIGINT"},
                {"name": "total_revenue", "type": "DOUBLE"},
                {"name": "avg_order_value", "type": "DOUBLE"},
                {"name": "conversion_rate_pct", "type": "DOUBLE"},
                {"name": "last_recomputed_at", "type": "TIMESTAMP"}
            ]
        }
    }

    marts_retention_schema = {
        "schema": {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [
                {"name": "cohort_month", "type": "DATE"},
                {"name": "cohort_size", "type": "BIGINT"},
                {"name": "m0_active", "type": "BIGINT"},
                {"name": "m1_active", "type": "BIGINT"},
                {"name": "m1_retention_pct", "type": "DOUBLE"}
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

    # 5. dbt Intermediate: stg_events -> int_sessionized_events & int_user_daily_activity
    emit_event(
        job_name="dbt_intermediate_models",
        inputs=[{"namespace": "duckdb://dbt/dev.duckdb", "name": "stg_events", "facets": silver_schema}],
        outputs=[
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_sessionized_events"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_user_daily_activity"}
        ]
    )

    # 6. dbt Dims & Facts: int_* -> dim_customer, dim_products, fact_events, fact_purchases
    emit_event(
        job_name="dbt_core_dims_and_facts",
        inputs=[
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_sessionized_events"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "int_user_daily_activity"}
        ],
        outputs=[
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "dim_customer"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "dim_products"},
            {"namespace": "duckdb://dbt/dev.duckdb", "name": "dim_dates"},
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
