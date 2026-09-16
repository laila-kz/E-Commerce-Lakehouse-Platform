#!/usr/bin/env python3
"""
generate_graph.py
=================
E-Commerce Lakehouse — 3D Lineage Graph Data Generator

Reads dbt target/manifest.json and Airflow DAG metadata to produce
docs/graph_data.json consumed by docs/index.html (3d-force-graph).

Usage:
    python scripts/generate_graph.py
    python scripts/generate_graph.py --manifest dbt/target/manifest.json
    python scripts/generate_graph.py --force-static
"""

import argparse
from datetime import datetime
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

LAYER_META = {
    "source": {
        "color": "#f97316", "size": 10,
        "particle_speed": 0.008, "particle_width": 3,
    },
    "landing": {
        "color": "#facc15", "size": 9,
        "particle_speed": 0.006, "particle_width": 3,
    },
    "bronze": {
        "color": "#cd7f32", "size": 8,
        "particle_speed": 0.005, "particle_width": 2.5,
    },
    "silver": {
        "color": "#c0c0c0", "size": 8,
        "particle_speed": 0.005, "particle_width": 2.5,
    },
    "gold": {
        "color": "#ffd700", "size": 7,
        "particle_speed": 0.004, "particle_width": 2,
    },
    "bi": {
        "color": "#a78bfa", "size": 12,
        "particle_speed": 0.003, "particle_width": 2,
    },
    "orchestration": {
        "color": "#38bdf8", "size": 6,
        "particle_speed": 0.007, "particle_width": 1.5,
    },
    "quality": {
        "color": "#4ade80", "size": 6,
        "particle_speed": 0.007, "particle_width": 1.5,
    },
}

DBT_FOLDER_TO_LAYER = {
    "staging": "gold",
    "intermediate": "gold",
    "marts": "gold",
}


def resolve_dbt_model_layer(node_id, node):
    resource_type = node.get("resource_type", "model")
    if resource_type == "source":
        return "silver"
    if resource_type == "exposure":
        return "bi"
    fqn = node.get("fqn", [])
    if len(fqn) >= 3:
        folder = fqn[-2]
        if folder in DBT_FOLDER_TO_LAYER:
            return DBT_FOLDER_TO_LAYER[folder]
    return "gold"


def parse_dbt_manifest(manifest_path):
    nodes = []
    links = []
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    all_nodes = {}
    for cat in ("nodes", "sources", "exposures", "seeds", "snapshots"):
        all_nodes.update(manifest.get(cat, {}))

    for node_id, node in all_nodes.items():
        resource_type = node.get("resource_type", "model")
        name = node.get("name", node_id)
        layer = resolve_dbt_model_layer(node_id, node)
        meta = LAYER_META.get(layer, LAYER_META["gold"])
        schema = node.get("schema", node.get("source_name", ""))
        description = node.get("description", "")
        desc_short = description[:100] + "..." if len(description) > 100 else description

        if resource_type == "exposure":
            label = node.get("label", name)
            tooltip = (
                f"<b>{label}</b><br/>Type: {node.get('type','dashboard').title()} Dashboard"
                f"<br/>Layer: BI / Metabase<br/>{desc_short}"
            )
        else:
            tooltip = (
                f"<b>{name}</b><br/>Type: {resource_type.title()}<br/>"
                f"Layer: {layer.upper()}<br/>Schema: {schema}<br/>{desc_short}"
            )

        nodes.append({
            "id": node_id, "name": name,
            "resource_type": resource_type, "layer": layer,
            "color": meta["color"], "val": meta["size"],
            "tooltip": tooltip,
        })

        for parent_id in node.get("depends_on", {}).get("nodes", []):
            if parent_id.startswith(("model.", "source.", "seed.", "snapshot.")):
                links.append({
                    "source": parent_id, "target": node_id,
                    "particle_speed": meta["particle_speed"],
                    "particle_width": meta["particle_width"],
                    "particle_color": meta["color"],
                })

    return nodes, links


def build_infrastructure_nodes():
    nodes = [
        {
            "id": "infra.kaggle_csv", "name": "Kaggle CSV Dataset",
            "resource_type": "external", "layer": "source",
            "tooltip": "<b>Kaggle CSV Dataset</b><br/>~4M e-commerce click events<br/>Layer: SOURCE<br/>Demo: 50k-row fast sample",
        },
        {
            "id": "infra.minio_raw", "name": "MinIO raw/",
            "resource_type": "object_store", "layer": "landing",
            "tooltip": "<b>MinIO raw/ bucket</b><br/>Layer: LANDING<br/>Format: Parquet (partitioned by event_date)<br/>Ingested by: kaggle_ingest.py",
        },
        {
            "id": "airflow.ingest_raw", "name": "Airflow: ingest_raw",
            "resource_type": "airflow_task", "layer": "orchestration",
            "tooltip": "<b>Airflow Task: ingest_raw</b><br/>BashOperator<br/>Runs: kaggle_ingest.py<br/>Schedule: @daily",
        },
        {
            "id": "airflow.bronze_transform", "name": "Airflow: bronze_transform",
            "resource_type": "airflow_task", "layer": "orchestration",
            "tooltip": "<b>Airflow Task: bronze_transform</b><br/>spark-submit via docker exec<br/>Output: s3a://bronze/",
        },
        {
            "id": "infra.bronze_delta", "name": "Bronze Delta Table",
            "resource_type": "delta_table", "layer": "bronze",
            "tooltip": "<b>Bronze Delta Table</b><br/>s3a://bronze/ecommerce_events/<br/>Format: Delta Lake<br/>+ingested_at, source_file, pipeline_run_id",
        },
        {
            "id": "airflow.bronze_quality_gate", "name": "Soda: Bronze Gate",
            "resource_type": "quality_gate", "layer": "quality",
            "tooltip": "<b>Bronze Quality Gate</b><br/>Soda Core + PySpark<br/>11 checks: schema, nulls, freshness, duplicates<br/>Blocks pipeline on FAIL",
        },
        {
            "id": "airflow.silver_transform", "name": "Airflow: silver_transform",
            "resource_type": "airflow_task", "layer": "orchestration",
            "tooltip": "<b>Airflow Task: silver_transform</b><br/>spark-submit via docker exec<br/>SHA-256 deduplication, Delta MERGE",
        },
        {
            "id": "infra.silver_delta", "name": "Silver Delta Table",
            "resource_type": "delta_table", "layer": "silver",
            "tooltip": "<b>Silver Delta Table</b><br/>s3a://silver/ecommerce_events/<br/>Deduplicated + quality-gated<br/>+event_key, product_category, price_bucket",
        },
        {
            "id": "airflow.silver_quality_gate", "name": "Soda: Silver Gate",
            "resource_type": "quality_gate", "layer": "quality",
            "tooltip": "<b>Silver Quality Gate</b><br/>Soda Core + PySpark<br/>Uniqueness, null rates, referential integrity<br/>Blocks dbt layer on FAIL",
        },
    ]

    for n in nodes:
        meta = LAYER_META[n["layer"]]
        n["color"] = meta["color"]
        n["val"] = meta["size"]

    edges = [
        ("infra.kaggle_csv",            "airflow.ingest_raw"),
        ("airflow.ingest_raw",          "infra.minio_raw"),
        ("infra.minio_raw",             "airflow.bronze_transform"),
        ("airflow.bronze_transform",    "infra.bronze_delta"),
        ("infra.bronze_delta",          "airflow.bronze_quality_gate"),
        ("airflow.bronze_quality_gate", "airflow.silver_transform"),
        ("airflow.silver_transform",    "infra.silver_delta"),
        ("infra.silver_delta",          "airflow.silver_quality_gate"),
        ("infra.silver_delta",          "source.silver.ecommerce_events"),
        ("airflow.silver_quality_gate", "source.silver.ecommerce_events"),
    ]

    links = []
    nodes_map = {n["id"]: n for n in nodes}
    for src, tgt in edges:
        src_n = nodes_map.get(src, {})
        meta = LAYER_META.get(src_n.get("layer", "orchestration"), LAYER_META["orchestration"])
        links.append({
            "source": src, "target": tgt,
            "particle_speed": meta["particle_speed"],
            "particle_width": meta["particle_width"],
            "particle_color": meta["color"],
        })

    return nodes, links


def build_static_fallback():
    infra_nodes, infra_links = build_infrastructure_nodes()

    dbt_raw = [
        {
            "id": "source.silver.ecommerce_events",
            "name": "silver.ecommerce_events",
            "resource_type": "source", "layer": "silver",
            "tooltip": "<b>silver.ecommerce_events</b><br/>dbt Source<br/>Layer: SILVER<br/>Quality-gated Delta Lake<br/>Freshness warn: 7d / error: 30d",
        },
        {
            "id": "model.ecommerce_lakehouse.stg_events",
            "name": "stg_events", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>stg_events</b><br/>Staging Model<br/>Deduplicates by event_key, casts types, date_key",
        },
        {
            "id": "model.ecommerce_lakehouse.stg_products",
            "name": "stg_products", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>stg_products</b><br/>Staging Model<br/>Distinct product catalogue from click events",
        },
        {
            "id": "model.ecommerce_lakehouse.int_events_enriched",
            "name": "int_events_enriched", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>int_events_enriched</b><br/>Intermediate<br/>Events + product join, price_tier, category_hierarchy",
        },
        {
            "id": "model.ecommerce_lakehouse.int_sessions",
            "name": "int_sessions", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>int_sessions</b><br/>Intermediate<br/>Session boundary detection (30-min inactivity gap)",
        },
        {
            "id": "model.ecommerce_lakehouse.int_customer_month_activity",
            "name": "int_customer_month_activity", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>int_customer_month_activity</b><br/>Intermediate<br/>Monthly cohort roll-up per customer",
        },
        {
            "id": "model.ecommerce_lakehouse.dim_customer",
            "name": "dim_customer", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>dim_customer</b><br/>Dimension<br/>SCD Type 1, lifetime spend bucket",
        },
        {
            "id": "model.ecommerce_lakehouse.dim_product",
            "name": "dim_product", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>dim_product</b><br/>Dimension<br/>Product catalogue, price bands, brand normalisation",
        },
        {
            "id": "model.ecommerce_lakehouse.dim_date",
            "name": "dim_date", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>dim_date</b><br/>Dimension<br/>Date spine: day/week/month/quarter/year, ISO weeks",
        },
        {
            "id": "model.ecommerce_lakehouse.fact_events",
            "name": "fact_events", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>fact_events</b><br/>Fact Table<br/>Grain: one enriched click event<br/>FK: dim_customer, dim_product, dim_date",
        },
        {
            "id": "model.ecommerce_lakehouse.fact_purchases",
            "name": "fact_purchases", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>fact_purchases</b><br/>Fact Table<br/>Purchase events only, revenue, session linkage",
        },
        {
            "id": "model.ecommerce_lakehouse.mart_daily_summary",
            "name": "mart_daily_summary", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>mart_daily_summary</b><br/>Analytical Mart<br/>Daily totals: events, purchases, revenue, active users",
        },
        {
            "id": "model.ecommerce_lakehouse.mart_customer_retention",
            "name": "mart_customer_retention", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>mart_customer_retention</b><br/>Analytical Mart<br/>Monthly cohort retention matrix",
        },
        {
            "id": "model.ecommerce_lakehouse.mart_category_performance",
            "name": "mart_category_performance", "resource_type": "model", "layer": "gold",
            "tooltip": "<b>mart_category_performance</b><br/>Analytical Mart<br/>MoM category revenue growth, conversion rates",
        },
        {
            "id": "exposure.ecommerce_lakehouse.daily_executive_dashboard",
            "name": "Daily Executive Dashboard", "resource_type": "exposure", "layer": "bi",
            "tooltip": "<b>Daily Executive Dashboard</b><br/>Metabase Dashboard<br/>KPIs: events, purchases, revenue, active users<br/>Audience: C-Suite",
        },
        {
            "id": "exposure.ecommerce_lakehouse.retention_analytics_dashboard",
            "name": "Retention Analytics Dashboard", "resource_type": "exposure", "layer": "bi",
            "tooltip": "<b>Retention Analytics Dashboard</b><br/>Metabase Dashboard<br/>Cohort retention curves<br/>Audience: Growth Analytics",
        },
        {
            "id": "exposure.ecommerce_lakehouse.category_performance_dashboard",
            "name": "Category Performance Dashboard", "resource_type": "exposure", "layer": "bi",
            "tooltip": "<b>Category Performance Dashboard</b><br/>Metabase Dashboard<br/>MoM category trends, top SKUs<br/>Audience: Merchandising",
        },
    ]

    for n in dbt_raw:
        meta = LAYER_META[n["layer"]]
        n["color"] = meta["color"]
        n["val"] = meta["size"]

    dbt_edges = [
        ("source.silver.ecommerce_events",                        "model.ecommerce_lakehouse.stg_events"),
        ("source.silver.ecommerce_events",                        "model.ecommerce_lakehouse.stg_products"),
        ("model.ecommerce_lakehouse.stg_events",                  "model.ecommerce_lakehouse.int_events_enriched"),
        ("model.ecommerce_lakehouse.stg_events",                  "model.ecommerce_lakehouse.int_sessions"),
        ("model.ecommerce_lakehouse.stg_events",                  "model.ecommerce_lakehouse.int_customer_month_activity"),
        ("model.ecommerce_lakehouse.stg_products",                "model.ecommerce_lakehouse.int_events_enriched"),
        ("model.ecommerce_lakehouse.int_events_enriched",         "model.ecommerce_lakehouse.dim_customer"),
        ("model.ecommerce_lakehouse.int_events_enriched",         "model.ecommerce_lakehouse.dim_product"),
        ("model.ecommerce_lakehouse.int_events_enriched",         "model.ecommerce_lakehouse.fact_events"),
        ("model.ecommerce_lakehouse.int_events_enriched",         "model.ecommerce_lakehouse.fact_purchases"),
        ("model.ecommerce_lakehouse.int_sessions",                "model.ecommerce_lakehouse.fact_events"),
        ("model.ecommerce_lakehouse.stg_events",                  "model.ecommerce_lakehouse.dim_date"),
        ("model.ecommerce_lakehouse.fact_events",                 "model.ecommerce_lakehouse.mart_daily_summary"),
        ("model.ecommerce_lakehouse.fact_purchases",              "model.ecommerce_lakehouse.mart_daily_summary"),
        ("model.ecommerce_lakehouse.dim_customer",                "model.ecommerce_lakehouse.mart_customer_retention"),
        ("model.ecommerce_lakehouse.int_customer_month_activity", "model.ecommerce_lakehouse.mart_customer_retention"),
        ("model.ecommerce_lakehouse.fact_purchases",              "model.ecommerce_lakehouse.mart_category_performance"),
        ("model.ecommerce_lakehouse.dim_product",                 "model.ecommerce_lakehouse.mart_category_performance"),
        ("model.ecommerce_lakehouse.mart_daily_summary",          "exposure.ecommerce_lakehouse.daily_executive_dashboard"),
        ("model.ecommerce_lakehouse.mart_customer_retention",
         "exposure.ecommerce_lakehouse.retention_analytics_dashboard"),
        ("model.ecommerce_lakehouse.mart_category_performance",
         "exposure.ecommerce_lakehouse.category_performance_dashboard"),
    ]

    dbt_nodes_map = {n["id"]: n for n in dbt_raw}
    dbt_links = []
    for src, tgt in dbt_edges:
        src_n = dbt_nodes_map.get(src, {})
        meta = LAYER_META.get(src_n.get("layer", "gold"), LAYER_META["gold"])
        dbt_links.append({
            "source": src, "target": tgt,
            "particle_speed": meta["particle_speed"],
            "particle_width": meta["particle_width"],
            "particle_color": meta["color"],
        })

    return infra_nodes + dbt_raw, infra_links + dbt_links


def main():
    parser = argparse.ArgumentParser(description="Generate 3D lineage graph data")
    parser.add_argument("--manifest", default=str(PROJECT_ROOT / "dbt" / "target" / "manifest.json"))
    parser.add_argument("--out", default=str(PROJECT_ROOT / "docs" / "graph_data.json"))
    parser.add_argument("--force-static", action="store_true")
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[generate_graph] Output -> {out_path}")

    if not args.force_static and manifest_path.exists():
        print(f"[generate_graph] Reading manifest: {manifest_path}")
        try:
            dbt_nodes, dbt_links = parse_dbt_manifest(manifest_path)
            infra_nodes, infra_links = build_infrastructure_nodes()
            all_nodes_map = {n["id"]: n for n in infra_nodes}
            for n in dbt_nodes:
                all_nodes_map[n["id"]] = n
            nodes = list(all_nodes_map.values())
            links = infra_links + dbt_links
            source = "dbt manifest.json + static infrastructure"
        except Exception as exc:
            print(f"[generate_graph] WARNING: manifest parse failed ({exc}), using static fallback")
            nodes, links = build_static_fallback()
            source = "static fallback (manifest parse error)"
    else:
        reason = "--force-static" if args.force_static else f"no manifest at {manifest_path}"
        print(f"[generate_graph] Using static fallback ({reason})")
        nodes, links = build_static_fallback()
        source = "static fallback"

    node_ids = {n["id"] for n in nodes}
    valid_links = [link_item for link_item in links if link_item["source"] in node_ids and link_item["target"] in node_ids]

    layer_counts = {}
    for n in nodes:
        layer_counts[n["layer"]] = layer_counts.get(n["layer"], 0) + 1

    graph = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": source,
            "node_count": len(nodes),
            "link_count": len(valid_links),
            "layer_counts": layer_counts,
            "project": "E-Commerce Data Lakehouse",
        },
        "nodes": nodes,
        "links": valid_links,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(graph, f, indent=2)

    print(f"[generate_graph] Written {len(nodes)} nodes, {len(valid_links)} edges -> {out_path}")
    print(f"[generate_graph] Layers: {layer_counts}")


if __name__ == "__main__":
    main()
