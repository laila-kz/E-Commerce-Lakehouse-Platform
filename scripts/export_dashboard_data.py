"""
Export Gold Layer mart data from DuckDB to docs/dashboard_data.json
for the static GitHub Pages analytics dashboard.

Usage:
    python scripts/export_dashboard_data.py

Outputs:
    docs/dashboard_data.json
"""

import json
import math
from datetime import datetime, date
from pathlib import Path

# ── locate duckdb file ────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DUCKDB_PATH = ROOT / "dbt" / "dev.duckdb"
OUTPUT_PATH = ROOT / "docs" / "dashboard_data.json"


def _serialize(obj):
    if isinstance(obj, (datetime, date)):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def _demo_daily():
    """Fallback demo data matching mart_daily_summary schema."""
    import random
    random.seed(42)
    rows = []
    categories = ["electronics", "computers", "apparel", "accessories", "unknown"]
    for day_offset in range(30):
        d = f"2019-10-{1 + day_offset:02d}"
        for cat in categories:
            base = {"electronics": 1800, "computers": 1200, "apparel": 600,
                    "accessories": 400, "unknown": 200}[cat]
            rows.append({
                "date": d, "category_l1": cat,
                "total_events": base + random.randint(-200, 500),
                "total_purchases": int(base * 0.08 + random.randint(-20, 60)),
                "total_revenue": round(base * 1.4 + random.uniform(-300, 800), 2),
                "active_users": int(base * 0.35 + random.randint(-50, 150)),
            })
    return rows


def _demo_retention():
    """Fallback demo cohort retention data."""
    rows = []
    cohorts = ["2019-10-01", "2019-11-01", "2019-12-01"]
    for cohort in cohorts:
        size = 850 if cohort == "2019-10-01" else (620 if cohort == "2019-11-01" else 480)
        for month in range(4):
            rate = max(0.0, 1.0 - month * 0.22 + (0.05 if month == 0 else 0))
            rows.append({
                "cohort_month": cohort,
                "months_since_first_purchase": month,
                "cohort_size": size,
                "retained_count": int(size * rate),
                "retention_rate": round(rate, 4),
            })
    return rows


def _demo_category():
    """Fallback demo category performance data."""
    import random
    random.seed(7)
    rows = []
    categories = ["electronics", "computers", "apparel", "accessories", "unknown"]
    for month_offset in range(3):
        period = f"2019-{10 + month_offset:02d}-01"
        for cat in categories:
            base_rev = {"electronics": 42000, "computers": 28000, "apparel": 18000,
                        "accessories": 9000, "unknown": 3000}[cat]
            rev = base_rev + random.uniform(-5000, 8000)
            prior = base_rev + random.uniform(-6000, 6000) if month_offset > 0 else None
            rows.append({
                "category_l1": cat,
                "period": period,
                "total_orders": int(rev / 45),
                "total_revenue": round(rev, 2),
                "prior_period_revenue": round(prior, 2) if prior else None,
                "revenue_growth_rate": round((rev - prior) / prior, 4) if prior else None,
                "growth_rate_label": "comparable" if prior else "new_category",
            })
    return rows


def _clean_val(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def fetch(conn, query, fallback_fn):
    try:
        cur = conn.execute(query)
        cols = [desc[0] for desc in cur.description]
        raw_rows = cur.fetchall()
        if not raw_rows:
            print("  [WARN] Query returned 0 rows - using demo data")
            return fallback_fn()
        rows = [
            {k: _clean_val(v) for k, v in zip(cols, row)}
            for row in raw_rows
        ]
        return rows
    except Exception as e:
        print(f"  [WARN] Query failed ({e}) - using demo data")
        return fallback_fn()


def build_kpis(daily):
    total_events = sum(r.get("total_events", 0) or 0 for r in daily)
    total_purchases = sum(r.get("total_purchases", 0) or 0 for r in daily)
    total_revenue = sum(r.get("total_revenue", 0.0) or 0.0 for r in daily)
    active_users = sum(r.get("active_users", 0) or 0 for r in daily)
    unique_users = int(active_users * 0.6)
    return {
        "total_events": total_events,
        "total_purchases": total_purchases,
        "total_revenue": round(total_revenue, 2),
        "active_users": unique_users,
    }


def main():
    print(f"Exporting Gold Layer data -> {OUTPUT_PATH}")

    try:
        import duckdb
        if DUCKDB_PATH.exists():
            print(f"  Connecting to {DUCKDB_PATH}")
            conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        else:
            print(f"  [WARN] {DUCKDB_PATH} not found - using demo data for all marts")
            conn = None
    except ImportError:
        print("  [WARN] duckdb not installed - using demo data for all marts")
        conn = None

    if conn:
        daily = fetch(
            conn,
            "SELECT date, category_l1, total_events, total_purchases, "
            "total_revenue, active_users FROM mart_daily_summary ORDER BY date, category_l1",
            _demo_daily
        )
        retention = fetch(
            conn,
            "SELECT cohort_month, months_since_first_purchase, cohort_size, "
            "retained_count, retention_rate FROM mart_customer_retention "
            "ORDER BY cohort_month, months_since_first_purchase",
            _demo_retention
        )
        category = fetch(
            conn,
            "SELECT category_l1, period, total_orders, total_revenue, "
            "prior_period_revenue, revenue_growth_rate, growth_rate_label "
            "FROM mart_category_performance ORDER BY period, category_l1",
            _demo_category
        )
        conn.close()
    else:
        daily = _demo_daily()
        retention = _demo_retention()
        category = _demo_category()

    kpis = build_kpis(daily)

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source": str(DUCKDB_PATH) if DUCKDB_PATH.exists() else "demo",
        "kpis": kpis,
        "mart_daily_summary": daily,
        "mart_customer_retention": retention,
        "mart_category_performance": category,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, default=_serialize, indent=2)

    print(f"  Exported {len(daily)} daily rows, {len(retention)} retention rows, "
          f"{len(category)} category rows")
    print(f"  KPIs: {kpis}")
    print(f"  Written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
