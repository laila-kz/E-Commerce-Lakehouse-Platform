#!/usr/bin/env python3
"""
Enterprise Data Contract Engine (lakehouse-contract-cli)
- Validates datasets against versioned YAML contracts
- Analyzes contract evolution compatibility (BREAKING, WARNING, COMPATIBLE)
- Automatically routes violating batches to dead-letter quarantine
- Maintains a persistent schema registry table in DuckDB
"""

import argparse
import csv
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import urllib.request
import urllib.error

import duckdb

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def load_yaml(yaml_path):
    """Load YAML file with PyYAML or robust fallback parser"""
    try:
        import yaml
        with open(yaml_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except ImportError:
        contract = {"columns": {}, "sla": {}}
        current_col = None
        with open(yaml_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("dataset_name:"):
                    contract["dataset_name"] = line.split(":", 1)[1].strip().strip('"\'')
                elif line.startswith("version:"):
                    contract["version"] = line.split(":", 1)[1].strip().strip('"\'')
                elif line.startswith("owner:"):
                    contract["owner"] = line.split(":", 1)[1].strip().strip('"\'')
                elif line.startswith("description:"):
                    contract["description"] = line.split(":", 1)[1].strip().strip('"\'')
                elif line.endswith(":") and not line.startswith("sla:") and not line.startswith("columns:"):
                    current_col = line[:-1].strip()
                    contract["columns"][current_col] = {"allowed_values": []}
                elif current_col and ":" in line:
                    k, v = line.split(":", 1)
                    k, v = k.strip(), v.strip().strip('"\'')
                    if k == "required":
                        contract["columns"][current_col]["required"] = (v.lower() == "true")
                    elif k == "nullable":
                        contract["columns"][current_col]["nullable"] = (v.lower() == "true")
                    elif k == "type":
                        contract["columns"][current_col]["type"] = v
                    elif k == "min_value":
                        contract["columns"][current_col]["min_value"] = float(v)
                elif current_col and line.startswith("- "):
                    val = line[2:].strip().strip('"\'')
                    contract["columns"][current_col]["allowed_values"].append(val)
        return contract


def get_duckdb_connection():
    db_path = os.environ.get("DUCKDB_PATH", os.path.join(os.getcwd(), "dbt", "dev.duckdb"))
    return duckdb.connect(db_path if os.path.exists(db_path) else ":memory:")


def init_registry_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS contract_registry (
            dataset_name VARCHAR,
            version VARCHAR,
            owner VARCHAR,
            compatibility_status VARCHAR,
            sla_max_latency_hours INTEGER,
            last_validated_at TIMESTAMP,
            validation_status VARCHAR,
            active_flag BOOLEAN
        )
    """)


_SEVERITY_RANK = {"COMPATIBLE": 0, "WARNING": 1, "BREAKING": 2}


def _worst_severity(severities):
    return max(severities, key=lambda s: _SEVERITY_RANK[s])


def compare_contracts(old_contract, new_contract):
    """
    Classify schema evolution into COMPATIBLE, WARNING, or BREAKING.
    - Adding nullable column with a default value:    COMPATIBLE
    - Adding optional column without a default value: WARNING
    - Adding required column:                         BREAKING
    - Dropping column / renaming (drop+add):          BREAKING
    - Changing data type:                             BREAKING
    - Tightening nullability (nullable->non-nullable): BREAKING

    Overall status = the worst severity reported by any single change.
    """
    old_cols = old_contract.get("columns", {})
    new_cols = new_contract.get("columns", {})
    changes = []
    severities = ["COMPATIBLE"]

    # Check for dropped/deprecated columns or altered definitions
    for col_name, old_spec in old_cols.items():
        if col_name not in new_cols:
            changes.append({
                "type": "COLUMN_DROPPED",
                "severity": "BREAKING",
                "column": col_name,
                "detail": f"Column '{col_name}' was removed in version {new_contract.get('version')}"
            })
            severities.append("BREAKING")
        else:
            new_spec = new_cols[col_name]
            if old_spec.get("type") != new_spec.get("type"):
                changes.append({
                    "type": "TYPE_CHANGED",
                    "severity": "BREAKING",
                    "column": col_name,
                    "detail": f"Type changed from '{old_spec.get('type')}' to '{new_spec.get('type')}'"
                })
                severities.append("BREAKING")
            if old_spec.get("nullable", True) and not new_spec.get("nullable", True):
                changes.append({
                    "type": "NULLABILITY_TIGHTENED",
                    "severity": "BREAKING",
                    "column": col_name,
                    "detail": f"Column '{col_name}' became non-nullable (breaking for existing nulls)"
                })
                severities.append("BREAKING")

    # Check for newly added columns
    for col_name, new_spec in new_cols.items():
        if col_name in old_cols:
            continue
        if new_spec.get("required", False):
            changes.append({
                "type": "REQUIRED_COLUMN_ADDED",
                "severity": "BREAKING",
                "column": col_name,
                "detail": f"New required column '{col_name}' added without default value"
            })
            severities.append("BREAKING")
        elif "default" in new_spec:
            changes.append({
                "type": "OPTIONAL_COLUMN_ADDED",
                "severity": "COMPATIBLE",
                "column": col_name,
                "detail": f"New nullable column '{col_name}' added with a default value (backward-compatible)"
            })
        else:
            changes.append({
                "type": "OPTIONAL_COLUMN_ADDED_NO_DEFAULT",
                "severity": "WARNING",
                "column": col_name,
                "detail": f"New optional column '{col_name}' added without a default value "
                          f"- existing rows will be NULL (non-breaking but be aware)"
            })
            severities.append("WARNING")

    return {"overall_status": _worst_severity(severities), "changes": changes}


_NUMERIC_TYPES = {"long", "int", "integer", "bigint", "double", "float", "decimal", "number"}


def _trunc(value, limit=64):
    """Truncate a raw value for diagnostics (protects against PII over-capture)."""
    s = str(value)
    return s if len(s) <= limit else s[:limit] + "…"


def _coerce(value, ctype):
    """Cast a raw CSV value to the contract-declared type (raises on mismatch)."""
    s = value.strip()
    if ctype in ("string", "str", "varchar"):
        return s
    if ctype in ("long", "int", "integer", "bigint"):
        return int(s)
    if ctype in ("double", "float", "decimal", "number"):
        return float(s)
    if ctype in ("timestamp", "datetime", "date"):
        return datetime.fromisoformat(s.replace(" ", "T").replace("Z", "+00:00"))
    raise ValueError(f"Unsupported contract type '{ctype}' for coercion")


def validate_dataset(data_path, contract):
    """Validate CSV against contract rules (headers, nullability, types, allowed
    values, numeric bounds). Emits one violation record per (column, rule) pair
    with a failed-row count and a truncated example value."""
    columns_spec = contract.get("columns", {})
    violations_agg = {}
    rows_checked = 0

    def add_violation(column, rule, count=1, example=None):
        record = violations_agg.setdefault((column, rule), {
            "column": column,
            "rule": rule,
            "severity": "BREAKING",
            "failed_rows": 0,
            "example": None,
        })
        record["failed_rows"] += count
        if example is not None and record["example"] is None:
            record["example"] = _trunc(example)

    with open(data_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        actual_headers = reader.fieldnames or []

        # 1. Required columns must exist in the dataset header
        for col_name, spec in columns_spec.items():
            if spec.get("required", False) and col_name not in actual_headers:
                add_violation(col_name, "missing_required_column", count=0)

        # 2. Row-level rules: nullability, types, allowed values, numeric bounds
        for row in reader:
            rows_checked += 1
            for col_name, spec in columns_spec.items():
                if col_name not in actual_headers:
                    # Entire column missing - already reported by the required-columns check.
                    continue
                raw = row.get(col_name)
                if raw is None or raw.strip() == "":
                    if not spec.get("nullable", True):
                        add_violation(col_name, "null_in_non_nullable_column", example="")
                    continue
                try:
                    coerced = _coerce(raw, spec.get("type", "string"))
                except Exception:
                    add_violation(col_name, "type_mismatch", example=raw)
                    continue
                allowed = spec.get("allowed_values")
                if allowed and raw not in allowed:
                    add_violation(col_name, "not_in_allowed_values", example=raw)
                if spec.get("type", "string") in _NUMERIC_TYPES:
                    if spec.get("min_value") is not None and coerced < spec.get("min_value"):
                        add_violation(col_name, "below_min_value", example=raw)
                    if spec.get("max_value") is not None and coerced > spec.get("max_value"):
                        add_violation(col_name, "above_max_value", example=raw)

    violations = []
    for record in violations_agg.values():
        record.setdefault("failed_rows", 0)
        if record["rule"] == "missing_required_column" and record["failed_rows"] == 0:
            record["failed_rows"] = rows_checked
        violations.append(record)

    return {
        "valid": len(violations) == 0,
        "violations": violations,
        "rows_checked": rows_checked
    }


def quarantine_bad_batch(data_path, violations, quarantine_dir="./data/quarantine"):
    os.makedirs(quarantine_dir, exist_ok=True)
    filename = Path(data_path).name
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    quarantine_meta_path = os.path.join(quarantine_dir, f"quarantine_{timestamp}_{filename}.json")

    with open(quarantine_meta_path, "w") as f:
        json.dump({
            "source_file": data_path,
            "quarantined_at": datetime.utcnow().isoformat() + "Z",
            "violations": violations
        }, f, indent=2)

    print(f"[QUARANTINE] Violating batch quarantined with metadata: {quarantine_meta_path}")
    return quarantine_meta_path


def send_slack_alert(message_text):
    """Post a breaking-batch alert to Slack. No-op (with a logged warning) when
    SLACK_WEBHOOK_URL is not set; never raises even if the post fails."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url or webhook_url.startswith("<"):
        print("[SLACK] SLACK_WEBHOOK_URL not set - skipping Slack alert (no-op).")
        return False
    try:
        payload = json.dumps({"text": message_text}).encode("utf-8")
        request = urllib.request.Request(
            webhook_url, data=payload,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status in (200, 202, 204):
                print("[SLACK] Breaking-batch alert sent.")
                return True
            print(f"[SLACK] Webhook returned unexpected status {response.status}.")
    except (urllib.error.URLError, OSError, ValueError) as exc:
        print(f"[SLACK] Failed to send alert (non-fatal): {exc}")
    return False


def main():
    parser = argparse.ArgumentParser(description="Enterprise Data Contract CLI Engine")
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands (contract definitions in contracts/schemas/)")

    # Command: validate
    val_parser = subparsers.add_parser("validate", help="Validate a data file against a contract")
    val_parser.add_argument("--input-file", required=True, help="Path to input data file")
    val_parser.add_argument("--contract-file", required=True, help="Path to YAML contract specification (fixtures in contracts/schemas/)")
    val_parser.add_argument("--quarantine-dir", default="./data/quarantine", help="Quarantine directory")

    # Command: diff
    diff_parser = subparsers.add_parser("diff", help="Compare two contract versions for compatibility")
    diff_parser.add_argument("--old-contract", required=True, help="Path to base contract YAML")
    diff_parser.add_argument("--new-contract", required=True, help="Path to target contract YAML (fixtures in contracts/schemas/)")

    # Command: registry
    subparsers.add_parser("registry", help="Display registered dataset contracts and status")

    args = parser.parse_args()

    if args.command == "diff":
        old_c = load_yaml(args.old_contract)
        new_c = load_yaml(args.new_contract)
        diff_res = compare_contracts(old_c, new_c)
        print("\n" + "=" * 70)
        print(f" CONTRACT COMPATIBILITY REPORT: {old_c.get('version')} -> {new_c.get('version')}")
        print("=" * 70)
        print(f"Overall Classification: [{diff_res['overall_status']}]")
        print("-" * 70)
        for ch in diff_res["changes"]:
            print(f"[{ch['severity']}] {ch['type']:<22} | Column: {ch['column']:<15} | {ch['detail']}")
        print("=" * 70 + "\n")
        return 0 if diff_res["overall_status"] != "BREAKING" else 1

    elif args.command == "registry":
        con = get_duckdb_connection()
        init_registry_table(con)
        res = con.execute("SELECT * FROM contract_registry").fetchall()
        print("\n" + "=" * 95)
        print(f"{'DATASET':<20} {'VERSION':<10} {'OWNER':<25} {'COMPATIBILITY':<15} {'STATUS'}")
        print("-" * 95)
        if not res:
            print("No contracts currently registered in registry table.")
        for r in res:
            print(f"{r[0]:<20} {r[1]:<10} {r[2]:<25} {r[3]:<15} {r[6]}")
        print("=" * 95 + "\n")
        return 0

    else:
        # Default / validate command
        input_file = getattr(args, "input_file", None)
        contract_file = getattr(args, "contract_file", None)
        if not input_file or not contract_file:
            parser.print_help()
            return 1

        contract = load_yaml(contract_file)
        val_res = validate_dataset(input_file, contract)

        con = get_duckdb_connection()
        init_registry_table(con)

        status_str = "VALID" if val_res["valid"] else "VIOLATIONS_FOUND"
        con.execute("""
            INSERT INTO contract_registry VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            contract.get("dataset_name", "unknown"),
            contract.get("version", "1.0.0"),
            contract.get("owner", "data-team"),
            status_str,
            contract.get("sla", {}).get("max_latency_hours", 24),
            datetime.utcnow(),
            status_str,
            True
        ))

        if not val_res["valid"]:
            print(f"\n[CONTRACT VIOLATION] {len(val_res['violations'])} contract rule(s) violated in '{input_file}':")
            for v in val_res["violations"]:
                column = v.get("column", "?")
                example = f" (e.g. '{v['example']}')" if v.get("example") is not None else ""
                print(f"  - [{v.get('severity', 'BREAKING')}] column={column!r} rule={v['rule']}"
                      f" failed_rows={v.get('failed_rows', 0)}{example}")
            quarantine_path = quarantine_bad_batch(
                input_file, val_res["violations"],
                getattr(args, "quarantine_dir", "./data/quarantine"))
            send_slack_alert(
                f"[Contract CLI] {contract.get('dataset_name', 'unknown')} v{contract.get('version', '?')} "
                f"violated {len(val_res['violations'])} rule(s) in {input_file} - "
                f"quarantined to {quarantine_path}")
            return 1

        ver = contract.get('version')
        print(f"\n[CONTRACT PASS] '{input_file}' successfully passed all contract checks for version {ver}!\n")
        return 0


if __name__ == "__main__":
    sys.exit(main())
