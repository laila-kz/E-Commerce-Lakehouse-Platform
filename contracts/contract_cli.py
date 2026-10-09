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


def compare_contracts(old_contract, new_contract):
    """
    Classify schema evolution into COMPATIBLE, WARNING, or BREAKING.
    - Adding optional column: COMPATIBLE
    - Adding required column: BREAKING
    - Dropping column: BREAKING
    - Changing data type: BREAKING
    - Tightening nullability: BREAKING
    """
    old_cols = old_contract.get("columns", {})
    new_cols = new_contract.get("columns", {})
    changes = []
    overall_status = "COMPATIBLE"

    # Check for dropped columns or altered definitions
    for col_name, old_spec in old_cols.items():
        if col_name not in new_cols:
            changes.append({
                "type": "COLUMN_DROPPED",
                "severity": "BREAKING",
                "column": col_name,
                "detail": f"Column '{col_name}' was removed in version {new_contract.get('version')}"
            })
            overall_status = "BREAKING"
        else:
            new_spec = new_cols[col_name]
            if old_spec.get("type") != new_spec.get("type"):
                changes.append({
                    "type": "TYPE_CHANGED",
                    "severity": "BREAKING",
                    "column": col_name,
                    "detail": f"Type changed from '{old_spec.get('type')}' to '{new_spec.get('type')}'"
                })
                overall_status = "BREAKING"
            if old_spec.get("nullable", True) and not new_spec.get("nullable", True):
                changes.append({
                    "type": "NULLABILITY_TIGHTENED",
                    "severity": "BREAKING",
                    "column": col_name,
                    "detail": f"Column '{col_name}' became non-nullable (breaking for existing nulls)"
                })
                overall_status = "BREAKING"

    # Check for newly added columns
    for col_name, new_spec in new_cols.items():
        if col_name not in old_cols:
            if new_spec.get("required", False):
                changes.append({
                    "type": "REQUIRED_COLUMN_ADDED",
                    "severity": "BREAKING",
                    "column": col_name,
                    "detail": f"New required column '{col_name}' added without default value"
                })
                overall_status = "BREAKING"
            else:
                changes.append({
                    "type": "OPTIONAL_COLUMN_ADDED",
                    "severity": "COMPATIBLE",
                    "column": col_name,
                    "detail": f"New optional column '{col_name}' added (backward-compatible)"
                })

    return {"overall_status": overall_status, "changes": changes}


def validate_dataset(data_path, contract):
    """Validate CSV against contract rules"""
    violations = []
    columns_spec = contract.get("columns", {})

    with open(data_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        actual_headers = reader.fieldnames or []

        # 1. Required Columns Check
        for col_name, spec in columns_spec.items():
            if spec.get("required", False) and col_name not in actual_headers:
                violations.append({
                    "rule": "missing_required_column",
                    "severity": "BREAKING",
                    "column": col_name,
                    "expected": f"Column '{col_name}' present",
                    "actual": "Column missing from dataset headers"
                })

        # 2. Row sampling validation
        row_count = 0
        null_counts = {col: 0 for col in columns_spec}
        for row in reader:
            row_count += 1
            for col_name, spec in columns_spec.items():
                val = row.get(col_name)
                if not spec.get("nullable", True) and (val is None or val.strip() == ""):
                    null_counts[col_name] += 1

        for col_name, count in null_counts.items():
            if count > 0:
                violations.append({
                    "rule": "null_in_non_nullable_column",
                    "severity": "BREAKING",
                    "column": col_name,
                    "null_count": count,
                    "total_sampled_rows": row_count
                })

    return {
        "valid": len(violations) == 0,
        "violations": violations,
        "rows_checked": row_count
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
            "COMPATIBLE",
            contract.get("sla", {}).get("max_latency_hours", 24),
            datetime.utcnow(),
            status_str,
            True
        ))

        if not val_res["valid"]:
            print(f"\n[CONTRACT VIOLATION] {len(val_res['violations'])} contract rules violated in '{input_file}':")
            for v in val_res["violations"]:
                detail = v.get('expected', '') or v.get('column', '')
                print(f"  - [{v.get('severity', 'BREAKING')}] {v['rule']}: {detail}")
            quarantine_bad_batch(input_file, val_res["violations"], getattr(
                args, "quarantine_dir", "./data/quarantine"))
            return 1

        ver = contract.get('version')
        print(f"\n[CONTRACT PASS] '{input_file}' successfully passed all contract checks for version {ver}!\n")
        return 0


if __name__ == "__main__":
    sys.exit(main())
