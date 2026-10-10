import json
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from contracts import contract_cli as cc

DATA_DIR = ROOT / "data"
SCHEMA_DIR = ROOT / "contracts" / "schemas"

V1 = SCHEMA_DIR / "ecommerce_events_v1.yml"
V2 = SCHEMA_DIR / "ecommerce_events_v2.yml"
V2_BREAKING = SCHEMA_DIR / "ecommerce_events_v2_breaking.yml"
VALID_CSV = DATA_DIR / "test_valid.csv"
BREAKING_CSV = DATA_DIR / "test_breaking.csv"


def _run_cli(monkeypatch, tmp_path, args):
    db = tmp_path / "registry.duckdb"
    duckdb.connect(str(db)).close()
    monkeypatch.setenv("DUCKDB_PATH", str(db))
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "")
    monkeypatch.setattr(sys, "argv", ["contract_cli"] + args)
    return cc.main()


def _contract(columns):
    return {"dataset_name": "synthetic", "version": "1.0.0", "owner": "test",
            "sla": {"max_latency_hours": 24}, "columns": columns}


@pytest.mark.parametrize(
    ("csv", "expected_violations"),
    [
        (VALID_CSV, set()),
        (
            BREAKING_CSV,
            {"missing_required_column", "not_in_allowed_values", "below_min_value"},
        ),
    ],
)
def test_validate_exit_codes(monkeypatch, tmp_path, csv, expected_violations):
    result = cc.validate_dataset(str(csv), cc.load_yaml(V1))
    violations = {v["rule"] for v in result["violations"]}
    assert violations == expected_violations
    assert result["valid"] == (len(expected_violations) == 0)


def test_valid_csv_passes_with_exit_zero(monkeypatch, tmp_path):
    args = ["validate", "--input-file", str(VALID_CSV),
            "--contract-file", str(V1), "--quarantine-dir", str(tmp_path / "q")]
    assert _run_cli(monkeypatch, tmp_path, args) == 0


def test_breaking_csv_quarantines_and_registers_real_status(monkeypatch, tmp_path):
    q = tmp_path / "q"
    args = ["validate", "--input-file", str(BREAKING_CSV),
            "--contract-file", str(V1), "--quarantine-dir", str(q)]
    assert _run_cli(monkeypatch, tmp_path, args) == 1

    quarantine_files = list(q.glob("quarantine_*.json"))
    assert len(quarantine_files) == 1
    meta = json.loads(quarantine_files[0].read_text(encoding="utf-8"))
    rules = {v["rule"] for v in meta["violations"]}
    assert rules == {"missing_required_column", "not_in_allowed_values", "below_min_value"}
    assert {"column", "rule", "failed_rows"} <= set(meta["violations"][0].keys())

    con = duckdb.connect(str(tmp_path / "registry.duckdb"))
    row = con.execute(
        "SELECT compatibility_status, validation_status FROM contract_registry"
    ).fetchone()
    assert row == ("VIOLATIONS_FOUND", "VIOLATIONS_FOUND")


def test_validate_dataset_row_rules(tmp_path):
    contract = _contract({
        "n": {"type": "long", "required": True, "nullable": False},
        "s": {"type": "string", "required": True, "nullable": False,
              "allowed_values": ["a", "b"]},
        "f": {"type": "double", "required": True, "nullable": True,
              "min_value": 0.0, "max_value": 100.0},
    })
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "n,s,f\nx,a,1.5\n2,c,200.0\n,a,2.0\n",
        encoding="utf-8",
    )
    result = cc.validate_dataset(str(csv_path), contract)
    by_rule = {(v["column"], v["rule"]): v for v in result["violations"]}
    assert (("n", "type_mismatch") in by_rule)
    assert (("s", "not_in_allowed_values") in by_rule)
    assert (("f", "above_max_value") in by_rule)
    assert (("n", "null_in_non_nullable_column") in by_rule)
    assert result["rows_checked"] == 3
    assert by_rule[("n", "type_mismatch")]["example"] == "x"


def test_v1_to_v2_is_warning(monkeypatch, tmp_path):
    diff = cc.compare_contracts(cc.load_yaml(V1), cc.load_yaml(V2))
    assert diff["overall_status"] == "WARNING"
    types = {c["type"] for c in diff["changes"]}
    assert "OPTIONAL_COLUMN_ADDED_NO_DEFAULT" in types
    args = ["diff", "--old-contract", str(V1), "--new-contract", str(V2)]
    assert _run_cli(monkeypatch, tmp_path, args) == 0


def test_v1_to_v2_breaking_exits_nonzero(monkeypatch, tmp_path):
    diff = cc.compare_contracts(cc.load_yaml(V1), cc.load_yaml(V2_BREAKING))
    assert diff["overall_status"] == "BREAKING"
    types = {c["type"] for c in diff["changes"]}
    assert {"COLUMN_DROPPED", "TYPE_CHANGED", "NULLABILITY_TIGHTENED"} <= types
    args = ["diff", "--old-contract", str(V1), "--new-contract", str(V2_BREAKING)]
    assert _run_cli(monkeypatch, tmp_path, args) == 1


def test_add_nullable_with_default_is_compatible():
    base = {"id": {"type": "long", "required": True, "nullable": False}}
    old_c = _contract(base)
    new_c = _contract({
        **base,
        "tag": {"type": "string", "required": False, "nullable": True, "default": "n/a"},
    })
    assert cc.compare_contracts(old_c, new_c)["overall_status"] == "COMPATIBLE"


def test_add_optional_without_default_is_warning():
    base = {"id": {"type": "long", "required": True, "nullable": False}}
    old_c = _contract(base)
    new_c = _contract({
        **base,
        "tag": {"type": "string", "required": False, "nullable": True},
    })
    assert cc.compare_contracts(old_c, new_c)["overall_status"] == "WARNING"


def test_drop_column_is_breaking():
    old_c = _contract({"id": {"type": "long"}, "extra": {"type": "string"}})
    new_c = _contract({"id": {"type": "long"}})
    assert cc.compare_contracts(old_c, new_c)["overall_status"] == "BREAKING"


def test_type_change_is_breaking():
    old_c = _contract({"id": {"type": "long"}})
    new_c = _contract({"id": {"type": "string"}})
    assert cc.compare_contracts(old_c, new_c)["overall_status"] == "BREAKING"


def test_rename_is_breaking():
    old_c = _contract({"id": {"type": "long"}})
    new_c = _contract({"id_new": {"type": "long"}})
    assert cc.compare_contracts(old_c, new_c)["overall_status"] == "BREAKING"