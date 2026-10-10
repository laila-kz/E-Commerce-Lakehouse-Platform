"""Failure-injection tests for the Silver post-MERGE / pre-watermark boundary,
the quality-gate exit code, and the DAG dependency order that runs the Bronze
gate before Silver.

Runs the real Delta MERGE + watermark logic against local ``tmp_path`` using the
session-scoped Spark fixture, plus pure-Python checks of the gate and DAG.
"""

import ast
import logging
from datetime import datetime
from pathlib import Path

import pytest

from checks import run_quality_gate
from spark_jobs.silver_transform import (
    advance_watermark,
    ensure_silver_table_exists,
    maybe_inject_crash_after_merge,
    merge_into_silver,
    read_latest_version,
    read_watermark,
    verify_merge_success,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = REPO_ROOT / "airflow" / "dags" / "lakehouse_pipeline.py"

SILVER_COLUMNS = ["event_key", "user_id", "event_time", "event_date"]
CRASH_FLAGS = [
    "SIMULATE_CRASH_AFTER_MERGE",
    "FAIL_AFTER_SILVER_MERGE",
    "FAIL_BEFORE_WATERMARK_UPDATE",
]


@pytest.fixture()
def logger():
    log = logging.getLogger("test_failure_recovery_silver")
    log.propagate = False
    log.setLevel(logging.INFO)
    if not log.handlers:
        log.addHandler(logging.NullHandler())
    return log


def _batch(spark, rows):
    return spark.createDataFrame(rows, SILVER_COLUMNS)


def test_crash_after_merge_keeps_watermark_and_replay_inserts_nothing(
    spark, logger, tmp_path, monkeypatch
):
    for flag in CRASH_FLAGS:
        monkeypatch.delenv(flag, raising=False)

    silver_path = str(tmp_path / "silver")
    batch1 = _batch(spark, [("k1", 1, datetime(2024, 1, 1, 10, 0, 0), "2024-01-01")])
    ensure_silver_table_exists(spark, silver_path, batch1, logger)
    advance_watermark(spark, silver_path, batch1, "batch-1", logger)
    watermark_before = read_watermark(spark, silver_path, logger)
    assert watermark_before == datetime(2024, 1, 1, 10, 0, 0)

    batch2 = _batch(spark, [("k2", 2, datetime(2024, 1, 1, 12, 0, 0), "2024-01-01")])
    version_before = read_latest_version(spark, silver_path)

    monkeypatch.setenv("SIMULATE_CRASH_AFTER_MERGE", "true")
    expected_new = merge_into_silver(spark, silver_path, batch2, logger)
    assert expected_new == 1
    verification = verify_merge_success(
        spark, silver_path, expected_new, logger, version_before=version_before
    )
    assert verification["verified"] is True

    with pytest.raises(RuntimeError):
        maybe_inject_crash_after_merge(logger, "batch-2-crash")

    # The MERGE committed, but the watermark must be untouched by the crash.
    assert spark.read.format("delta").load(silver_path).count() == 2
    assert read_watermark(spark, silver_path, logger) == watermark_before

    # Recovery re-run: idempotent MERGE inserts 0 rows, then advances the watermark.
    monkeypatch.delenv("SIMULATE_CRASH_AFTER_MERGE")
    version_before_retry = read_latest_version(spark, silver_path)
    replay_expected = merge_into_silver(spark, silver_path, batch2, logger)
    assert replay_expected == 0
    replay_verification = verify_merge_success(
        spark, silver_path, replay_expected, logger, version_before=version_before_retry
    )
    assert replay_verification["verified"] is True

    advance_watermark(spark, silver_path, batch2, "batch-2-retry", logger)
    assert read_watermark(spark, silver_path, logger) == datetime(2024, 1, 1, 12, 0, 0)
    assert spark.read.format("delta").load(silver_path).count() == 2


def test_silver_crash_hook_is_off_by_default(logger, monkeypatch):
    for flag in CRASH_FLAGS:
        monkeypatch.delenv(flag, raising=False)
    maybe_inject_crash_after_merge(logger, "no-op")  # must not raise


@pytest.mark.parametrize("flag", CRASH_FLAGS)
def test_each_silver_crash_flag_raises(logger, monkeypatch, flag):
    for other in CRASH_FLAGS:
        monkeypatch.delenv(other, raising=False)
    monkeypatch.setenv(flag, "true")
    with pytest.raises(RuntimeError):
        maybe_inject_crash_after_merge(logger, "batch-x")


def test_quality_gate_exit_code_is_nonzero_when_any_suite_fails():
    assert run_quality_gate.aggregate_results(
        [{"passed": True}, {"passed": True}]
    )["overall_exit_code"] == 0

    failed = run_quality_gate.aggregate_results([{"passed": True}, {"passed": False}])
    assert failed["overall_exit_code"] == 1
    assert failed["overall_status"] == "FAILED"
    assert failed["summary"] == {"total": 2, "passed": 1, "failed": 1}


def _call_name(node):
    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return func.attr
    return None


def _operand_name(node):
    if isinstance(node, ast.Name):
        return node.id
    return ast.unparse(node)


def _flatten_chain(node):
    """Flatten a left-associative ``a >> b >> c`` expression into ['a', 'b', 'c']."""
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
        return _flatten_chain(node.left) + [_operand_name(node.right)]
    return [_operand_name(node)]


def _parse_dag_dependencies(source):
    """Return (group_members, dependency_chains) parsed from the DAG source."""
    tree = ast.parse(source)

    group_members = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.With):
            for item in node.items:
                if _call_name(item.context_expr) != "TaskGroup":
                    continue
                if not isinstance(item.optional_vars, ast.Name):
                    continue
                group = item.optional_vars.id
                members = set()
                for stmt in node.body:
                    if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call):
                        if _call_name(stmt.value) == "BashOperator":
                            for kw in stmt.value.keywords:
                                if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                                    members.add(kw.value.value)
                group_members[group] = members

    chains = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.BinOp)
            and isinstance(node.value.op, ast.RShift)
        ):
            chains.append(_flatten_chain(node.value))

    return group_members, chains


def _expand_edges(group_members, chains):
    """Expand group-level edges into concrete task-to-task edges."""
    def members(name):
        return group_members.get(name, {name})

    edges = {}
    for chain in chains:
        for upstream, downstream in zip(chain, chain[1:]):
            for src in members(upstream):
                for dst in members(downstream):
                    edges.setdefault(src, set()).add(dst)
    return edges


def _reachable(edges, start):
    seen = set()
    stack = [start]
    while stack:
        node = stack.pop()
        for nxt in edges.get(node, ()):
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen


def test_dag_runs_bronze_gate_before_silver():
    group_members, chains = _parse_dag_dependencies(DAG_PATH.read_text(encoding="utf-8"))

    assert group_members.get("bronze_stage") == {"bronze_transform", "bronze_quality_gate"}
    assert group_members.get("silver_stage") == {"silver_transform", "silver_quality_gate"}

    edges = _expand_edges(group_members, chains)

    # Bronze gate is downstream of bronze transform and upstream of the Silver
    # transform, so a failed gate halts Silver.
    assert "silver_transform" in _reachable(edges, "bronze_quality_gate")
    assert "bronze_quality_gate" in _reachable(edges, "bronze_transform")
