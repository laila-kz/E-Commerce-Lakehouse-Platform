# Agent Handoff Brief: E-Commerce Lakehouse Remediation

**Purpose:** give a new agent everything the previous agent knew, so it can continue the remediation without re-auditing. **Use together with `Remediation_Runbook.md`** (the step-by-step plan). Paste this brief first, then the runbook's Section 0 (Global Rules), then the step you are authorizing.

**Trust rule:** this brief is a summary written from reports, not from the repo. Where it conflicts with what you see in the repo, **the repo wins**. Items marked **\[UNVERIFIED\]** or **\[UNKNOWN\]** must be checked before you rely on them.

---

## 1. The project in one paragraph

A local, production-patterned **batch** lakehouse for e-commerce clickstream: CSV → contract CLI → MinIO `raw` → PySpark Bronze Delta → Soda/boto3 quality gates → PySpark Silver Delta (SHA-256 `event_key`, insert-only MERGE, 3-day lookback, post-commit watermark) → dbt-duckdb Gold (staging views, sessionization, star schema, incremental marts) → Airflow DAG `ecommerce_lakehouse` (file `airflow/dags/lakehouse_pipeline.py`, 10 tasks) → OpenLineage/Marquez, plus a Scala Catalyst extension. It is a portfolio / interview project. The owner will present it, so **documentation must not claim anything the repo can't back**.

- Repo path (Windows): `C:\Users\kheza\Desktop\Data_Engineering\E-Commerce-Lakehouse-Platform`
- Host shell: **PowerShell** (so `uuidgen`, `head`, `cat <<EOF` etc. don't exist). Host has Python 3.11.9 and pytest 8.3.4; **no pyspark, delta-spark or flake8**.
- **Docker is unavailable** on the owner's laptop (no disk space). Never start it.
- Encoding lesson: PowerShell `Set-Content -Encoding UTF8` writes a BOM. Use `[System.IO.File]::WriteAllText(path, text, (New-Object System.Text.UTF8Encoding($false)))` or equivalent, and use LF endings for `.yml` and `.py` you create.

## 2. How we got here

1. A read-only **audit** compared README / Project_summary / docs / ADRs against the code. It found 25 findings (F-001 to F-025).
2. **Batch 0** (read-only) closed audit gaps and added F-026 to F-031, corrected F-004 (downgraded) and F-014 (timeouts do exist).
3. A remediation plan was set: Batch 1 (security), Batch S (Silver correctness), Batch A (docs), then B, E-lite, C, D, I, F, R, final re-audit. It was later renumbered into 12 steps in `Remediation_Runbook.md`.
4. Work executed so far (reported by the previous agent): Batch 1, Batch A, the F-011 formula commit. Batch S was **in progress** when the agent stopped.

## 3. Repository state (as last reported)

**Branch and tag**

- Working branch: `audit-remediation`. Rollback tag: `pre-audit-fix` at `2c45a9f`. Original branch `polish-project` still contains `a3a6479` (Batch 1 was first committed there, then `audit-remediation` was created at that tip). Do not modify `polish-project`.
- Nothing has been pushed.

**Commits on `audit-remediation` since `pre-audit-fix` (expected order)**

| Commit | Message | Content |
| --- | --- | --- |
| `a3a6479` | `fix(batch1): F-004 F-030 rename .env.exemple to .env.example, ignore audit logs` | `.env.exemple` → `.env.example` (refs updated in README, runbook, Project_summary); `.gitignore` += `audit/logs/`. **Also accidentally committed the untracked `Project_summary.md` (1,417 lines).** |
| `fac9b24` | `docs(batchA): F-009 F-010 F-011 F-012 F-017 F-020 F-021 F-022 F-023 F-024` | 8 files, 56+/45−. See section 4. |
| `8e4ee61` | `docs(batchA): F-011 precise event_key formula` | `docs/data_dictionary.md` event_key now `sha2(concat_ws("\|", user_id, event_type, product_id, event_time as UTC ISO 'yyyy-MM-dd'T'HH:mm:ss'Z''), 256)`. **\[UNVERIFIED\]** that this commit exists; the agent reported it. |

**Batch S: \[UNKNOWN\] commit status.** Last reports said:

- `spark_jobs/silver_transform.py` has an **uncommitted** change implementing within-batch dedup and a strict `verify_merge_success`, left by an earlier blocked session. The S0 review (by line cites) found it matches the spec (see section 5).
- The agent had started writing `tests/conftest.py` and `tests/...` and noted "malformed fixture, rewriting". **Files may be partial or absent. Verify.**
- `.github/workflows/silver-tests.yml` was planned; **\[UNKNOWN\]** whether it exists.
- A patch of the pre-review diff was saved **outside the repo**: `C:\Users\kheza\Desktop\Data_Engineering\silver_transform_prior.patch` (12,674 bytes).

**Working tree items that are NOT part of the work (leave unstaged)**

- `M Dockerfile.airflow`: unpins the `pip install` block (removes `--constraint`; installs `dbt-core==1.8.8`, `dbt-duckdb==1.8.4`, `dbt-adapters==1.11.0`, OpenLineage packages, `python-dotenv`). Owner hasn't said if intentional. Needed for Step 5.
- `D Upgrade_implementation_plan.md` (deleted, uncommitted).
- `?? .vscode/`, `?? audit/*.json` (two files), `?? test_probe_spark.py` (throwaway probe with hardcoded `/opt/spark/python`; **never commit**).

## 4. What each completed commit changed

**Batch 1:** `.env.example` rename; README (lines \~130, 147), `docs/EXECUTION_AND_RUNBOOK_GUIDE.md` (\~23, 27, 31), `Project_summary.md:41`; `.gitignore`. The real `.env` exists on disk (untracked, ignored, **never committed**: git history clean). The owner states the key was rotated. **Never read or print `.env`.**

**Batch A (docs only, plus one help-string code touch):**

- F-009 watermark path now `s3a://silver/ecommerce_events/_watermarks` in ADR-002 and the runbook.
- F-010 README Mermaid: "SCD Type 2 (product) + Star Schema"; `Project_summary.md` "Type 1 thin customer dimension". **Only `dim_product` is SCD2; `dim_customer` is Type 1.**
- F-011 initial formula fix (superseded by `8e4ee61`).
- F-012 Bronze lineage columns documented as `ingested_at, source_file, pipeline_run_id, batch_id, event_date`.
- F-017 `contracts/contract_cli.py` argparse help strings repointed to `contracts/schemas/` fixtures (py_compile OK).
- F-020 `user_session` non-nullable in `bronze_layer_review.md`; F-021 raw = CSV only; F-022 README "Unified 3-layer quality gate runner (Raw + Bronze + Silver)" (raw is boto3, not Soda).
- F-023 all `file:///c:/...` links → relative (23 in `Project_summary.md` plus `design_decisions.md`).
- F-024 Gold data dictionary regenerated (added `dim_customer` Type 1; dbt writes `dev.duckdb`, there's no `gold` bucket).
- Bonus: README ADR-004 line now says 84 tests.
- **Deliberately untouched:** every "exactly-once / zero duplicates" sentence (listed in section 6) because the Silver fix was pending.

## 5. The Silver fix (Batch S): what it must be

Facts established by reading the code:

- Original bug (**F-026**): `silver_transform.py` computed `event_key` and only *logged* duplicates. No `dropDuplicates`/`distinct`. The first-run path overwrites with the whole source; the MERGE is `whenNotMatchedInsertAll()` only. So duplicates **within one batch** all landed; only cross-run re-reads were deduped.
- Second bug (**F-026b**): `verify_merge_success` passed whenever `num_inserted >= 0` (always true) or the op was WRITE/CTAS, so it could never fail.
- `ingested_at` and `source_file` exist at the dedup point (written in `bronze_transform.py:266-267`, carried through `read_bronze_new_rows`, filters and `compute_event_key`).

Spec the working-tree change was reviewed against (agent reported all PASS, line cites approximate):

- (a) `within_batch_dedup`: `row_number()` over `partitionBy("event_key").orderBy(ingested_at asc, source_file asc)`, keep rank 1 (about lines 410 to 449).
- (b) called once before the first-run/MERGE split (about line 710).
- (c) logs rows before, distinct keys, rows after, rows dropped.
- (d) strict verification: MERGE path expects `numTargetRowsInserted == expected_new_keys` (distinct deduped keys absent from target via left-anti); WRITE path expects `numOutputRows == expected_rows`; raises on failure (about lines 509 to 600, 727 to 732).
- (e) watermark advance and the failure-injection block (three env aliases, about lines 734 to 751) unchanged.
- Known non-blocking risks: three extra `count()` actions (rescans) in the dedup function; possible spurious failure under a concurrent writer (single-runner design); stale `merge_into_silver` docstring; **NULL `event_key`/`ingested_at`/`source_file` behavior not yet analysed**.
- Module import is side-effect free; the only `s3a://` literals are argparse defaults.

## 6. Open findings (all 31) and where they are handled

| ID | Short description | Status | Step |
| --- | --- | --- | --- |
| F-001 | DAG uses `docker exec spark ...` but `Dockerfile.airflow` has no Docker CLI | **Open, NEEDS RUNTIME** | 10 |
| F-002 | Contract validator ignores type/allowed_values/min_value; never emits WARNING; registry hardcodes COMPATIBLE; no Slack call | Open | 6 |
| F-003 | Quarantine path chaos (`./data/quarantine` vs `s3a://raw/quarantine/` vs `s3a://bronze/_quarantine/`) | Open | 6 |
| F-004 / F-030 | Secrets hygiene | **Done** (LOW) | 1 |
| F-005 | "3.0x" benchmark is the hardcoded fallback block (`spark_benchmark.py` \~294-310, mode `EMPIRICAL_REFERENCE_PROFILE`) | Open | 4, 10 |
| F-006 | No `OpenLineageSparkListener`; emitter script references ghost columns/models | Open | 4 |
| F-007 | Test count: truth is **84 (74 generic + 10 singular)**; docs say 83/84/57/55 | Partly (one README line) | 4 |
| F-008 / F-028 | Failure flags in README don't exist; real ones are 3 aliases for **one** point | Open | 4, 8 |
| F-009 to F-012 | Watermark path, dim_customer, event_key, Bronze lineage | **Done** | A |
| F-013 | Reconciliation checks 3 Gold pairs only; SKIP exits 0 | Open | 4, 6 |
| F-014 | Retries uniform (3 / 2 min); per-task `execution_timeout` **does exist** | Open | 7 |
| F-015 | Design doc task ids and `--date {{ ds }}` not real | Open | 4 |
| F-016 | DAG spark-submit omits custom extension/jars; Scala date-range rewrite untested | Open | 7, 10 |
| F-017 | CLI help path | **Done** | A |
| F-018 | Version drift (requirements 3.5.1/3.2.0; Dockerfile.spark Spark 3.5.0 + Delta 3.1.0; build.sbt 3.5.0/3.1.0/2.12.18; sbt image 2.12.17; docs 3.5.0/3.0.0) | Open | 5 |
| F-019 | CI = flake8 + dbt parse/compile + py_compile of 6 files | Open | 9 |
| F-020 to F-024 | Misc doc drift | **Done** | A |
| F-025 | `Project_summary.md` integrity (duplicate tool dumps, wrong version claims) | Open | 9 |
| F-026 / F-026b | Silver within-batch dedup; verify can't fail | **Code in working tree \[UNVERIFIED\]** | 1, 3 |
| F-027 | README Mermaid puts contract gate after ingestion; `contract_cli.py` reads local files, is a standalone Makefile target (`test-contracts`), **not in the DAG** | Open | 4 |
| F-029 | `/workspace/...` vs `/opt/airflow/project/...` paths in the design doc | Open | 4 |
| F-031 | `Demo_guide.md` cites 57 tests; `silver_scan_output.txt:15` records a failed run (bad path) | Open | 4 |
| Slack | `lakehouse_pipeline.py:14-20` no-op fallback if `slack_alert` import fails (silent) | Open | 7 |

**"Zero duplicates / exactly-once" sentences to reword in Step 3** (re-grep for current lines): `README.md` (failure-matrix rows, about 207 to 209), `docs/EXECUTION_AND_RUNBOOK_GUIDE.md` (\~276), `docs/adr/ADR-002_hybrid_event_time_watermarking_and_lateness_buffer.md` (\~27), `docs/design_decisions.md` (\~455, \~858), `Project_summary.md` (\~529), and the `verify_merge_success` docstring.

## 7. Established facts (don't re-derive)

- **dbt tests:** 84 = 74 YAML-declared generic (not_null 54, unique 11, relationships 6, accepted_values 3) + 10 singular. "Generic, schema, custom singular" isn't a dbt taxonomy. The "55/57" figures match nothing in the repo. The offline subset count was claimed as 81 by one report and 71 generic by another: **derive it from the Makefile selector, don't trust either.**
- **Failure injection:** only one real hook, a block in `silver_transform.py` (about 734 to 751) between verified MERGE and `advance_watermark`; triggered by any of `SIMULATE_CRASH_AFTER_MERGE`, `FAIL_AFTER_SILVER_MERGE`, `FAIL_BEFORE_WATERMARK_UPDATE` = `"true"`. No ingest, Bronze or dbt hooks exist. `docs/EXECUTION_AND_RUNBOOK_GUIDE.md` (\~271) documents the Silver one correctly.
- **Quarantine today:** contract CLI writes `./data/quarantine` (default); Bronze writes Parquet to `s3a://bronze/_quarantine/batch_id=.../` (`bronze_transform.py` \~228). No `bronze_quarantine` bucket exists.
- **Lineage:** only `scripts/emit_marquez_lineage.py` (manual POST) plus Airflow transport config. Ghost entities in the script: columns `dedup_hash`, `is_valid`, `processed_at`; models `int_sessionized_events`, `int_user_daily_activity`, `dim_products`, `dim_dates`. Real: `int_events_enriched`, `int_sessions`, `int_customer_month_activity`, `dim_product`, `dim_date`.
- **Reconciliation:** `checks/run_reconciliation.py` has three Gold-only pairs; unavailable sources record `SKIP`; exit 0 at lines \~315 to 322.
- **Replay/metrics/reconciliation artifacts:** 8 `audit/replay_*.json` and 14 `audit/reconciliation_*.json` are tracked, evidence the tools ran at some point.
- **Catalyst:** Makefile (\~68 to 71) passes `spark.sql.extensions=org.lakehouse.optimizer.EcommerceSparkExtensions` and the jars (`./jars:/opt/spark/jars-extra`); the DAG prefix (\~37 to 42) passes only Delta's extension; `silver_transform.py` (\~139-141) also sets the custom extension in its builder. The Scala suite covers adjacent-filter folding only.
- **Event key:** `sha2(concat_ws("|", user_id, event_type, product_id, event_time_iso), 256)`, `event_time_iso = date_format(to_utc_timestamp(event_time,"UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'")`, excluding `user_session` (ADR-003).
- **DAG:** daily, 10 tasks `ingest_raw → bronze_transform → bronze_quality_gate → silver_transform → silver_quality_gate → dbt_staging → dbt_intermediate → dbt_core → dbt_marts → dbt_test_gate`, `max_active_runs=1`, per-task `execution_timeout`, retries 3 / 2 min uniform. No contract, replay, reconciliation or metrics task. DAG paths use `/workspace/...`.
- **Interview-relevant:** the project is batch, not streaming.

## 8. Decisions already made by the owner

- No Docker, no local Spark runs; verification of tests happens on **GitHub Actions** after the owner pushes.
- Branch `audit-remediation`; **the agent never pushes**; no history rewrite without explicit approval naming the commits.
- Keep the Silver fix if the S0 review passes (it did); tests are written but not run by the agent.
- Doc-first for anything the audit marks "code is truth"; code changes only in steps marked CODE.
- The owner rotated the Kaggle key.

## 9. Pending owner decisions (ask, don't assume)

1. Is the GitHub repo public or private? (Affects the committed `Project_summary.md`, which has local paths and old tool dumps: push private, rebase it out, or let Step 9 regenerate it.)
2. Was the uncommitted `Dockerfile.airflow` change intentional? (Step 5.)
3. Quarantine location policy (Step 6) and whether to do real failure-injection tests (Step 8).
4. How to run runtime checks (Step 10): GitHub runner, Codespaces, or leave "not verified".

## 10. Mistakes the previous agent made (avoid repeating)

- Staged an untracked file with a broad add (`Project_summary.md` landed in Batch 1). **Stage named paths only.**
- Committed Batch 1 on `polish-project` before creating the new branch.
- After context compaction its summary contradicted itself ("S0 review incomplete" after reporting PASS). **Re-verify state from git, not from memory.**
- Tried shell commands that don't exist in PowerShell (`uuidgen`, `head`) and ran `python -c "pass"` outside the allowed list. **Use PowerShell-native commands; run only the allowed ones.**
- A UTF-8 BOM crept into a file on Windows once. Check every new or edited file for a BOM and CRLF line endings before committing.

## 11. Your first actions (before touching anything)

1. Print `git branch --show-current`, `git log --oneline pre-audit-fix..HEAD`, `git status --short`, `git stash list`, `git tag -l pre-audit-fix`.
2. Compare with section 3. List every mismatch. Specifically report: whether `8e4ee61` exists; whether Batch S is committed; whether `tests/` and `.github/workflows/silver-tests.yml` exist and are complete or partial.
3. Confirm the patch file exists at the path in section 3 (don't open `.env`).
4. Deliver a **State Verification Report**, then wait for "APPROVED: STEP 1". Don't skip ahead.
5. Then follow `Remediation_Runbook.md` one step at a time, with its Global Rules.