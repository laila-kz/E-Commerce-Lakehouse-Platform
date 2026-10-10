# Version Matrix

Single source of truth for component versions across this repository (finding F-018).
One consistent set everywhere: **Apache Spark 3.5.0, Delta Lake 3.1.0, Scala 2.12.18**.

## Core runtime matrix

| Component | Version | Pinned in |
|---|---|---|
| Apache Spark | 3.5.0 | `Dockerfile.spark` (base image), `extensions/catalyst_optimizer/build.sbt`, `.github/workflows/silver-tests.yml` (pyspark) |
| PySpark | 3.5.0 | `requirements.txt` |
| Delta Lake | 3.1.0 | `Dockerfile.spark` (`delta-spark_2.12-3.1.0.jar`, `delta-storage-3.1.0.jar`), `extensions/catalyst_optimizer/build.sbt`, `requirements.txt` (delta-spark), `scripts/replay_events.py`, `soda/`, `tests/` |
| Scala | 2.12.18 | `extensions/catalyst_optimizer/build.sbt`, `Makefile` (sbt docker image tag) |
| sbt | 1.9.8 | `extensions/catalyst_optimizer/project/build.properties` |
| Airflow | 2.8.1-python3.10 | `Dockerfile.airflow` |
| dbt-core | 1.8.8 | `Dockerfile.airflow`, `requirements.txt` |
| dbt-duckdb | 1.8.4 | `Dockerfile.airflow`, `requirements.txt` |
| dbt-adapters | 1.11.0 | `Dockerfile.airflow`, `requirements.txt` |
| DuckDB | 1.1.3 | `requirements.txt` |
| Soda Core (SQL/DuckDB) | 3.0.45 | `requirements.txt` |
| openlineage-python | 1.21.1 | `requirements.txt` |
| hadoop-aws | 3.3.4 | `Dockerfile.spark` |
| aws-java-sdk-bundle | 1.12.262 | `Dockerfile.spark` |
| pytest / boto3 / minio / python-dotenv / pyyaml / tenacity / requests / tabulate | pinned in file | `requirements.txt` |

## Unpinned / floating

| Component | Pin | Where |
|---|---|---|
| MinIO | `latest` | `docker-compose.yml` |
| Marquez (+ Marquez web) | `latest` | `docker-compose.yml` |
| Metabase | `latest` | `docker-compose.yml` |
| PostgreSQL | 13 (airflow-db), 14 (marquez-db) | `docker-compose.yml` |
| dbt-duckdb (CI) | unpinned install | `.github/workflows/ci.yml` |

## Notes

- The Spark container is built from `Dockerfile.spark` (`FROM apache/spark:3.5.0`) and ships `delta-spark_2.12-3.1.0.jar` + `delta-storage-3.1.0.jar`; the `Makefile` sbt docker image tag was aligned to Scala 2.12.18 to match `build.sbt`.
- `soda/`, `tests/`, `scripts/replay_events.py`, and the Spark jobs reference the Delta 3.1.0 JAR and are consistent with this matrix.
- `Project_summary.md` (gitignored, not committed) still claims PySpark 3.5.0 / Delta 3.0.0; it is out of scope for version pinning.
- Builds were not re-run locally (Docker is offline). The `sbtscala/scala-sbt` tag `eclipse-temurin-17.0.4_1.7.2_2.12.18` should be verified at the next rebuild and bumped if the tag does not exist in the registry.