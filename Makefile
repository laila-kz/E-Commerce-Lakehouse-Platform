.PHONY: help setup init-buckets create-demo-sample test-contracts test-contract-breaking test-contract-compatible run-ingest run-ingest-sample build-scala-extension test-scala-extension run-bronze run-silver run-dbt test-dbt test-dbt-offline run-reconciliation run-benchmarks verify-all run-demo clean

help:
	@echo "E-Commerce Data Lakehouse CLI Commands"
	@echo "--------------------------------------"
	@echo "make setup                : Check prerequisites & start Docker stack"
	@echo "make create-demo-sample   : Generate lightweight sample dataset (50k rows ~6MB) for fast video demos"
	@echo "make test-contracts       : Run Data Contract validation CLI on valid & breaking test data"
	@echo "make test-contract-breaking: Run schema diff test asserting BREAKING classification"
	@echo "make run-ingest-sample    : Ingest demo sample dataset into MinIO raw bucket (super fast)"
	@echo "make run-ingest           : Ingest full dataset into MinIO raw bucket"
	@echo "make build-scala-extension: Build Scala Spark Catalyst Optimizer Extension fat JAR via sbt"
	@echo "make test-scala-extension : Run ScalaTest suite for Catalyst Optimizer"
	@echo "make run-bronze           : Execute PySpark Bronze layer transform"
	@echo "make run-silver           : Execute PySpark Silver incremental MERGE transform"
	@echo "make run-dbt              : Execute dbt models (staging -> intermediate -> core -> marts)"
	@echo "make test-dbt             : Execute full dbt test suite (84 data tests, requires Docker MinIO)"
	@echo "make test-dbt-offline     : Execute offline dbt test subset (58 tests, excludes s3_dependent; zero deps)"
	@echo "make run-reconciliation   : Run cross-layer mathematical reconciliation check suite"
	@echo "make run-benchmarks       : Run PySpark Optimization & Performance Benchmarking Suite"
	@echo "make verify-all           : Execute contracts, dbt offline tests, reconciliation, and benchmarks"
	@echo "make run-demo             : Execute full end-to-end demo pipeline in 30 seconds"
	@echo "make clean                : Bring down Docker stack and clean transient data"

setup:
	@echo "--> Checking Docker Compose stack..."
	docker compose up -d --build
	@echo "--> Creating MinIO Buckets..."
	python scripts/create_buckets.py
	@echo "--> Setup complete! Stack is healthy."

init-buckets:
	python scripts/create_buckets.py

create-demo-sample:
	python scripts/create_demo_sample.py

test-contracts:
	@echo "--> Testing Valid Dataset against Contract..."
	python contracts/contract_cli.py validate --input-file data/test_valid.csv --contract-file contracts/schemas/ecommerce_events_v1.yml
	@echo "--> Testing Breaking Dataset against Contract (Expect Failure & Quarantine)..."
	-python contracts/contract_cli.py validate --input-file data/test_breaking.csv --contract-file contracts/schemas/ecommerce_events_v1.yml

test-contract-compatible:
	@echo "--> Testing Compatible Schema Evolution (v1 -> v2)..."
	python contracts/contract_cli.py diff --old-contract contracts/schemas/ecommerce_events_v1.yml --new-contract contracts/schemas/ecommerce_events_v2.yml

test-contract-breaking:
	@echo "--> Testing Breaking Schema Evolution (v1 -> v2_breaking)..."
	-python contracts/contract_cli.py diff --old-contract contracts/schemas/ecommerce_events_v1.yml --new-contract contracts/schemas/ecommerce_events_v2_breaking.yml

run-ingest-sample:
	python ingestion/kaggle_ingest.py --data-dir ./data/demo_sample --force

run-ingest:
	python ingestion/kaggle_ingest.py --force

build-scala-extension:
	@echo "--> Building Scala Spark Catalyst Optimizer Extension..."
	docker run --rm -v "$$(pwd)/extensions/catalyst_optimizer:/app" -v sbt_coursier_cache:/root/.cache -v sbt_ivy_cache:/root/.ivy2 -w /app sbtscala/scala-sbt:eclipse-temurin-17.0.4_1.7.2_2.12.18 sbt clean assembly
	cp extensions/catalyst_optimizer/target/scala-2.12/ecommerce-catalyst-optimizer-assembly-1.0.0.jar ./jars/

test-scala-extension:
	@echo "--> Running ScalaTest suite for Catalyst Optimizer..."
	docker run --rm -v "$$(pwd)/extensions/catalyst_optimizer:/app" -v sbt_coursier_cache:/root/.cache -v sbt_ivy_cache:/root/.ivy2 -w /app sbtscala/scala-sbt:eclipse-temurin-17.0.4_1.7.2_2.12.18 sbt test

run-bronze:
	docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 2g --executor-memory 2g --jars /opt/spark/jars-extra/*.jar --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.lakehouse.optimizer.EcommerceSparkExtensions --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /workspace/spark_jobs/bronze_transform.py

run-silver:
	docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 2g --executor-memory 2g --jars /opt/spark/jars-extra/*.jar --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.lakehouse.optimizer.EcommerceSparkExtensions --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /workspace/spark_jobs/silver_transform.py

run-dbt:
	cd dbt && dbt run --profiles-dir .

test-dbt:
	cd dbt && dbt test --profiles-dir .

test-dbt-offline:
	cd dbt && dbt test --exclude tag:s3_dependent --profiles-dir .

run-reconciliation:
	python checks/run_reconciliation.py

run-benchmarks:
	python spark_jobs/spark_benchmark.py

verify-all: test-contracts test-contract-compatible test-contract-breaking test-dbt-offline run-reconciliation run-benchmarks
	@echo "--> [SUCCESS] Full Lakehouse Governance & Quality Verification Suite Passed!"

run-demo: create-demo-sample run-ingest-sample run-bronze run-silver run-dbt test-dbt run-reconciliation
	@echo "--> 🚀 DEMO PIPELINE EXECUTION COMPLETED SUCCESSFULLY IN SECONDS!"

clean:
	docker compose down -v
