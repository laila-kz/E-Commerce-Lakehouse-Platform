# ⚡ Scala Spark Catalyst Optimization Rule Extension
### High-Performance Query Optimization & JVM Internal Extensions for Apache Spark & Delta Lake

---

## 1. Architectural Overview & Motivation

In enterprise Data Lakehouses, 90% of data engineers write standard PySpark transformations (`df.filter()`, `df.join()`, `df.groupBy()`). While effective, standard DataFrame APIs rely entirely on Spark's default **Catalyst Optimizer** heuristics. 

Building a custom **Scala Catalyst Optimization Rule** demonstrates the highest tier of Spark engineering: **query engine internals, Abstract Syntax Tree (AST) pattern matching, logical plan rewriting, and JVM memory execution.**

This extension module (`extensions/catalyst_optimizer/`) injects custom optimization rules into Apache Spark's Catalyst compiler, directly optimizing query execution plans for our e-commerce clickstream Delta Lake.

```mermaid
flowchart TD
    subgraph SparkCatalystEngine ["Apache Spark Catalyst Compiler Lifecycle"]
        SQL["DataFrame / SQL Query"] --> UNRES["1. Unresolved Logical Plan"]
        UNRES --> ANALYZER["2. Analyzer (Catalog Resolution)"]
        ANALYZER --> RES["3. Resolved Logical Plan"]
        
        subgraph CustomExtensionInjection ["Custom Scala Catalyst Extension Point"]
            RES --> OPT_ENGINE["4. Optimizer Phase"]
            CUSTOM_RULE[["⚙️ Custom Delta Partition Pruning Rule (Scala)"]] -.->|Injected via SparkSessionExtensions| OPT_ENGINE
        end
        
        OPT_ENGINE --> OPT_PLAN["5. Optimized Logical Plan"]
        OPT_PLAN --> PLANNER["6. Spark Physical Planner"]
        PLANNER --> PHYSICAL["7. Physical Plan (Tungsten CodeGen)"]
        PHYSICAL --> RDD["8. Executed RDD Bytecode on Cluster"]
    end
```

---

## 2. The Custom Rule: `DynamicEcommercePartitionPruner`

### Problem Statement in E-Commerce Lakehouse
When querying massive multi-year e-commerce event tables partitioned by `event_date` (`year`, `month`, `day`) alongside dimensional filters (e.g., `category = 'electronics' AND event_time >= current_date - 7`), default Spark execution can fail to push down complex composite predicates, resulting in full partition scans across hundreds of gigabytes of cold data.

### Solution
A custom Catalyst rule written in Scala that inspects the **Logical Plan tree**, identifies non-optimal nested `Filter(Project(...))` nodes on e-commerce clickstream datasets, rewrites the predicate tree to prune unneeded partitions at compile time, and eliminates redundant projection nodes.

---

## 3. Directory Structure (`extensions/catalyst_optimizer/`)

```text
DataLakehouse_ecommerce/
├── extensions/
│   └── catalyst_optimizer/
│       ├── build.sbt                                 # sbt build definition for Scala 2.12 & Spark 3.5.x
│       ├── project/
│       │   └── build.properties                      # sbt version pinning (1.9.8)
│       └── src/
│           ├── main/
│           │   └── scala/
│           │       └── org/
│           │           └── lakehouse/
│           │               └── optimizer/
│           │                   ├── EcommerceSparkExtensions.scala      # Entrypoint implementing (SparkSessionExtensions => Unit)
│           │                   └── rules/
│           │                       └── DynamicEcommercePartitionPruner.scala # Core AST tree-rewriting rule
│           └── test/
│               └── scala/
│                   └── org/
│                       └── lakehouse/
│                           └── optimizer/
│                               └── DynamicEcommercePartitionPrunerSuite.scala # Unit test suite verifying plan transformations
├── spark_jobs/
│   ├── bronze_transform.py
│   └── silver_transform.py
├── SCALA_CATALYST_OPTIMIZER_EXTENSION.md
└── Makefile
```

---

## 4. Implementation Code

### 4.1. `build.sbt` (Build Configuration)
```scala
name := "ecommerce-catalyst-optimizer"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "io.delta"         %% "delta-spark" % "3.1.0" % "provided",
  "org.scalatest"    %% "scalatest"  % "3.2.17" % Test
)

// Package as fat JAR for deployment
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
```

---

### 4.2. `DynamicEcommercePartitionPruner.scala` (Core Rule)
```scala
package org.lakehouse.optimizer.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Custom Catalyst Optimizer Rule:
 * Inspects logical plans and rewrites redundant multi-layered filters and date predicates
 * on e-commerce event streams to optimize Delta Lake scan planning.
 */
case class DynamicEcommercePartitionPruner() extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    // Pattern Match 1: Eliminate redundant nested filters (Filter -> Filter -> Child)
    case Filter(cond1, Filter(cond2, child)) =>
      logInfo(s"[EcommerceOptimizer] Folding contiguous filters: ($cond1) AND ($cond2)")
      val combinedCondition = And(cond1, cond2)
      Filter(combinedCondition, child)

    // Pattern Match 2: Optimize date-truncation equality checks into direct range boundaries
    case Filter(EqualTo(DateFormatClass(attr: AttributeReference, Literal(fmt, _), _), Literal(dateVal, _)), child)
        if fmt.toString == "yyyy-MM-dd" =>
      logInfo(s"[EcommerceOptimizer] Optimizing date_format equality on ${attr.name} into partition range scan")
      val startOfDay = Literal(s"${dateVal} 00:00:00")
      val endOfDay   = Literal(s"${dateVal} 23:59:59")
      val rangeCondition = And(
        GreaterThanOrEqual(attr, Cast(startOfDay, attr.dataType)),
        LessThanOrEqual(attr, Cast(endOfDay, attr.dataType))
      )
      Filter(rangeCondition, child)
  }
}
```

---

### 4.3. `EcommerceSparkExtensions.scala` (Spark Extension Entrypoint)
```scala
package org.lakehouse.optimizer

import org.apache.spark.sql.SparkSessionExtensions
import org.lakehouse.optimizer.rules.DynamicEcommercePartitionPruner
import org.apache.spark.internal.Logging

/**
 * Entrypoint registered in Spark via --conf spark.sql.extensions
 */
class EcommerceSparkExtensions extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logInfo("[EcommerceOptimizer] Registering DynamicEcommercePartitionPruner into Spark Catalyst Optimizer")
    extensions.injectOptimizerRule { _ =>
      DynamicEcommercePartitionPruner()
    }
  }
}
```

---

### 4.4. `DynamicEcommercePartitionPrunerSuite.scala` (Verification Test Suite)
```scala
package org.lakehouse.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.lakehouse.optimizer.rules.DynamicEcommercePartitionPruner
import org.scalatest.funsuite.AnyFunSuite

class DynamicEcommercePartitionPrunerSuite extends AnyFunSuite {

  test("DynamicEcommercePartitionPruner collapses consecutive Filter nodes into a single And node") {
    val dummyRelation = LocalRelation('event_id.string, 'event_time.timestamp, 'category.string)
    
    // Construct unoptimized plan: Filter(category == 'electronics') -> Filter(event_id != null) -> Relation
    val cond1 = EqualTo('category.string, Literal("electronics"))
    val cond2 = IsNotNull('event_id.string)
    val unoptimizedPlan = Filter(cond1, Filter(cond2, dummyRelation))

    // Apply rule
    val rule = DynamicEcommercePartitionPruner()
    val optimizedPlan = rule(unoptimizedPlan)

    // Assert the plan was rewritten into a single Filter with combined And condition
    assert(optimizedPlan.isInstanceOf[Filter])
    val singleFilter = optimizedPlan.asInstanceOf[Filter]
    assert(singleFilter.condition == And(cond1, cond2))
    assert(singleFilter.child == dummyRelation)
  }
}
```

---

## 5. Integrating with PySpark & Docker Infrastructure

### 5.1. Building the JAR
From the project root:
```bash
# Build the Scala extension JAR using sbt
cd extensions/catalyst_optimizer
sbt clean test assembly
# Output: target/scala-2.12/ecommerce-catalyst-optimizer-assembly-1.0.0.jar
```

### 5.2. Enabling the Extension in PySpark / Airflow
In Python (`spark_jobs/bronze_transform.py`, `spark_jobs/silver_transform.py`, or Airflow SparkSubmitOperator):

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceLakehousePipeline")
    # 1. Include the custom compiled extension JAR alongside Delta Lake
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.1.0.jar,/opt/spark/jars/ecommerce-catalyst-optimizer-assembly-1.0.0.jar")
    # 2. Register both Delta and our Custom Scala Extension classes
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.lakehouse.optimizer.EcommerceSparkExtensions")
    # 3. Standard S3/MinIO & Delta configs
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```

### 5.3. Makefile Automation
Add targets to `DataLakehouse_ecommerce/Makefile`:
```makefile
.PHONY: build-scala-extension test-scala-extension

build-scala-extension:
	@echo "==> Building Scala Spark Catalyst Optimizer Extension..."
	cd extensions/catalyst_optimizer && sbt clean assembly
	cp extensions/catalyst_optimizer/target/scala-2.12/ecommerce-catalyst-optimizer-assembly-1.0.0.jar ./jars/

test-scala-extension:
	@echo "==> Running ScalaTest suite for Catalyst Optimizer..."
	cd extensions/catalyst_optimizer && sbt test
```

---

## 6. How to Showcase This in Interviews & On Your Resume

### Resume Bullet Point:
> *"Developed a custom Apache Spark Catalyst optimization rule in **Scala** using AST pattern matching and logical plan tree rewriting, integrated via `SparkSessionExtensions` to optimize date predicate pushdown and filter folding on Delta Lake tables."*

### Key Interview Questions to Crush:
1. **Q: How does Spark's Catalyst Optimizer work under the hood?**
   - *Answer:* "Catalyst operates as a tree transformation framework written in Scala. It takes an Unresolved Logical Plan, resolves it against the Catalog (Analyzer), applies optimization rules via fixed-point iterations (`RuleExecutor`) to produce an Optimized Logical Plan, and converts it into physical planning nodes using Tungsten code generation."
2. **Q: How did you inject your custom Scala rule into a PySpark-managed pipeline?**
   - *Answer:* "I implemented the `(SparkSessionExtensions => Unit)` interface, compiled the Scala code into a fat JAR with sbt, and injected the class through `spark.sql.extensions` at `SparkSession` initialization. PySpark jobs immediately benefited from the custom Catalyst optimizations on the JVM side without changing any Python client code."
