package org.lakehouse.optimizer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.lakehouse.optimizer.rules.DynamicEcommercePartitionPruner

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
