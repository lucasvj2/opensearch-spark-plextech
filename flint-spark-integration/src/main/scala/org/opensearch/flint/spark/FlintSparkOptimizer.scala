/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

// import org.opensearch.flint.spark.SqlExecutionListener
import org.opensearch.flint.spark.MetricsListener
import org.opensearch.flint.spark.covering.ApplyFlintSparkCoveringIndex
import org.opensearch.flint.spark.skipping.ApplyFlintSparkSkippingIndex

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Flint Spark optimizer that manages all Flint related optimizer rule.
 * @param spark
 *   Spark session
 */
class FlintSparkOptimizer(spark: SparkSession) extends Rule[LogicalPlan] {

  /** Flint Spark API */
  private val flint: FlintSpark = new FlintSpark(spark)
  // private val li = spark.listenerManager.register(new SqlExecutionListener(spark))
  spark.sparkContext.addSparkListener(new MetricsListener(spark))
  logError("LOGGING WORKING CORRECTLY???")

  /** Skipping index rewrite rule */
  private val skippingIndexRule = new ApplyFlintSparkSkippingIndex(flint)

  /** Covering index rewrite rule */
  private val coveringIndexRule = new ApplyFlintSparkCoveringIndex(flint)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (isFlintOptimizerEnabled) {
      if (isCoveringIndexOptimizerEnabled) {
        // Apply covering index rule first
        skippingIndexRule.apply(coveringIndexRule.apply(plan))
      } else {
        skippingIndexRule.apply(plan)
      }
    } else {
      plan
    }
  }

  private def isFlintOptimizerEnabled: Boolean = {
    FlintSparkConf().isOptimizerEnabled
  }

  private def isCoveringIndexOptimizerEnabled: Boolean = {
    FlintSparkConf().isCoveringIndexOptimizerEnabled
  }
}
