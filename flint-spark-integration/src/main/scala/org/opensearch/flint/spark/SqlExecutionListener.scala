/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class SqlExecutionListener extends QueryExecutionListener with Logging {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val durationMs: Long = durationNs / 1000000
    logError(s"FlintSparkOptimizer LISTENER: Query succeeded in $durationMs ms")

    val parsedPlanJson = qe.logical.prettyJson
    logError(s"Parsed Logical Plan in JSON: $parsedPlanJson")

    val analyzedPlanJson = qe.analyzed.prettyJson
    logError(s"Analyzed Logical Plan in JSON: $analyzedPlanJson")

    val optimizedPlanJson = qe.optimizedPlan.prettyJson
    logError(s"Optimized Logical Plan in JSON: $optimizedPlanJson")

    val physicalPlanJson = qe.sparkPlan.prettyJson
    logError(s"Physical/Execution Plan in JSON: $physicalPlanJson")
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logError("FlintSparkOptimizer LISTENER error")
  }
}
