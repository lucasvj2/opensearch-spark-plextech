package org.opensearch.flint.spark

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class SqlExecutionListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logError("FlintSparkOptimizer LISTENER logged CORRECTLY")
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logError("FlintSparkOptimizer LISTENER error")
  }
}
