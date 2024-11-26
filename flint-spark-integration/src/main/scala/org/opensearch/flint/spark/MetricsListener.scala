/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Imputes missing cardinality metrics in Spark query plans using post-order traversal. For most
 * operators, copies cardinality from child if missing. For joins, uses maximum of child
 * cardinalities if missing.
 */
object CardinalityImputer {
  // Thread-safe map to store computed/imputed cardinalities
  private val cardinalityMetrics = scala.collection.mutable.Map[Long, Long]()

  /**
   * Main method to impute cardinalities for a given plan
   * @param plan
   *   The SparkPlan to process
   * @return
   *   Map of operator IDs to their cardinalities
   */
  def imputeCardinalities(plan: SparkPlan): Map[Long, Long] = {
    // Clear previous metrics before starting new imputation
    cardinalityMetrics.clear()

    // Perform post-order traversal and return results
    traverseAndImpute(plan)
    cardinalityMetrics.toMap
  }

  /**
   * Recursive helper method to perform post-order traversal and imputation
   * @param plan
   *   Current SparkPlan node
   */
  private def traverseAndImpute(plan: SparkPlan): Unit = {
    // Process children first (post-order traversal)
    plan.children.foreach(traverseAndImpute)

    // Get actual metric if available
    val actualCardinality = plan.metrics
      .get("number of output rows")
      .orElse(plan.metrics.get("numOutputRows"))
      .map(_.value)
      .getOrElse(0L)

    val imputedCardinality = if (actualCardinality > 0L) {
      // Use actual metric if available
      actualCardinality
    } else {
      // Impute based on operator type
      plan match {
        case _: org.apache.spark.sql.execution.joins.BaseJoinExec =>
          // For joins, use max of child cardinalities
          val childCardinalities =
            plan.children.flatMap(child => cardinalityMetrics.get(child.id))
          if (childCardinalities.isEmpty) 0L else childCardinalities.max

        case _ =>
          // For other operators, use sum of child cardinalities
          val childCardinalities =
            plan.children.flatMap(child => cardinalityMetrics.get(child.id))
          if (childCardinalities.isEmpty) 0L else childCardinalities.sum
      }
    }

    // Store the computed cardinality
    cardinalityMetrics.put(plan.id, imputedCardinality)
  }
}

/**
 * Spark listener that captures query executions and performs cardinality imputation
 * @param spark
 *   Active SparkSession
 */
class MetricsListener(spark: SparkSession) extends SparkListener with Logging {
  private val queryExecutions = new java.util.concurrent.ConcurrentHashMap[Long, QueryExecution]()

  // Register query execution listener
  spark.listenerManager.register(new QueryExecutionListener {
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      logError("I AM WORKING")
      queryExecutions.put(qe.id, qe)
      val imputedMetrics = CardinalityImputer.imputeCardinalities(qe.executedPlan)

            // Log the results
            logError(s"""
              |Query ${qe.id} Cardinality Metrics:
              |${imputedMetrics
                         .map { case (id, card) =>
                           s"  Operator $id: $card rows"
                         }
                         .mkString("\n")}
              |""".stripMargin)
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      queryExecutions.put(qe.id, qe)
    }
  })

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case executionEnd: SparkListenerSQLExecutionEnd =>
        Option(queryExecutions.get(executionEnd.executionId)).foreach { queryExecution =>
          try {
            val plan = queryExecution.executedPlan
            val imputedMetrics = CardinalityImputer.imputeCardinalities(plan)

            // Log the results
            logError(s"""
              |Query ${executionEnd.executionId} Cardinality Metrics:
              |${imputedMetrics
                         .map { case (id, card) =>
                           s"  Operator $id: $card rows"
                         }
                         .mkString("\n")}
              |""".stripMargin)

          } catch {
            case e: Exception =>
              logError(
                s"Error processing metrics for query ${executionEnd.executionId}: ${e.getMessage}")
          } finally {
            // Cleanup
            queryExecutions.remove(executionEnd.executionId)
          }
        }

      case _ => // Ignore other events
    }
  }
}
