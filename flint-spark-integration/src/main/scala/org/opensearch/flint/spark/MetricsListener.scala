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
 * Imputes missing cardinality and execution time metrics in Spark query plans using post-order
 * traversal.
 */
object CardinalityAndTimeImputer extends Logging {
  // Thread-safe maps to store computed/imputed metrics
  private val cardinalityMetrics = scala.collection.mutable.Map[Long, Long]()
  private val executionTimeMetrics = scala.collection.mutable.Map[Long, Long]()

  /**
   * Main method to impute cardinalities and execution times for a given plan
   * @param plan
   *   The SparkPlan to process
   * @return
   *   Tuple of maps for cardinality and execution time metrics
   */
  def imputeMetrics(plan: SparkPlan): (Map[Long, Long], Map[Long, Long]) = {
    // Clear previous metrics before starting new imputation
    cardinalityMetrics.clear()
    executionTimeMetrics.clear()

    // Perform post-order traversal and return results
    traverseAndImpute(plan)
    (cardinalityMetrics.toMap, executionTimeMetrics.toMap)
  }

  /**
   * Recursive helper method to perform post-order traversal and imputation
   * @param plan
   *   Current SparkPlan node
   */
  private def traverseAndImpute(plan: SparkPlan): Unit = {
    // Process children first (post-order traversal)
    plan.children.foreach(traverseAndImpute)

    // Get actual cardinality metric
    val actualCardinality = plan.metrics
      .get("number of output rows")
      .orElse(plan.metrics.get("numOutputRows"))
      .map(_.value)
      .getOrElse(0L)

    // Get actual execution time metric
    val actualExecutionTime = plan.metrics
      .get("time taken")
      .orElse(plan.metrics.get("executionTime"))
      .map(_.value)
      .getOrElse(0L)

    // Impute cardinality
    val imputedCardinality = if (actualCardinality > 0L) {
      actualCardinality
    } else {
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

    // Impute execution time
    val imputedExecutionTime = if (actualExecutionTime > 0L) {
      actualExecutionTime
    } else {
      // Distribute time based on cardinality proportion
      val childTimes = plan.children.flatMap(child => executionTimeMetrics.get(child.id))
      val childCardinalities = plan.children.flatMap(child => cardinalityMetrics.get(child.id))

      if (childTimes.isEmpty) 0L
      else if (childCardinalities.isEmpty) childTimes.sum
      else {
        // Proportionally distribute total child time based on child cardinalities
        val totalChildCardinality = childCardinalities.sum
        childTimes
          .zip(childCardinalities)
          .map { case (time, card) => (time * card / totalChildCardinality) }
          .sum
      }
    }

    // Store the computed metrics
    cardinalityMetrics.put(plan.id, imputedCardinality)
    executionTimeMetrics.put(plan.id, imputedExecutionTime)

    logInfo(s"Imputed Metrics for ${plan.getClass.getSimpleName}:")
    logInfo(s"  Operator ID: ${plan.id}")
    logInfo(s"  Cardinality: $imputedCardinality rows")
    logInfo(s"  Execution Time: $imputedExecutionTime ms")
  }
}

/**
 * Spark listener that captures query executions and performs metrics imputation
 * @param spark
 *   Active SparkSession
 */
class MetricsListener(spark: SparkSession) extends SparkListener with Logging {
  private val queryExecutions = new java.util.concurrent.ConcurrentHashMap[Long, QueryExecution]()

  // Register query execution listener
  spark.listenerManager.register(new QueryExecutionListener {
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      queryExecutions.put(qe.id, qe)
      val (imputedCardinalities, imputedTimes) =
        CardinalityAndTimeImputer.imputeMetrics(qe.executedPlan)

      // Log the results
      logInfo(s"""
        |Query ${qe.id} Imputed Metrics:
        |Cardinalities:
        |${imputedCardinalities
                  .map { case (id, card) => s"  Operator $id: $card rows" }
                  .mkString("\n")}
        |Execution Times:
        |${imputedTimes
                  .map { case (id, time) => s"  Operator $id: $time ms" }
                  .mkString("\n")}
        |""".stripMargin)
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      queryExecutions.put(qe.id, qe)
      logError(s"Query execution failed: ${exception.getMessage}")
    }
  })

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case executionEnd: SparkListenerSQLExecutionEnd =>
        Option(queryExecutions.get(executionEnd.executionId)).foreach { queryExecution =>
          try {
            val plan = queryExecution.executedPlan
            val (imputedCardinalities, imputedTimes) =
              CardinalityAndTimeImputer.imputeMetrics(plan)

            // Log the results
            logInfo(s"""
              |Query ${executionEnd.executionId} Imputed Metrics:
              |Cardinalities:
              |${imputedCardinalities
                        .map { case (id, card) => s"  Operator $id: $card rows" }
                        .mkString("\n")}
              |Execution Times:
              |${imputedTimes
                        .map { case (id, time) => s"  Operator $id: $time ms" }
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
