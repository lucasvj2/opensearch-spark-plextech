/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.security.MessageDigest

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.util.QueryExecutionListener

case class QueryPlanNode(
  queryId: String,
  parsedPlanJson: String,
  analyzedPlanJson: String,
  optimizedPlanJson: String,
  physicalPlanJson: String,
  nodeSignatures: Map[String, Map[String, (String, String)]], // For each plan, map node to signatures
  metadata: Map[String, String]
)

object PlanSignature {

  private def hashString(s: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  def computeStrictSignature(plan: LogicalPlan): String = {
    val nodeDetails = plan.nodeName + plan.expressions.map(_.toString).mkString
    hashString(nodeDetails)
  }

  def computeRecurringSignature(plan: LogicalPlan): String = {
    val nodeDetails = plan.nodeName + plan.expressions.map(removeLiterals).mkString
    hashString(nodeDetails)
  }

  private def removeLiterals(expr: Expression): String = {
    expr.transform { case lit: Literal => Literal(null) }.toString
  }

  def computeStrictSignature(plan: SparkPlan): String = {
    val nodeDetails = plan.nodeName + plan.expressions.map(_.toString).mkString
    hashString(nodeDetails)
  }

  def computeRecurringSignature(plan: SparkPlan): String = {
    val nodeDetails = plan.nodeName + plan.expressions.map(removeLiterals).mkString
    hashString(nodeDetails)
  }
}

class SqlExecutionListener(spark: SparkSession) extends QueryExecutionListener with Logging {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val durationMs: Long = durationNs / 1000000
    logInfo(s"Query succeeded in $durationMs ms")

    // Gather all plans as JSON
    val parsedPlanJson = qe.logical.prettyJson
    val analyzedPlanJson = qe.analyzed.prettyJson
    val optimizedPlanJson = qe.optimizedPlan.prettyJson
    val physicalPlanJson = qe.executedPlan.prettyJson

    // Compute signatures for each plan
    val parsedSignatures = computePlanSignatures(qe.logical)
    val analyzedSignatures = computePlanSignatures(qe.analyzed)
    val optimizedSignatures = computePlanSignatures(qe.optimizedPlan)
    val physicalSignatures = computePlanSignatures(qe.executedPlan)

    // Create a single map of all node signatures
    log.Error("SIGNATURE:" + optimizedSignatures)
    log.Error("PLAN JSON" + optimizedPlanJson)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logError(s"Query failed with error: ${exception.getMessage}")
  }

  // Helper function to compute the signatures for all nodes in a plan
  private def computePlanSignatures(plan: LogicalPlan): Map[String, (String, String)] = {
    plan.collect {
      case node =>
        val strictSignature = PlanSignature.computeStrictSignature(node)
        val recurringSignature = PlanSignature.computeRecurringSignature(node)
        node.nodeName -> (strictSignature, recurringSignature)
    }.toMap
  }

  private def computePlanSignatures(plan: SparkPlan): Map[String, (String, String)] = {
    plan.collect {
      case node =>
        logError("NODE: " + node)
        val strictSignature = PlanSignature.computeStrictSignature(node)
        val recurringSignature = PlanSignature.computeRecurringSignature(node)
        node.nodeName -> (strictSignature, recurringSignature)
    }.toMap
  }
}