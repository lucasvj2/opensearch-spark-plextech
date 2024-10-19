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
    expr.transform { case lit: Literal =>
      Literal(null) // Replace literals with a null literal
    }.toString
  }

  // For SparkPlan (Physical Plan)
  def computeStrictSignature(plan: SparkPlan): String = {
    val nodeDetails = plan.nodeName + plan.expressions.map(_.toString).mkString
    hashString(nodeDetails)
  }

  def computeRecurringSignature(plan: SparkPlan): String = {
    val nodeDetails = plan.nodeName + plan.expressions.map(removeLiterals).mkString
    hashString(nodeDetails)
  }
}

class SqlExecutionListener extends QueryExecutionListener with Logging {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val durationMs: Long = durationNs / 1000000
    logError(s"Query succeeded in $durationMs ms")

    // Log the parsed, analyzed, optimized plans (Logical Plans)
    logPlanWithSignatures(qe.logical, "Parsed Logical Plan")
    logPlanWithSignatures(qe.analyzed, "Analyzed Logical Plan")
    logPlanWithSignatures(qe.optimizedPlan, "Optimized Logical Plan")
    logPhysicalPlanWithSignatures(qe.executedPlan, "Physical Execution Plan")
  }

  // Method for LogicalPlan
  def logPlanWithSignatures(plan: LogicalPlan, planType: String): Unit = {
    logError(s"$planType in JSON: ${plan.prettyJson}")

    plan.foreach { node =>
      val strictSignature = PlanSignature.computeStrictSignature(node)
      val recurringSignature = PlanSignature.computeRecurringSignature(node)
      logError(
        s"Node: ${node.nodeName}, Strict Signature: $strictSignature, Recurring Signature: $recurringSignature")
    }
  }

  // Method for SparkPlan (Physical Plan)
  def logPhysicalPlanWithSignatures(plan: SparkPlan, planType: String): Unit = {
    logError(s"$planType in JSON: ${plan.prettyJson}")

    plan.foreach { node =>
      val strictSignature = PlanSignature.computeStrictSignature(node)
      val recurringSignature = PlanSignature.computeRecurringSignature(node)
      logError(
        s"Node: ${node.nodeName}, Strict Signature: $strictSignature, Recurring Signature: $recurringSignature")
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logError(s"Query failed with error: ${exception.getMessage}")
  }
}

