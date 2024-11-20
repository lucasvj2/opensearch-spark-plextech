/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.security.MessageDigest

import play.api.libs.json._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener

object JsonUtils {
  // Custom Writes for Any
  implicit val anyWrites: Writes[Any] = new Writes[Any] {
    def writes(value: Any): JsValue = value match {
      case s: String => JsString(s)
      case n: Int => JsNumber(n)
      case n: Long => JsNumber(n)
      case n: Double => JsNumber(n)
      case b: Boolean => JsBoolean(b)
      case m: Map[_, _] =>
        Json.toJson(m.asInstanceOf[Map[String, Any]]) // Recursively handle nested Maps
      case l: Seq[_] =>
        JsArray(l.map(serializeAny)) // Serialize sequences using helper method
      case None => JsNull
      case null => JsNull
      case other => JsString(other.toString) // Fallback for unsupported types
    }
  }

  // Helper method for serializing elements
  private def serializeAny(element: Any): JsValue = {
    Json.toJson(element)(anyWrites)
  }

  // Custom Writes for Map[String, Any]
  implicit val mapWrites: Writes[Map[String, Any]] = new Writes[Map[String, Any]] {
    def writes(map: Map[String, Any]): JsValue = Json.obj(map.map { case (key, value) =>
      key -> Json.toJsFieldJsValueWrapper(serializeAny(value))
    }.toSeq: _*)
  }

  // Custom Writes for Map[String, Map[String, Any]]
  implicit val telemetryWrites: Writes[Map[String, Map[String, Any]]] =
    new Writes[Map[String, Map[String, Any]]] {
      def writes(map: Map[String, Map[String, Any]]): JsValue = Json.toJson(map.map {
        case (k, v) => k -> Json.toJson(v)(mapWrites)
      })
    }
}

case class QueryPlanNode(
    queryId: String,
    parsedPlanJson: String,
    analyzedPlanJson: String,
    optimizedPlanJson: String,
    physicalPlanJson: String,
    nodeSignatures: Map[String, Map[String, (String, String)]], // Map node to signatures
    metadata: Map[String, String])

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

  import JsonUtils._ // Import implicit Writes for JSON serialization

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

    val telemetryData = computePlanSignaturesWithMetrics(qe.executedPlan)
    val jsonTelemetry = Json.toJson(telemetryData).toString()

    // Create a single map of all node signatures
    logError("SIGNATURE: " + telemetryData)
    logError("PLAN JSON: " + physicalPlanJson)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logError(s"Query failed with error: ${exception.getMessage}")
  }

  // Helper function to compute the signatures for all nodes in a plan
  private def computePlanSignatures(plan: LogicalPlan): Map[String, (String, String)] = {
    plan.collect { case node =>
      val strictSignature = PlanSignature.computeStrictSignature(node)
      val recurringSignature = PlanSignature.computeRecurringSignature(node)
      node.nodeName -> (strictSignature, recurringSignature)
    }.toMap
  }

  private def computePlanSignatures(plan: SparkPlan): Map[String, (String, String)] = {
    plan.collect { case node =>
      logError("NODE: " + node)
      val strictSignature = PlanSignature.computeStrictSignature(node)
      val recurringSignature = PlanSignature.computeRecurringSignature(node)
      node.nodeName -> (strictSignature, recurringSignature)
    }.toMap
  }

  private def computePlanSignaturesWithMetrics(plan: SparkPlan): Map[String, Map[String, Any]] = {
    plan.collect { case node =>
      val metrics = node.metrics.map { case (name, metric) => name -> metric.value }
      val strictSignature = PlanSignature.computeStrictSignature(node)
      val recurringSignature = PlanSignature.computeRecurringSignature(node)

      node.nodeName -> Map(
        "strictSignature" -> strictSignature,
        "recurringSignature" -> recurringSignature,
        "metrics" -> metrics,
        "duration" -> metrics.getOrElse(
          "executionTime",
          0L
        ), // Use getOrElse if executionTime might be absent
        "numRows" -> metrics.getOrElse(
          "numOutputRows",
          0L
        ), // Use getOrElse if numOutputRows might be absent
        "spill" -> metrics.getOrElse(
          "spillSize",
          0L
        ) // Use getOrElse if spillSize might be absent
      )
    }.toMap
  }
}
