// /*
//  * Copyright OpenSearch Contributors
//  * SPDX-License-Identifier: Apache-2.0
//  */

// package org.opensearch.flint.spark

// import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
// import org.apache.spark.sql.execution.SparkPlan
// import 
// import

// case class Node

// object Linker {
//     /*
//     {
//         join: [filter, sort, merge],
//         filter: filterExec
        
//     }
//     */

//     def link(planA: LogicalPlan, planB: SparkPlan): Unit {
//         // link the plan
//         /*
//         foreach (node) in planA and planB:
//             if nodeA is similar to nodeB
//                 edge(nodeA, nodeB)
//         put it in a list
//         */
//         val nodeA = planA.
//         val new_link = edge(nodeA, nodeB)
//     }
//     case class edge(from: LogicalPlan, to: SparkPlan) //need the nodes

//     private def areNodesSimilar(logicalNode: Node, physicalNode: Node): Boolean {
//         val logicalNodeName = logicalNode
//     }

// }