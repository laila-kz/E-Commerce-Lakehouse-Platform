package org.lakehouse.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.lakehouse.optimizer.rules.DynamicEcommercePartitionPruner
import org.scalatest.funsuite.AnyFunSuite

class DynamicEcommercePartitionPrunerSuite extends AnyFunSuite {

  test("DynamicEcommercePartitionPruner collapses consecutive Filter nodes into a single And node") {
    val dummyRelation = LocalRelation('event_id.string, 'event_time.timestamp, 'category.string)
    
    // Construct unoptimized plan: Filter(category == 'electronics') -> Filter(event_id != null) -> Relation
    val cond1 = EqualTo('category.string, Literal("electronics"))
    val cond2 = IsNotNull('event_id.string)
    val unoptimizedPlan = Filter(cond1, Filter(cond2, dummyRelation))

    // Apply rule
    val rule = DynamicEcommercePartitionPruner()
    val optimizedPlan = rule(unoptimizedPlan)

    // Assert the plan was rewritten into a single Filter with combined And condition
    assert(optimizedPlan.isInstanceOf[Filter])
    val singleFilter = optimizedPlan.asInstanceOf[Filter]
    assert(singleFilter.condition == And(cond1, cond2))
    assert(singleFilter.child == dummyRelation)
  }
}
