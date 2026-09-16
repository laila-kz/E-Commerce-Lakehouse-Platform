package org.lakehouse.optimizer.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Custom Catalyst Optimizer Rule:
 * Inspects logical plans and rewrites redundant multi-layered filters and date predicates
 * on e-commerce event streams to optimize Delta Lake scan planning.
 */
case class DynamicEcommercePartitionPruner() extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    // Pattern Match 1: Eliminate redundant nested filters (Filter -> Filter -> Child)
    case Filter(cond1, Filter(cond2, child)) =>
      logInfo(s"[EcommerceOptimizer] Folding contiguous filters: ($cond1) AND ($cond2)")
      val combinedCondition = And(cond1, cond2)
      Filter(combinedCondition, child)

    // Pattern Match 2: Optimize date-truncation equality checks into direct range boundaries
    case Filter(EqualTo(DateFormatClass(attr: AttributeReference, Literal(fmt, _), _), Literal(dateVal, _)), child)
        if fmt != null && fmt.toString == "yyyy-MM-dd" && dateVal != null =>
      logInfo(s"[EcommerceOptimizer] Optimizing date_format equality on ${attr.name} into partition range scan")
      val startOfDay = Literal(s"${dateVal} 00:00:00")
      val endOfDay   = Literal(s"${dateVal} 23:59:59")
      val rangeCondition = And(
        GreaterThanOrEqual(attr, Cast(startOfDay, attr.dataType)),
        LessThanOrEqual(attr, Cast(endOfDay, attr.dataType))
      )
      Filter(rangeCondition, child)
  }
}
