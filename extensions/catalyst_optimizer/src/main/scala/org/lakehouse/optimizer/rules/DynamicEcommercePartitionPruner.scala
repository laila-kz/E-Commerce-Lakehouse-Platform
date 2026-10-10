package org.lakehouse.optimizer.rules

import java.time.LocalDate

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

    // Pattern Match 2: Optimize date-truncation equality checks into direct range boundaries.
    // date_format(event_time, 'yyyy-MM-dd') = 'D'
    //   => event_time >= 'D 00:00:00' AND event_time < 'D+1 00:00:00'
    // The upper bound is exclusive (start of the next day) so that sub-second rows such as
    // 23:59:59.999999 remain inside the day and no rows are wrongly dropped.
    case Filter(EqualTo(DateFormatClass(attr: AttributeReference, Literal(fmt, _), _), Literal(dateVal, _)), child)
        if fmt != null && fmt.toString == "yyyy-MM-dd" && dateVal != null =>
      logInfo(s"[EcommerceOptimizer] Optimizing date_format equality on ${attr.name} into partition range scan")
      val day = dateVal.toString
      val startOfDay = Literal(s"$day 00:00:00")
      val nextDayStart = Literal(s"${LocalDate.parse(day).plusDays(1)} 00:00:00")
      val rangeCondition = And(
        GreaterThanOrEqual(attr, Cast(startOfDay, attr.dataType)),
        LessThan(attr, Cast(nextDayStart, attr.dataType))
      )
      Filter(rangeCondition, child)
  }
}
