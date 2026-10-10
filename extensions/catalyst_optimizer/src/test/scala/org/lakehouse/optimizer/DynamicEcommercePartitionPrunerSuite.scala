package org.lakehouse.optimizer

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.TimestampType
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

  test("DynamicEcommercePartitionPruner rewrites date_format equality into a half-open day range") {
    val attr = 'event_time.timestamp
    val plan = Filter(
      EqualTo(DateFormatClass(attr, Literal("yyyy-MM-dd")), Literal("2024-01-01")),
      LocalRelation(attr)
    )

    val optimized = DynamicEcommercePartitionPruner()(plan)

    assert(optimized.isInstanceOf[Filter])
    val rewritten = optimized.asInstanceOf[Filter].condition
    val expected = And(
      GreaterThanOrEqual(attr, Cast(Literal("2024-01-01 00:00:00"), TimestampType)),
      LessThan(attr, Cast(Literal("2024-01-02 00:00:00"), TimestampType))
    )
    assert(rewritten == expected)
  }

  test("date_format day predicate and its range rewrite select the same rows across day boundaries") {
    val attr = 'event_time.timestamp
    val original = EqualTo(DateFormatClass(attr, Literal("yyyy-MM-dd")), Literal("2024-01-01"))
    val plan = Filter(original, LocalRelation(attr))
    val rewritten = DynamicEcommercePartitionPruner()(plan).asInstanceOf[Filter].condition

    val boundaryTimestamps = Seq(
      "2023-12-31 23:59:59.999999",
      "2024-01-01 00:00:00",
      "2024-01-01 00:00:00.000001",
      "2024-01-01 12:00:00",
      "2024-01-01 23:59:59",
      "2024-01-01 23:59:59.999999",
      "2024-01-02 00:00:00"
    ).map(Timestamp.valueOf)

    val rows = boundaryTimestamps.map(ts => InternalRow(DateTimeUtils.fromJavaTimestamp(ts)))

    def selectedWith(condition: Expression): Seq[Timestamp] = {
      val bound = BindReferences.bindReference(condition, Seq(attr))
      rows.zip(boundaryTimestamps)
        .collect { case (row, ts) if bound.eval(row).asInstanceOf[Boolean] => ts }
    }

    val originalRows = selectedWith(original)
    val rewrittenRows = selectedWith(rewritten)

    // Rule enabled vs disabled must return the same rows.
    assert(rewrittenRows == originalRows)

    // And the selected set must include both day boundaries of 2024-01-01, including sub-second rows.
    assert(originalRows == Seq(
      Timestamp.valueOf("2024-01-01 00:00:00"),
      Timestamp.valueOf("2024-01-01 00:00:00.000001"),
      Timestamp.valueOf("2024-01-01 12:00:00"),
      Timestamp.valueOf("2024-01-01 23:59:59"),
      Timestamp.valueOf("2024-01-01 23:59:59.999999")
    ))
  }

  test("date_format predicate with a different format string is left unchanged") {
    val attr = 'event_time.timestamp
    val plan = Filter(
      EqualTo(DateFormatClass(attr, Literal("yyyy/MM/dd")), Literal("2024/01/01")),
      LocalRelation(attr)
    )
    assert(DynamicEcommercePartitionPruner()(plan) == plan)
  }

  test("date_format predicate compared against a non-literal is left unchanged") {
    val attr = 'event_time.timestamp
    val other = 'other.string
    val plan = Filter(
      EqualTo(DateFormatClass(attr, Literal("yyyy-MM-dd")), other),
      LocalRelation(attr, other)
    )
    assert(DynamicEcommercePartitionPruner()(plan) == plan)
  }
}
