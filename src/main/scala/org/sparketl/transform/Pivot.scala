package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.sparketl.parser.Transformation

case class PivotAggregation(agg_function: String, column: String, alias: String)

case class Pivot(
                                column: String,
                                values: List[String],
                                aggregation: PivotAggregation
                              ) extends Transformation {
  val operation = "pivot"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    val grouped = df.groupBy()
    val pivoted = grouped.pivot(column, values)
    val colExpr = col(aggregation.column)
    val aggExpr = aggregation.agg_function.toLowerCase match {
      case "sum" => sum(colExpr).as(aggregation.alias)
      case "avg" => avg(colExpr).as(aggregation.alias)
      case "max" => max(colExpr).as(aggregation.alias)
      case "min" => min(colExpr).as(aggregation.alias)
      case "count" => count(colExpr).as(aggregation.alias)
      case _ => throw new IllegalArgumentException(s"Unsupported aggregation: ${aggregation.agg_function}")
    }
    pivoted.agg(aggExpr)
  }
}

