package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.sparketl.parser.Transformation

case class Aggregation(agg_function: String, column: String, alias: String)

case class GroupBy(columns: List[String], aggregations: List[Aggregation]) extends Transformation {
  val operation = "groupBy"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    val grouped = df.groupBy(columns.map(col): _*)
    val aggExprs = aggregations.map { agg =>
      val c = col(agg.column)
      agg.agg_function.toLowerCase match {
        case "sum" => sum(c).as(agg.alias)
        case "avg" => avg(c).as(agg.alias)
        case "max" => max(c).as(agg.alias)
        case "min" => min(c).as(agg.alias)
        case "count" => count(c).as(agg.alias)
        case _ => throw new IllegalArgumentException(s"Unsupported aggregation: ${agg.agg_function}")
      }
    }
    grouped.agg(aggExprs.head, aggExprs.tail: _*)
  }
}
