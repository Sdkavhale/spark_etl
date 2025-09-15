package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.sparketl.parser.Transformation

case class OrderByColumn(column: String, order: String)

case class OrderBy(columns: List[OrderByColumn]) extends Transformation {
  val operation = "orderBy"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    val cols = columns.map(c => if (c.order.toLowerCase == "desc") col(c.column).desc else col(c.column).asc)
    df.orderBy(cols: _*)
  }
}
