package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.sparketl.parser.Transformation

case class WindowFunction(function: String, alias: String, column: Option[String])

case class Window(
                                 partition_by: List[String],
                                 order_by: List[String],
                                 window_name: String,
                                 functions: List[WindowFunction]
                               ) extends Transformation {
  val operation = "window"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    val windowSpec = org.apache.spark.sql.expressions.Window
      .partitionBy(partition_by.map(col): _*)
      .orderBy(order_by.map(col): _*)

    functions.foldLeft(df) { (tempDF, func) =>
      val colExpr = func.column.map(col).getOrElse(lit(1))
      val newCol = func.function.toLowerCase match {
        case "row_number" => row_number().over(windowSpec)
        case "sum" => sum(colExpr).over(windowSpec)
        case "avg" => avg(colExpr).over(windowSpec)
        case "max" => max(colExpr).over(windowSpec)
        case "min" => min(colExpr).over(windowSpec)
        case _ => throw new IllegalArgumentException(s"Unsupported window function: ${func.function}")
      }
      tempDF.withColumn(func.alias, newCol)
    }
  }
}
