package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import org.sparketl.parser.Transformation

case class WithColumn(column_name: String, expression: String) extends Transformation {
  val operation = "withColumn"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.withColumn(column_name, expr(expression))
  }
}
