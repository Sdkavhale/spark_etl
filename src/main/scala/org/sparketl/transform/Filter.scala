package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import org.sparketl.parser.Transformation

case class Filter(condition: String) extends Transformation {
  val operation = "filter"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.filter(expr(condition))
  }
}
