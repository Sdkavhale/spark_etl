package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.sparketl.parser.Transformation

case class Select(columns: List[String]) extends Transformation {
  val operation = "select"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.select(columns.map(col): _*)
  }
}
