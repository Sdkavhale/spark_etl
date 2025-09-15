package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sparketl.parser.Transformation

case class Drop(columns: List[String]) extends Transformation {
  val operation = "drop"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.drop(columns: _*)
  }
}
