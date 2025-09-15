package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sparketl.parser.Transformation

case class Limit(count: Int) extends Transformation {
  val operation = "limit"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.limit(count)
  }
}
