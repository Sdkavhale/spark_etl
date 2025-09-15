package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sparketl.parser.Transformation

case class Cache() extends Transformation {
  val operation = "cache"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.cache()
  }
}
