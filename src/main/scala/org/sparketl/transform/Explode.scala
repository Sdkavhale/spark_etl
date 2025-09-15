package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.sparketl.parser.Transformation

case class Explode(column: String, alias: String) extends Transformation {
  val operation = "explode"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.withColumn(alias, explode(col(column)))
  }
}

