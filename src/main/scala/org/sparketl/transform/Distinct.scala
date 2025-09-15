package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sparketl.parser.Transformation

case class Distinct() extends Transformation {
  val operation = "distinct"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.distinct()
  }
}

