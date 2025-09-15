package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sparketl.parser.Transformation

case class WithColumnRenamed(old_name: String, new_name: String) extends Transformation {
  val operation = "withColumnRenamed"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.withColumnRenamed(old_name, new_name)
  }
}
