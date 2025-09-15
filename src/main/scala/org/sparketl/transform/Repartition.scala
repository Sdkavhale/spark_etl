package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.sparketl.parser.Transformation

case class Repartition(num_partitions: Int, columns: List[String]) extends Transformation {
  val operation = "repartition"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    df.repartition(num_partitions, columns.map(col): _*)
  }
}
