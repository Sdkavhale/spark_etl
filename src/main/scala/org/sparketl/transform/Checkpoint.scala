package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sparketl.parser.Transformation

case class Checkpoint() extends Transformation {
  val operation = "checkpoint"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints") // or use configuration
    df.checkpoint()
  }
}
