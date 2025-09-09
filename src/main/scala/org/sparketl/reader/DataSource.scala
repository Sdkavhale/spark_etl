package org.sparketl.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataSource {
  /** Initialize the connection and check connectivity */
  def init(spark: SparkSession): Boolean

  /** Read data and return DataFrame */
  def read(spark: SparkSession): DataFrame

  /** Write DataFrame to the target */
  def write(df: DataFrame): Unit
}