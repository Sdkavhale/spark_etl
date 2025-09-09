package org.sparketl.transform


import org.apache.spark.sql.SparkSession

trait Transformation {
  def operation: String
  def apply(spark: SparkSession, df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame
}