package org.sparketl.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager

case class TableDataSource(tableName: String, options: Map[String, String]) extends DataSource {
  private val logger = LogManager.getLogger(getClass)

  override def init(spark: SparkSession): Boolean = {
    try {
      val df = spark.read.format("iceberg").options(options).load(tableName)
      df.limit(1).count()
      logger.info(s"Table $tableName is accessible.")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Failed to access table $tableName", e)
        false
    }
  }

  override def read(spark: SparkSession): DataFrame = {
    spark.read.format("iceberg").options(options).load(tableName)
  }

  override def write(df: DataFrame): Unit = {
    df.write.format("iceberg").options(options).mode("append").save(tableName)
  }
}
