package org.sparketl.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager

case class FileDataSource(path: String, format: String, options: Map[String, String]) extends DataSource {
  private val logger = LogManager.getLogger(getClass)

  override def init(spark: SparkSession): Boolean = {
    try {
      val df = spark.read.format(format).options(options).load(path)
      df.limit(1).count() // Simple operation to check connectivity
      logger.info(s"File source at $path is accessible.")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Failed to access file at $path", e)
        false
    }
  }

  override def read(spark: SparkSession): DataFrame = {
    spark.read.format(format).options(options).load(path)
  }

  override def write(df: DataFrame): Unit = {
    df.write.format(format).options(options).save(path)
  }
}