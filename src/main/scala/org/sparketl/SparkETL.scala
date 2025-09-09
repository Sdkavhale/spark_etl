package org.sparketl

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.LogManager
import org.sparketl.config.ETLConfig

import java.time.Instant
import java.time.Duration


object SparkETL {
   val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: spark-submit --class Main <jar-file> <config-path>")
      sys.exit(1)
    }

    val configPath = args(0)
    val config = ETLConfig.load(configPath)

    val spark = SparkSession.builder()
      .appName("Spark ETL Library")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .getOrCreate()

    try {
      logger.info("Starting ETL process...")
      val start = Instant.now()

      val end = Instant.now()
      val duration = Duration.between(start, end)
      println(s"Execution Time: ${duration.toMillis} ms")
      logger.info("ETL process completed successfully.")
    } catch {
      case e: Exception =>
        logger.error("ETL process failed: ", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}
