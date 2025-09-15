package org.sparketl

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.LogManager
import org.sparketl.config.ETLConfig

import org.sparketl.utils.ConsoleColorUtils._

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
      .master("local")
      .appName("Spark ETL Library")
      .getOrCreate()

    try {
      logger.info("Starting ETL process...")
      config.sparkConfiguration.foreach { case (k, v) =>
        spark.conf.set(k, v)
      }
      val start = Instant.now()

      // Initialize sources
      val initialDF = Executor.initializeSources(spark, config)

      // Apply transformations using reflection
      val finalDF = Executor.applyTransformations(spark, config, initialDF)

      finalDF.write.option("header", "true").mode("overwrite").csv("/Users/shubhamk/Downloads/spark_etl/outputs")

      // Stop Spark session after completion
      spark.stop()

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
