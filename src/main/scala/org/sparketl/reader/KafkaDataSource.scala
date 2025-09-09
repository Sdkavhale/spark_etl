package org.sparketl.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager

case class KafkaDataSource(brokers: String, topic: String, options: Map[String, String]) extends DataSource {
  private val logger = LogManager.getLogger(getClass)

  override def init(spark: SparkSession): Boolean = {
    try {
      val df = read(spark)
      df.printSchema() // Just to trigger reading schema
      logger.info(s"Kafka stream $topic at $brokers is accessible.")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Failed to access Kafka stream at $brokers", e)
        false
    }
  }

  override def read(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .options(options)
      .load()
  }

  override def write(df: DataFrame): Unit = {
    df.writeStream
      .format("console") // Example: write to console for testing
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
