package org.sparketl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sparketl.parser.JobDefinition


object Executor {
  def initializeSources(spark: SparkSession, jobDef: JobDefinition): DataFrame = {
    if (jobDef.sources.isEmpty)
      throw new RuntimeException("No source defined in job config")

    // For simplicity, we handle only the first source as main input DataFrame
    val src = jobDef.sources.head
    val df = spark.read
      .format(src.format)
      .options(src.options)
      .load(src.path)

    // Register as temporary view if joins are required later
    df.createOrReplaceTempView("left")

    // Validate connectivity by triggering an action
    df.limit(1).count()

    df
  }

  def applyTransformations(spark: SparkSession, jobDef: JobDefinition, initialDF: DataFrame): DataFrame = {
    jobDef.transformations.foldLeft(initialDF) { (df, transformation) =>
      println(s"Applying transformation: ${transformation.operation}")

      val updatedDF = transformation.apply(spark, df)

      // Handle special cases like join
//      transformation match {
//        case jt: JoinTransformation =>
//          val rightDF = spark.table(jt.right_dataset)
//          rightDF.createOrReplaceTempView("right")
//        case _ =>
//      }

      updatedDF
    }
  }

}
