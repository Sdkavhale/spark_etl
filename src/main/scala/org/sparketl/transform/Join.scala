package org.sparketl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import org.sparketl.parser.Transformation

case class Join(
                               right_dataset: String,
                               join_type: String,
                               join_condition: String
                             ) extends Transformation {
  val operation = "join"
  override def apply(spark: SparkSession, df: DataFrame): DataFrame = {
    val rightDF = spark.table(right_dataset)
    df.join(rightDF, expr(join_condition), join_type)
  }
}
