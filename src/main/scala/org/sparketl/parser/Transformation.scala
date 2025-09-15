package org.sparketl.parser

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sparketl.transform._

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "operation"
)
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[Select], name = "select"),
  new JsonSubTypes.Type(value = classOf[Filter], name = "filter"),
  new JsonSubTypes.Type(value = classOf[WithColumn], name = "withColumn"),
  new JsonSubTypes.Type(value = classOf[WithColumnRenamed], name = "withColumnRenamed"),
  new JsonSubTypes.Type(value = classOf[Drop], name = "drop"),
  new JsonSubTypes.Type(value = classOf[Distinct], name = "distinct"),
  new JsonSubTypes.Type(value = classOf[Limit], name = "limit"),
  new JsonSubTypes.Type(value = classOf[OrderBy], name = "orderBy"),
  new JsonSubTypes.Type(value = classOf[GroupBy], name = "groupBy"),
  new JsonSubTypes.Type(value = classOf[Join], name = "join"),
  new JsonSubTypes.Type(value = classOf[Repartition], name = "repartition"),
  new JsonSubTypes.Type(value = classOf[Cache], name = "cache"),
  new JsonSubTypes.Type(value = classOf[Checkpoint], name = "checkpoint"),
  new JsonSubTypes.Type(value = classOf[Window], name = "window"),
  new JsonSubTypes.Type(value = classOf[Pivot], name = "pivot"),
  new JsonSubTypes.Type(value = classOf[Explode], name = "explode"),
//  new JsonSubTypes.Type(value = classOf[WriteTransformation], name = "write")
))
trait Transformation {
  def operation: String
  def apply(spark: SparkSession, df: DataFrame): DataFrame
}
