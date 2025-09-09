package org.sparketl.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.file.{Files, Paths}

case class Source(path: String, format: String, options: Map[String, String])
case class Target(location: String, table_name: String, format: String)
case class Transformation(operation: String, columns: Option[List[String]], condition: Option[String], column_name: Option[String], expression: Option[String])
case class ETLConfig(source: Source, target: Target, transformations: List[Transformation])

object ETLConfig {
  def load(path: String): ETLConfig = {
    val content = new String(Files.readAllBytes(Paths.get(path)))
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(content, classOf[ETLConfig])
  }
}