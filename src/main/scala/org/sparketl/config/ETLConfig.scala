package org.sparketl.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.sparketl.parser.JobDefinition

import java.nio.file.{Files, Paths}

object ETLConfig {
  def load(path: String): JobDefinition = {
    val content = new String(Files.readAllBytes(Paths.get(path)))
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(content, classOf[JobDefinition])
  }
}