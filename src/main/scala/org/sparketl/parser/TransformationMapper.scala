package org.sparketl.parser


import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.sparketl.config.Transformation

import java.io.File

case class TransformationMapper(transformations: Seq[Transformation])

class JsonMapper {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def fromFile(file: File): TransformationMapper = {
    mapper.readValue(file, classOf[TransformationMapper])
  }
}