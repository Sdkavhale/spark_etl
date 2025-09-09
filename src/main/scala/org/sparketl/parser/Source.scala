package org.sparketl.parser


case class Source(
                   path: String,
                   format: String,
                   options: Map[String, String]
                 )