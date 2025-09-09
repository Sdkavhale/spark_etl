package org.sparketl.parser


case class JobConfig(
                      jobId: Int,
                      jobName: String,
                      jobType: String,
                      sparkConfiguration: Map[String, String],
                      sources: Seq[Source],
                      target: Seq[Target],
                      transformations: Seq[Transformation]
                    )