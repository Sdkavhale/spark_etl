package org.sparketl.parser


case class JobDefinition(
                          jobId: Int,
                          jobName: String,
                          jobType: String,
                          sparkConfiguration: Map[String, String],
                          sources: List[Source],
                          target: List[Target],
                          transformations: List[Transformation]
                        )