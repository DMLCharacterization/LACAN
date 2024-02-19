package fr.insa.lacan.experiment

import fr.insa.lacan.reader.Reader
import fr.insa.lacan.splitter.Splitter
import fr.insa.lacan.writer.Writer

import org.apache.spark.SparkConf
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable._
import scala.language.existentials

case class ExperimentConfig(
  platformConf:     SparkConf,
     execution: ExecutionConfig,
       metrics:   MetricsConfig,
      workflow:  WorkflowConfig
)

case class WorkflowConfig(
          reader: Reader,
    transformers: Pipeline,
        splitter: Option[Splitter],
   preprocessors: Pipeline,
      estimators: Pipeline,
  postprocessors: Pipeline,
      evaluators: Map[String, Evaluator]
)

case class MetricsConfig(
  applicative: Option[ApplicativeMetricsConfig],
     platform: Option[PlatformMetricsConfig]
)

case class PlatformMetricsConfig(
   level: String,
   fitWriter: Writer,
   transformWriter: Writer
)

case class ApplicativeMetricsConfig(
  writer: Writer
)

case class ClassConfig(
   classname: String,
  parameters: Map[String, Any]
)

case class ExecutionConfig(
  storage: StorageLevel,
   lazily: Boolean
)
