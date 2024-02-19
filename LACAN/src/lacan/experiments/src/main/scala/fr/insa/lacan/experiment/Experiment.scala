package fr.insa.lacan.experiment

import fr.insa.lacan.metrics.MetricsCollectors
import fr.insa.lacan.utils._

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector

import scala.collection.immutable._
import scala.language.existentials

class Experiment(config: ExperimentConfig) {

  private val         reader = config.workflow.reader
  private val       splitter = config.workflow.splitter
  private val   transformers = config.workflow.transformers
  private val  preprocessors = config.workflow.preprocessors
  private val postprocessors = config.workflow.postprocessors
  private val     estimators = config.workflow.estimators
  private val     evaluators = config.workflow.evaluators

  private val  lazily = config.execution.lazily
  private val storage = config.execution.storage

  private val platformLevel  = config.metrics.platform.map(_.level)
  private val platformFitWriter = config.metrics.platform.map(_.fitWriter)
  private val platformTransformWriter = config.metrics.platform.map(_.transformWriter)
  private val applicativeWriter = config.metrics.applicative.map(_.writer)

  private val platformConf = config.platformConf

  def perform(): Unit = {
    withSparkSession(platformConf, implicit spark => execute)
  }

  private def execute()(implicit spark: SparkSession): Unit = {

    // Reading raw dataset.
    val raw = reader.read()

    // Apply transformation.
    val transformation = transformers.fit(raw)
    val data = transformation.transform(raw)

    // Perform train/test split if enabled.
    val Array(trainData, testData) = splitter.map(_.split(data)).getOrElse(Array(data, data))

    // Apply preprocessing.
    val preprocessing = preprocessors.fit(trainData)
    val Array(train, test) = Array(trainData, testData).map(preprocessing.transform)

    val collector = platformLevel.map(MetricsCollectors.fromLevel)

    ifDefined(collector)(_.begin())

    val (model, fitTime) = time { estimators.fit(train) }

    ifDefined(collector)(_.end())

    val fitMetrics = collector.map(_.collect())

    ifDefined(collector)(_.begin())

    val (predictions, transformTime) = time {
      val predictions = model.transform(test)
      predictions.count()
      predictions
    }

    ifDefined(collector)(_.end())

    val transformMetrics = collector.map(_.collect())

    // Compute and save metrics if enabled.
    import spark.implicits._
    ifDefined(      platformFitWriter,       fitMetrics)((writer, metrics) => writer.write(metrics))
    ifDefined(platformTransformWriter, transformMetrics)((writer, metrics) => writer.write(metrics))
    ifDefined(applicativeWriter)(writer => {

      val results = predictions

      val evaluMetrics = evaluators.mapValues(_.evaluate(results))
      val appliMetrics = Map[String, Double](
              "fitTime" ->       fitTime,
        "transformTime" -> transformTime,
           "trainCount" -> train.count(),
            "testCount" ->  test.count(),
             "features" -> train.first().getAs[Vector]("features").size
      )

      writer.write((appliMetrics.toSeq ++ evaluMetrics.toSeq).toDF("metric", "value"))
    })
  }
}
