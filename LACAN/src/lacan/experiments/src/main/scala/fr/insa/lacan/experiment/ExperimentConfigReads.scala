package fr.insa.lacan.experiment

import fr.insa.lacan.reader.Reader
import fr.insa.lacan.writer.Writer
import fr.insa.lacan.splitter.Splitter
import fr.insa.lacan.utils._

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.apache.spark.SparkConf
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable._
import scala.reflect.ClassTag

object ExperimentConfigReads {

  /*
  * Create a new parameterized instance from a class configuration.
  * */
  private def fromClassConfig[T: ClassTag](config: ClassConfig): T = {
    newParameterizedInstance[T](config.classname, config.parameters)
  }

  /*
  * Create a new reader by applying a function to an existing reader.
  * */
  private def map[C, T](f: C => T)(implicit rds: Reads[C]): Reads[T] = {
    new Reads[T] {
      override def reads(json: JsValue): JsResult[T] = {
        json.validate[C](rds) match {
          case JsSuccess(value, path) => JsSuccess(f(value), path)
          case JsError(errors)        => JsError(errors)
        }
      }
    }
  }

  /*
  * Read the "parameters" field from a ClassConfig in order to configure a parameterized instance.
  * */
  implicit val anyReads: Reads[Any] = new Reads[Any] {

    override def reads(json: JsValue): JsResult[Any] = {
      json match {
        case JsArray(values)  => toHomogeneousArray(values.toStream)
        case JsObject(values) => toMap(values.toMap)
        case value            => toAny(value)
      }
    }

    /*
    * Convert Json arrays into type Array[Int] | Array[Double] | Array[Boolean] | Array[String].
    * */
    private def toHomogeneousArray(json: Seq[JsValue]): JsResult[Array[_]] = {
      val (err, any) = json.map(toAny).partition(_.isError)
      err.headOption match {
        case Some(JsError(errors)) => JsError(errors)
        case Some(JsSuccess(_, _)) => throw new IllegalStateException
        case None                  =>
          val values = any.map(_.get).toArray
          values.headOption match {
            case Some(_: java.lang.Integer) => JsSuccess(castAll[Int    ](values))
            case Some(_: java.lang.Double ) => JsSuccess(castAll[Double ](values))
            case Some(_: java.lang.Boolean) => JsSuccess(castAll[Boolean](values))
            case Some(_: java.lang.String ) => JsSuccess(castAll[String ](values))
            case Some(_)                    => JsError("Type must be Int | Double | Boolean | String")
            case None                       => JsError("Array must not be empty to infer type")
          }
      }
    }

    /*
    * Convert Json objects into type Map[String, Int | Double | Boolean | String].
    * */
    private def toMap(json: Map[String, JsValue]): JsResult[Map[String, _]] = {
      val (err, any) = json.mapValues(toAny).partition(_._2.isError)
      err.headOption match {
        case Some((_, JsError(errors))) => JsError(errors)
        case Some((_, JsSuccess(_, _))) => throw new IllegalStateException
        case None                       =>
          val values = any.mapValues(_.get)
          JsSuccess(values)
      }
    }

    /*
    * Convert Json values into type Int | Double | Boolean | String.
    * */
    private def toAny(json: JsValue): JsResult[Any] = {
      json match {
        case JsNumber(number) =>
          if (number.isValidInt) JsSuccess(number.intValue)
          else                   JsSuccess(number.doubleValue)
        case JsBoolean(bool)  => JsSuccess(bool)
        case JsString(string) => JsSuccess(string)
        case _                => JsError("Type must be Int | Double | Boolean | String")
      }
    }
  }

  implicit val classConfigReads: Reads[ClassConfig] = (
    (JsPath \ "classname" ).read[String] and
    (JsPath \ "parameters").read[Map[String, Any]]
  )(ClassConfig)

  implicit val platformConfReads: Reads[SparkConf] = map(
    (platformConf: Map[String, String]) => new SparkConf().setAll(platformConf)
  )

  implicit val storageLevelReads: Reads[StorageLevel] = map(
    (level: String) => cast[StorageLevel](StorageLevel.getClass.getMethod(level).invoke(StorageLevel))
  )

  implicit val pipelineReads: Reads[Pipeline] = map(
    (stages: Array[ClassConfig]) => new Pipeline().setStages(stages.map(fromClassConfig[PipelineStage]))
  )

  implicit val    readerReads: Reads[Reader   ] = map(fromClassConfig[Reader   ])
  implicit val    writerReads: Reads[Writer   ] = map(fromClassConfig[Writer   ])
  implicit val  splitterReads: Reads[Splitter ] = map(fromClassConfig[Splitter ])
  implicit val evaluatorReads: Reads[Evaluator] = map(fromClassConfig[Evaluator])

  implicit val platformMetricsConfigReads: Reads[PlatformMetricsConfig] = (
    (JsPath \ "level"    ).read[String] and
    (JsPath \ "fit"      ).read[Writer] and
    (JsPath \ "transform").read[Writer]
  )(PlatformMetricsConfig)

  implicit val applicativeMetricsConfigReads: Reads[ApplicativeMetricsConfig] =
    (JsPath \ "writer").read[Writer]
      .map(ApplicativeMetricsConfig)

  implicit val metricsConfigReads: Reads[MetricsConfig] = (
    (JsPath \ "applicative").readNullable[ApplicativeMetricsConfig] and
    (JsPath \ "platform"   ).readNullable[   PlatformMetricsConfig]
  )(MetricsConfig)

  implicit val executionConfigReads: Reads[ExecutionConfig] = (
    (JsPath \ "storage").read[StorageLevel] and
    (JsPath \ "lazily" ).read[Boolean     ]
  )(ExecutionConfig)

  implicit val workflowConfigReads: Reads[WorkflowConfig] = (
    (JsPath \ "reader"        ).read[Reader  ]         and
    (JsPath \ "transformers"  ).read[Pipeline]         and
    (JsPath \ "splitter"      ).readNullable[Splitter] and
    (JsPath \ "preprocessors" ).read[Pipeline]         and
    (JsPath \ "estimators"    ).read[Pipeline]         and
    (JsPath \ "postprocessors").read[Pipeline]         and
    (JsPath \ "evaluators"    ).read[Map[String, Evaluator]]
  )(WorkflowConfig)

  implicit val experimentConfigReads: Reads[ExperimentConfig] = (
    (JsPath \ "platform" ).read[    SparkConf  ] and
    (JsPath \ "execution").read[ExecutionConfig] and
    (JsPath \ "metrics"  ).read[  MetricsConfig] and
    (JsPath \ "workflow" ).read[ WorkflowConfig]
  )(ExperimentConfig)
}
