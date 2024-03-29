package fr.insa.lacan

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.immutable._
import scala.reflect.ClassTag

package object utils {

  /*
  * Similar to asInstanceOf, but return an Option when the casting failed.
  * */
  def asInstanceOfOption[T: ClassTag](o: Any): Option[T] = Some(o) collect { case m: T => m }

  /*
  * Create a new parameterized instance of a class by calling its setter methods after instantiation.
  *
  * Call newParameterizedInstance("org.foo.MyClass", Map("myArgument" -> 2)) will result in:
  * val obj = new org.foo.MyClass()
  * obj.setMyArgument(2)
  *
  * We do not support setter methods with more than one argument since we don't use a Map[String, Array[Any]].
  * We do not support setter methods which take a boxed AnyVal (java.lang.Integer).
  * */
  def newParameterizedInstance[T: ClassTag](classname: String, parameters: Map[String, Any]): T = {

    // Create new instance.
    val obj = Class.forName(classname).getConstructor().newInstance()

    for ((name, value) <- parameters) {

      // Un-boxing Java Object to Scala AnyVal.
      val cls = value match {
        case _: java.lang.Integer => classOf[Int      ]
        case _: java.lang.Double  => classOf[Double   ]
        case _: java.lang.Boolean => classOf[Boolean  ]
        case _: Map[_, _]         => classOf[Map[_, _]] // To interpret a Map$Map1 type as a Map type.
        case o                    => o.getClass
      }

      // Invoke setter method to configure the instance.
      val method = obj.getClass.getMethod("set" + name.capitalize, Array(cls): _*)

      method.invoke(obj, Array(cast[AnyRef](value)): _*) // Auto-boxing AnyVal to AnyRef (Object).
    }

    cast[T](obj)
  }

  /*
  * Cast an array of type Array[_] to an array of type Array[T] along with its contents.
  * */
  def castAll[T: ClassTag](array: Array[_]): Array[T] = {
    array
      .map(asInstanceOfOption[T])
      .map(_.getOrElse(throw new ClassCastException("Failed to cast array content")))
  }

  /*
  * Cast an object of type Any to an object of T.
  * */
  def cast[T: ClassTag](obj: Any): T = {
    asInstanceOfOption[T](obj)
      .getOrElse(throw new ClassCastException("Failed to cast object"))
  }

  /*
  * Execute a function and collect time metric.
  * */
  def time[T](f: => T): (T, Long) = {
    val start  = System.currentTimeMillis()
    val result = f
    val end    = System.currentTimeMillis()
    (result, end - start)
  }

  /*
  * Execute a function within a Spark session.
  * */
  def withSparkSession[T](sparkConf: SparkConf, f: SparkSession => () => T): T = {

    // SparkSession.clearActiveSession()
    // SparkSession.clearDefaultSession()

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val result = f(spark)()
    spark.stop()

    result
  }

  /*
  * Execute a function if the specified options are defined.
  * */
  def ifDefined[A](optionA: Option[A])(f: A => Unit): Unit = {
    optionA.foreach(a => f(a))
  }

  def ifDefined[A, B](optionA: Option[A], optionB: Option[B])(f: (A, B) => Unit): Unit = {
    optionA.foreach(a => optionB.foreach(b => f(a, b)))
  }

  def ifDefined[A, B, C](optionA: Option[A], optionB: Option[B], optionC: Option[C])(f: (A, B, C) => Unit): Unit = {
    optionA.foreach(a => optionB.foreach(b => optionC.foreach(c => f(a, b, c))))
  }
}
