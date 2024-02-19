package fr.insa.lacan.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class ColumnDuplicator(override val uid: String) extends Transformer {

  var _existingName: String = _
  var      _newName: String = _

  def getExistingName: String = _existingName

  def setExistingName(existingName: String): this.type = {
    _existingName = existingName
    this
  }

  def getNewName: String = _newName

  def setNewName(newName: String): this.type = {
    _newName = newName
    this
  }

  def this() = this(Identifiable.randomUID("ColumnDuplicator"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn(_newName, dataset.col(_existingName))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val existingName = _existingName // Avoid VarClosure.
    val existing = schema.fields.find(_.name == existingName).getOrElse(throw new NoSuchElementException)
    StructType(schema.fields :+ StructField(_newName, existing.dataType, existing.nullable, existing.metadata))
  }
}
