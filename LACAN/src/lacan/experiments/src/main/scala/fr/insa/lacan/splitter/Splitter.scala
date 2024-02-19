package fr.insa.lacan.splitter

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Splitter {
  def split(dataframe: DataFrame)(implicit spark: SparkSession): Array[DataFrame]
}
