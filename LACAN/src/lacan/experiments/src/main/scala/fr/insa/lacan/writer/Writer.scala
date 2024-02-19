package fr.insa.lacan.writer

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Writer {
  def write(dataframe: DataFrame)(implicit spark: SparkSession): Unit
}
