package fr.insa.lacan.splitter

import org.apache.spark.sql.{DataFrame, SparkSession}

class RandomSplitter extends Splitter {

  private var _weights: Array[Double] = _
  private var _seed:    Option[Int]  = None

  def setWeights(weights: Array[Double]): this.type = {
    _weights = weights
    this
  }

  def getWeights: Array[Double] = _weights

  def setSeed(seed: Int): this.type = {
    _seed = Some(seed)
    this
  }

  def getSeed: Option[Int] = _seed

  def split(dataframe: DataFrame)(implicit spark: SparkSession): Array[DataFrame] = {
    _seed match {
      case Some(seed) => dataframe.randomSplit(_weights, seed)
      case None       => dataframe.randomSplit(_weights)
    }
  }
}
