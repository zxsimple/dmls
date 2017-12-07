package com.zxsimple.dmls.util

import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics, RegressionMetrics}
import org.apache.spark.rdd.RDD

/**
 * 度量分类模型的准确度
 */
object EvaluationUtils {

  /**
   * multiClassMetrics
   */
  def multiClassMetrics(predictionAndLabels: RDD[(Double, Double)]): MulticlassMetrics = {
    new MulticlassMetrics(predictionAndLabels)
  }

  /**
   * binaryClassMetrics
   */
  def binaryClassMetrics(scoreAndLabels: RDD[(Double, Double)]): BinaryClassificationMetrics = {
    new BinaryClassificationMetrics(scoreAndLabels)
  }

  /**
   * regressionMetrics
   */
  def regressionMetrics(predictionAndObservations: RDD[(Double, Double)]): RegressionMetrics = {
    new RegressionMetrics(predictionAndObservations)
  }

}