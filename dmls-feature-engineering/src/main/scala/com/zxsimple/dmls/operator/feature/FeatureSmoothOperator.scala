package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class FeatureSmoothOperator extends OperatorTemplate {
  /**
    * 验证参数
    *
    * @param properties
    */
  override def validate(properties: Properties): (Boolean, String) = {
    val functionName = properties.get("functionName")
    val parm1 = properties.get("pram1").toString.toDouble
    if (!functionName.equals("Z-Score")) {
      val parm2 = properties.get("pram2").toString.toDouble
      if (parm1 >= parm2) {
        (false, "参数验证失败")
      }
    }
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    val colNamesList = data.columns
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList
    val functionName = properties.get("functionName").toString
    val parm1 = properties.get("pram1").toString.toDouble
    val parm2 = if (functionName.equals("Z-Score")) {
      0.0
    } else {
      properties.get("pram2").toString.toDouble
    }
    //自定义数据
    val otherColsList = colNamesList.diff(selectedColsList)
    val tableName = "features_smooth"
    val otherName = tableName + "_other"
    val featureName = tableName + "_feature"
    data.registerTempTable(tableName)

    val selectedSql = "select `" + selectedColsList.mkString("`,`") + "` from " + tableName
    val otherSql = "select `" + otherColsList.mkString("`,`") + "` from " + tableName

    val selectedDF = hc.sql(selectedSql)
    val otherDF = hc.sql(otherSql)
    otherDF.registerTempTable(otherName)

    val selectedVector = selectedDF.map(row => {
      //seq下标从1开始
      val seq = row.toSeq.toArray.map(_.toString.toDouble)
      Vectors.dense(seq)
    })

    //通过自定义的smoth方法，对特征异常值进行平滑处理
    val feature_smothed = smoth(hc.sparkContext, selectedVector, parm1, parm2, functionName)


    //RDD[Vector]转换为DF
    val featureDF = TaskUtils.rddVectorToDF(hc, feature_smothed, selectedColsList)
    featureDF.registerTempTable(featureName)

    //合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)

    println("特征异常值平滑算子结束，输出结果为：")
    /*
      *将特征异常值平滑的数据保存中间结果
      */
    currentNode.setOutPortOneData(result)

  }

  /*
  *特征异常平滑方法—Z-Score
 */
  def z_score_smooth(features: RDD[Vector], n: Double): RDD[Vector] = {

    val matrix = new RowMatrix(features)
    val matrixSummary = matrix.computeColumnSummaryStatistics()
    val mean = matrixSummary.mean.toArray
    val Q = matrixSummary.variance.toArray.map(line => math.sqrt(line))

    val z_score_smoth = features.map { line =>
      val value = line.toArray
      val size = value.size
      var i = 0
      while (i < size) {
        value(i) = if (value(i) < (-1 * n * Q(i) + mean(i))) (-1 * n * Q(i) + mean(i)) else if (value(i) > (n * Q(i) + mean(i))) (n * Q(i) + mean(i)) else value(i)
        i += 1
      }
      Vectors.dense(value)
    }
    z_score_smoth
  }

  /*
    *特征异常平滑方法—阀值平滑
   */
  def threshold_smooth(features: RDD[Vector], minthreshold: Double, maxthreshold: Double): RDD[Vector] = {
    val threshold_smoth = features.map { line =>
      val value = line.toArray
      val size = value.size
      var i = 0
      while (i < size) {
        value(i) = if (value(i) < (minthreshold)) minthreshold else if (value(i) > maxthreshold) maxthreshold else value(i)
        i += 1
      }
      Vectors.dense(value)
    }
    threshold_smoth
  }

  /*
    *特征异常平滑方法—百分位平滑
   */
  def percent_smoth(sc: SparkContext, feature: RDD[Vector], minp: Double, maxp: Double): RDD[Vector] = {
    val valued = feature.map(_.toArray).collect().flatten
    val arr_value = valued.sorted
    val num = valued.size
    val maxpre = arr_value((num * maxp).toInt)
    val minpre = arr_value((num * minp).toInt)
    val value = valued.map { line =>
      if (line > maxpre) maxpre else if (line < minpre) minpre else line
    }
    val rd = sc.parallelize(value)
    rd.map { line => Vectors.dense(line) }

  }

  /*
          *特征异常平滑方法统一API
         */
  def smoth(sc: SparkContext, featuresed: RDD[Vector], pram1: Double, pram2: Double, functionName: String): RDD[Vector] = {
    functionName match {
      case "Z-Score" => z_score_smooth(featuresed, pram1)
      case "threshold" => threshold_smooth(featuresed, pram1, pram2)
      case "percent" => percent_smoth(sc, featuresed, pram1, pram2)
    }


  }

}
