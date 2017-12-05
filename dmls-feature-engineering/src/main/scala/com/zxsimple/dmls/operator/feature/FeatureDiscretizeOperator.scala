package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext


/**
  * Created by zxsimple on 2016/10/26.
  */
class FeatureDiscretizeOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    val colNamesList = data.columns
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList
    val n = properties.get("n").toString.toInt
    val functionName = properties.get("functionName").toString
    //自定义数据
    val tableName = "features_discretize"
    data.registerTempTable(tableName)
    val otherColsList = colNamesList.diff(selectedColsList)

    val selectedSql = "select `" + selectedColsList.mkString("`,`") + "` from " + tableName
    val otherSql = "select `" + otherColsList.mkString("`,`") + "` from " + tableName

    val selectedDF = hc.sql(selectedSql)
    val otherDF = hc.sql(otherSql)


    val selectedVector = selectedDF.map(row => {
      //seq下标从1开始
      val seq = row.toSeq.toArray.map(_.toString.toDouble)
      Vectors.dense(seq)
    })
    //通过自定义的discretize函数对特征进行离散
    val feature_discretiz: RDD[Vector] = discretize(hc, selectedVector, n, functionName, selectedDF, selectedColsList)

    val featureDF = TaskUtils.rddVectorToDF(hc, feature_discretiz, selectedColsList)

    //合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)


    println("特征标准化算子结束，输出结果为：")
    currentNode.setOutPortOneData(result)
  }

  /*
    *特征离散——等频离散
   */
  def frequency_discretize(hc: HiveContext, features: RDD[Vector], n: Int): RDD[Vector] = {
    val distanced = features.map(_.toArray).collect().flatten
    val num = distanced.size
    val data_index = distanced.zipWithIndex
    val m_sorted = data_index.sorted

    val sorted_data = m_sorted.map { case (data, index) => data }
    val sorted_index = m_sorted.map { case (data, index) => index }
    val sorted_data_index = sorted_data.zipWithIndex.map {
      case (datas, index) =>
        val d = num / n.toDouble
        val data = index / d
        data.toInt
    }
    val rdd_features = hc.sparkContext.parallelize(sorted_index.zip(sorted_data_index).sorted.map { case (data, index) => index })
    rdd_features.map { line => Vectors.dense(line.toDouble) }
  }

  /*
    *特征离散——等距离散
   */
  def distance_discretize(features: RDD[Vector], n: Int, dataFrame: DataFrame, selectedColsList: List[String]): RDD[Vector] = {
    val stat = TaskUtils.describe(dataFrame, selectedColsList)
    val min = stat.getMin.toArray.map(_.toString.toDouble)
    val max = stat.getMax.toArray.map(_.toString.toDouble)
    val distanced = features.map { line =>
      val value = line.toArray
      val size = value.size
      var i = 0
      while (i < size) {
        value(i) = ((value(i) - min(i)) * ((n - 1) / (max(i) - min(i)))).toInt
        i += 1
      }
      Vectors.dense(value)
    }
    distanced
  }

  /*
     *特征离散函数统一端口
   */
  def discretize(hc: HiveContext, features: RDD[Vector], n: Int, function: String, dataFrame: DataFrame, selectedColsList: List[String]): RDD[Vector] = {
    function match {
      case "distance" => distance_discretize(features, n, dataFrame, selectedColsList)
      case "frequency" => frequency_discretize(hc, features, n)
    }
  }
}
