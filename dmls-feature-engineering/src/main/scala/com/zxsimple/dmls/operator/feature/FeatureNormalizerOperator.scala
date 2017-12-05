package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class FeatureNormalizerOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {

    val data = inputPortOneData
    val colNamesList = data.columns
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList
    //自定义数据
    val otherColsList = colNamesList.diff(selectedColsList)
    val tableName = "normalizer"

    data.registerTempTable(tableName)

    val selectedSql = "select `" + selectedColsList.mkString("`,`") + "` from " + tableName
    val otherSql = "select `" + otherColsList.mkString("`,`") + "` from " + tableName

    val selectedDF = hc.sql(selectedSql)
    val otherDF = hc.sql(otherSql)

    val selectedVector = selectedDF.map(row => {
      //seq下标从1开始
      val seq = row.toSeq.toArray.map(_.toString.toDouble)
      Vectors.dense(seq)
    })

    /*
      *正则化算法
      */
    val p = if (properties.get("p").equals("infinity")) {
      Double.PositiveInfinity
    } else {
      properties.get("p").toString.toDouble
    }
    val normalizer = new Normalizer(p)
    val features_normalizer: RDD[Vector] = normalizer.transform(selectedVector)

    val featureDF = TaskUtils.rddVectorToDF(hc, features_normalizer, selectedColsList)

    //合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)

    println("特征正则化算子结束")
    //更新元数据
    currentNode.setOutPortOneData(result)

  }
}
