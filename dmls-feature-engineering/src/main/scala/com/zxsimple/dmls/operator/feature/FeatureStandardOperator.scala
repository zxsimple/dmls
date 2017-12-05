package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class FeatureStandardOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    val colNamesList = data.columns
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList
    val withmean = properties.get("mean").toString.toBoolean
    val withstd = properties.get("std").toString.toBoolean
    //自定义数据
    val otherColsList = colNamesList.diff(selectedColsList)
    val tableName = "standard"
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
      *特征标准化算法
      */
    val standardScalar = new StandardScaler(withMean = withmean, withStd = withstd).fit(selectedVector)
    val features_standardScalar: RDD[Vector] = standardScalar.transform(selectedVector)


    //RDD[Vector]转换为DF
    val featureDF = TaskUtils.rddVectorToDF(hc, features_standardScalar, selectedColsList)

    //合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)

    println("特征标准化算子结束")
    /*
      *将标准化的数据保存中间结果
      */
    currentNode.setOutPortOneData(result)
  }
}
