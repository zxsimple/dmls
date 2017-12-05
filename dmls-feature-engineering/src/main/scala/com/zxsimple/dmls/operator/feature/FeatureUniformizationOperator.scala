package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class FeatureUniformizationOperator extends OperatorTemplate {
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
    val tableName = "min_max"
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
      *特征归一化算法
      */
    val stat = TaskUtils.describe(selectedDF, selectedColsList)
    val min = stat.getMin.toArray.map(_.toString.toDouble)
    val max = stat.getMax.toArray.map(_.toString.toDouble)
    //    val matrix = new RowMatrix(selectedVector)
    //    val matrixSummary = matrix.computeColumnSummaryStatistics()
    //    val max = matrixSummary.max.toArray
    //    val min = matrixSummary.min.toArray

    val min_max = selectedVector.map { line =>
      val value = line.toArray
      val size = value.size
      var i = 0
      while (i < size) {
        value(i) = if ((max(i) - min(i)) != 0.0) (value(i) - min(i)) * (1.0 / (max(i) - min(i))) else 0.0
        i += 1
      }
      Vectors.dense(value)
    }

    //RDD[Vector]转换为DF
    val featureDF = TaskUtils.rddVectorToDF(hc, min_max, selectedColsList)

    //合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)

    println("特征归一化算子结束")
    /*
      *将归一化的数据保存中间结果
      */
    currentNode.setOutPortOneData(result)

  }
}
