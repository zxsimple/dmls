package com.zxsimple.dmls.operator.etl

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/10/27 0027.
  * 分层采样
  *
  * 界面输入参数
  * selectedCol 采样的列 只有一个
  * layer 层数 layer>0
  * k 每层采样比   (0,1]
  */
class StratifiedSamplingOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val layer = properties.get("layer").toString.toInt
    val k = properties.get("k").toString.toDouble
    val selectedCol = properties.get("selectedCol").toString
    //自定义数据
    val selectedColList = selectedCol.split(",").toList
    val tableName = "size_change"
    data.registerTempTable(tableName)

    //创建DataFrame
    val featureRDD = data.select(selectedCol).rdd.map(_.get(0).toString)
    val stats = TaskUtils.describe(data, selectedColList)
    val max = stats.getMax.get(0)
    val min = stats.getMin.get(0)

    //每层梯度
    val width = if (max - min != 0) {
      (max - min) / layer
    } else {
      0.0
    }

    val widthList = new ArrayBuffer[Double]
    for (i <- 1 to layer) {
      val value = min + (width * i)
      widthList += value
    }

    val resRDD: RDD[Int] = featureRDD.map(str => {
      val n = Math.ceil((str.toDouble - min) / width).toInt
      if (n == 0) {
        1
      } else if (n > layer) {
        layer
      } else {
        n
      }
    })

    val mapRDD: RDD[(Int, Row)] = resRDD.zip(data.rdd)
    // mapRDD.foreach(println)

    //设定抽样格式
    val buffer = new ArrayBuffer[(Int, Double)]()
    for (widthListIndex <- 1 to widthList.size) {
      val elem = (widthListIndex, k)
      buffer += elem
    }
    val fractions: Map[Int, Double] = buffer.toList.toMap
    //计算抽样样本
    val approxSample: RDD[(Int, Row)] = mapRDD.sampleByKey(withReplacement = false, fractions, 0)
    val resultRDD: RDD[Row] = approxSample.map(line => line._2)
    val schema = data.schema

    val result = hc.createDataFrame(resultRDD, schema)
    currentNode.setOutPortOneData(result)

  }
}
