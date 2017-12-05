package com.zxsimple.dmls.operator.statistics

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class HistogramOperator extends OperatorTemplate {
  /**
    * 验证参数
    *
    * @param properties
    */
  override def validate(properties: Properties): (Boolean, String) = {
    val selectedColLength = properties.get("selectedCol").toString.split(",").length
    if (selectedColLength != 1) {
      (false, "参数验证失败")
    }
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {

    val data = inputPortOneData
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val selectedCol = properties.get("selectedCol").toString
    val maxBins = properties.get("maxBins").toString.toInt
    //自定义数据
    val selectedDF = data.select(selectedCol).rdd.map(_.get(0).toString.toDouble)

    //特征直方分布，通过API：histogarm
    val feature_histogram = histogram(selectedDF, maxBins)

    //将数据保存中间结果
    //保存统计数据.并更新MySql状态
    val obj = new JSONObject()
    obj.put("taskName", "Histogram")
    obj.put("selectCol", selectedCol)
    obj.put("result", feature_histogram)
    val jsonStr = obj.toJSONString
    currentNode.setStatisticsData(jsonStr)
  }

  /**
    * 特征值按照分区数分开后，统计每个分区的特征数量API：histogram
    */
  def histogram(data: RDD[Double], maxBins: Int) = {
    val histogram = data.histogram(maxBins)

    //保存结果的区间
    def featureRegion(featureIndex: Int): String = {
      var region = ""
      region = histogram._1(featureIndex).formatted("%.6f").toDouble + "-" + histogram._1(featureIndex + 1).formatted("%.6f").toDouble
      region
    }

    //标签json
    var char = ""

    for (i <- 0 until maxBins) {
      char = char + "\"" + featureRegion(i) + "\"" + ":" + "\"" + histogram._2(i).toDouble + "\"" + ","
    }

    //标签json
    val labelJson = "{" + char.substring(0, char.length - 1) + "}"

    labelJson
  }
}
