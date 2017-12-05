package com.zxsimple.dmls.operator.statistics

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class CorrelationOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val instanceId = properties.get("instanceId").toString
    val functionName = properties.get("functionName").toString
    val selectedCol_1 = properties.get("selectedCol_1").toString
    val selectedCol_2 = properties.get("selectedCol_2").toString
    //创建DataFrame
    val selectedCol_1RDDDouble = data.select(selectedCol_1).map(_.get(0).toString.toDouble)
    val selectedCol_2RDDDouble = data.select(selectedCol_2).map(_.get(0).toString.toDouble)

    /*
     *两个特征列相关系数计算——correlation
     */
    val correlation = Statistics.corr(selectedCol_1RDDDouble, selectedCol_2RDDDouble, functionName)

    //保存统计数据.并更新MySql状态
    val obj = new JSONObject()
    val status: String = checkStatus(correlation)
    obj.put("taskName", "Correlation")
    obj.put("pValue", correlation)
    obj.put("result", status)
    obj.put("selectedCol_1", selectedCol_1)
    obj.put("selectedCol_2", selectedCol_2)
    val jsonStr = obj.toJSONString
    currentNode.setStatisticsData(jsonStr)

  }

  def checkStatus(correlation: Double): String = {
    correlation match {
      case correlation if (correlation < 1.0 && correlation >= 0.8) => "极强正相关"
      case correlation if (correlation < 0.8 && correlation >= 0.6) => "强正相关"
      case correlation if (correlation < 0.6 && correlation >= 0.4) => "中等程度正相关"
      case correlation if (correlation < 0.4 && correlation >= 0.2) => "弱正相关"
      case correlation if (correlation < 0.2 && correlation >= 0.0) => "极弱正相关或无相关"
      case correlation if (correlation < 0.0 && correlation >= -0.2) => "极弱负相关或无相关"
      case correlation if (correlation < -0.2 && correlation >= -0.4) => "弱负相关"
      case correlation if (correlation < -0.4 && correlation >= -0.6) => "中等程度负相关"
      case correlation if (correlation < -0.6 && correlation >= -0.8) => "强负相关"
      case correlation if (correlation < -0.8 && correlation > -1) => "极强负相关"
    }
  }
}
