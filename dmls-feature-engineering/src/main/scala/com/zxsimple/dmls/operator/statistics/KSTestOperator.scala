package com.zxsimple.dmls.operator.statistics

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class KSTestOperator extends OperatorTemplate{
  override def validate(properties: Properties): (Boolean, String) ={
    val selectedColLength = properties.get("selectedCol").toString.split(",").length
    if(selectedColLength != 1 ){
      (false,"参数验证失败")
    }
    (true, null)
  }
  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit ={
    val data = inputPortOneData
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val instanceId = properties.get("instanceId").toString
    val selectedCol = properties.get("selectedCol").toString
    //自定义数据
    val selectedColList = selectedCol.split(",").toList
    //创建DataFrame
    val selectedDF = data.select(selectedCol)
    val selectedColRDDouble = selectedDF.map(_.get(0).toString.toDouble)


    val stats = TaskUtils.describe(data,selectedColList)
    val mean = stats.getMean.get(0)
    val stddev = stats.getStddev.get(0)
    /*
     *对特征列进行正态分布检验
     */
    val K_STest = Statistics.kolmogorovSmirnovTest(selectedColRDDouble, "norm", mean, stddev)

    val pValue = K_STest.pValue

    //保存统计数据.并更新MySql状态
    val obj = new JSONObject()
    val status: String = checkStatus(pValue)
    obj.put("taskName", "KSTest")
    obj.put("pValue", pValue)
    obj.put("result", status)
    obj.put("selectedCol", selectedCol)
    val jsonStr = obj.toJSONString
    currentNode.setStatisticsData(jsonStr)
  }

  def checkStatus(pValue: Double): String = {
    pValue match {
      case pValue if (pValue >= 0.05) => "满足正态分布"
      case pValue if (pValue < 0.05) => "不满足正态分布"
    }
  }
}
