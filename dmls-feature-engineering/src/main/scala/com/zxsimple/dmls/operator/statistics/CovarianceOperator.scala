package com.zxsimple.dmls.operator.statistics

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class CovarianceOperator extends OperatorTemplate{

  override def validate(properties: Properties): (Boolean, String) ={
    (true, null)
  }
  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val instanceId = properties.get("instanceId")
    val selectedCol_1 = properties.get("selectedCol_1").toString
    val selectedCol_2 = properties.get("selectedCol_2").toString
    //计算两列特征的协方差“cov”
    val cov = data.stat.cov(selectedCol_1,selectedCol_2)
    //保存统计数据.并更新MySql状态
    val obj = new JSONObject()
    val status: String = checkStatus(cov)
    obj.put("taskName", "Covariance")
    obj.put("pValue", cov)
    obj.put("result", status)
    obj.put("selectedCol_1", selectedCol_1)
    obj.put("selectedCol_2", selectedCol_2)
    val jsonStr = obj.toJSONString
    currentNode.setStatisticsData(jsonStr)

  }

  def checkStatus(cov: Double): String = {
    cov match {
      case cov if (cov == 0.0) => "互相独立"
      case cov if (cov > 0.0) => "正相关"
      case cov if (cov < 0.0) => "负相关"
    }
  }

}
