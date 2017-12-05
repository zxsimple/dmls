package com.zxsimple.dmls.operator.etl

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class FilterOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    //获取参数
    val selectedCol = properties.get("selectedCol").toString
    val condition = properties.get("condition").toString
    val method = properties.get("method").toString
    val methodValue = properties.get("methodValue").toString
    //自定义数据
    val tableName = "filter"
    data.registerTempTable(tableName)

    var threshold: String = ""
    if (method.equals("Null")) {
      threshold = "null"
    } else if (method.equals("CustomDefine")) {
      threshold = methodValue
    } else {
      val stats = TaskUtils.describe(data, List(methodValue))
      if (method.equals("Avg")) {
        threshold = stats.getMean.get(0).toString
      } else if (method.equals("Max")) {
        threshold = stats.getMax.get(0).toString
      } else if (method.equals("Min")) {
        threshold = stats.getMin.get(0).toString
      }

    }

    var conditionid: String = ""
    var selectedColid: String = ""
    var thresholdid: String = ""

    selectedColid = "`" + selectedCol + "`"
    conditionid = condition
    thresholdid = threshold
    // 字符串判断
    try {
      java.lang.Double.parseDouble(threshold)
      thresholdid = threshold
    } catch {
      case ex: Exception =>
        thresholdid = "'" + threshold + "'"
    }
    // 修改非空BUG
    if (condition.equals("!=")) {
      if (threshold.equals("null")) {
        selectedColid = "length(trim(" + "`" + selectedCol + "`" + "))"
        conditionid = " >=1 "
        thresholdid = "and " + "`" + selectedCol + "`" + " is not NULL and " + "`" + selectedCol + "`" + " not in ('null','NULL')"
      }
    }

    val sql = "select * from " + tableName + " where " + selectedColid + conditionid + thresholdid
    val result = hc.sql(sql)
    println("行过滤算子结束")

    currentNode.setOutPortOneData(result)
  }
}
