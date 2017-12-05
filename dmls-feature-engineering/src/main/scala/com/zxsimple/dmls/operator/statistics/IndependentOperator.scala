package com.zxsimple.dmls.operator.statistics

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class IndependentOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val labelCol = properties.get("labelCol").toString
    val selectedCols = properties.get("selectedCols").toString
    //自定义数据
    val labelColList = properties.get("labelCol").toString.split(",").toList
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList
    //创建DataFrame
    val tableName = "idependent"
    data.registerTempTable(tableName)

    val selectedSql = "select `" + labelCol + "`,`" + selectedColsList.mkString("`,`") + "` from " + tableName
    val selectedDF = hc.sql(selectedSql)

    val labeledpoint = selectedDF.map(row => {
      LabeledPoint(row.get(0).toString.toDouble,
        {
          //seq下标从1开始
          val seq = row.toSeq.drop(1).toArray.map(_.toString.toDouble)
          Vectors.dense(seq)
        }
      )
    })

    //独立性检验—各feature 与label之间的独立性，P值<0.05,表示feature与lable有相关性
    val independentResult: Array[ChiSqTestResult] = Statistics.chiSqTest(labeledpoint)

    //保存统计数据.并更新MySql状态
    var obj = new JSONObject()
    var i = 0
    for (result <- independentResult) {
      val el = new JSONObject()
      val p = result.pValue
      var status: String = null
      if (p < 0.05) {
        status = "有相关性"
      } else {
        status = "无相关性"
      }
      el.put("taskName", "Independent")
      el.put("pValue", p)
      el.put("result", status)
      el.put("labelCol", labelCol)
      obj.put(selectedColsList(i), el)
      i += 1
    }
    val jsonStr = obj.toJSONString
    currentNode.setStatisticsData(jsonStr)
  }
}
