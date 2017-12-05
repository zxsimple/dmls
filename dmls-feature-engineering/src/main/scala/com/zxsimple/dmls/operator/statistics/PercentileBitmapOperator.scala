package com.zxsimple.dmls.operator.statistics

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class PercentileBitmapOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val pbSelectCol = properties.get("pbSelectCol").toString
    //自定义数据
    val sortedcols = data.select(pbSelectCol).sort(pbSelectCol).map(_.get(0).toString.toDouble).collect()
    val pbcols =
      if (sortedcols.length > 100) {
        val constant100 = hc.sparkContext.parallelize(1 to 100)
        constant100.map { number => sortedcols((number * sortedcols.length / 100).toInt - 1) }.collect()
      }
      else {
        sortedcols
      }
    //保存统计数据.并更新MySql状态
    val obj = new JSONObject()
    obj.put("taskName", "PercentileBitmap")
    obj.put("selectCol", pbSelectCol)
    obj.put("result", pbcols)
    val jsonStr = obj.toJSONString
    currentNode.setStatisticsData(jsonStr)

  }
}
