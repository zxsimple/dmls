package com.zxsimple.dmls.operator.statistics

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer


/**
  * Created by zxsimple on 2016/10/26.
  */
class TableStatisticsOperator extends OperatorTemplate {

  def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame) {
    //获取元数据
    val data: DataFrame = inputPortOneData
    val colNamesList: Array[String] = data.columns
    val colListWithoutHashKey = colNamesList.diff(Array("hashkey"))
    val stats: DataFrame = data.describe()
    val statsRDD = stats.toJSON
    val sum = data.count
    data.show
    val daty = data.dtypes

    val jsonArr = new ArrayBuffer[JSONObject]()
    val colList = statsRDD.collect()
    for (co <- colList) {
      jsonArr += JSON.parseObject(co)
    }

    val obj = new JSONObject()
    for (col <- colListWithoutHashKey) {
      val el = new JSONObject()
      for (arr <- jsonArr) {
        var key = arr.getString("summary")
        key = key match {
          case "count" => "计数"
          case "mean" => "平均值"
          case "stddev" => "标准差"
          case "min" => "最小值"
          case "max" => "最大值"
        }

        val value = arr.getString(col)
        if (value != null) {
          if (key.equals("计数")) {
            val missnum = sum - value.toInt
            el.put("缺失值", missnum.toString)
          }
          el.put(key, value)
        } else {
          if (key.equals("计数")) {
            el.put("缺失值", "-")
          }
          el.put(key, "-")
        }
      }
      obj.put(col, el)
    }
    val jsonStr = obj.toJSONString
    //保存中间结果
    currentNode.setStatisticsData(jsonStr)

  }

}
