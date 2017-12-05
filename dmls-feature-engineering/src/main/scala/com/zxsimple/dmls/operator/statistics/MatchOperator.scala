package com.zxsimple.dmls.operator.statistics

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class MatchOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val colNumber = properties.get("colNumber").toString.toInt
    val observedCol = properties.get("observedCol").toString
    val expectedCol = properties.get("expectedCol").toString
    //自定义数据

    val matchResult: ChiSqTestResult =
    //适配度检验——若不选expected特征，则默认为检验观察特征列是否为均匀分布，P>0.05表示，特征值呈均匀分布
      if (colNumber == 1) {
        val observedColRDD: RDD[String] = data.select(observedCol).map(_.get(0).toString)
        val observedColVector: Vector = rdd_StringToVector(observedColRDD)
        Statistics.chiSqTest(observedColVector)
      }
      //适配度检验——检验两个特征分布情况是否一致，P>0.05表示，两特征分布情况一致
      else if (colNumber == 2) {
        //在df中找出被选择的观察列，用于适配度检验
        val observedColRDD = data.select(observedCol).map(_.get(0).toString)
        val observedColVector = rdd_StringToVector(observedColRDD)
        //在df中找出被选择的参照列，用于适配度检验
        val expectedColRDD = data.select(expectedCol).map(_.get(0).toString)
        val expectedColVector = rdd_StringToVector(expectedColRDD)
        Statistics.chiSqTest(observedColVector, expectedColVector)
      }
      else null

    //保存统计数据.并更新MySql状态
    val obj = new JSONObject()
    val pValue = matchResult.pValue
    val status: String = checkStatus(pValue, colNumber)
    obj.put("taskName", "Match")
    obj.put("pValue", pValue)
    obj.put("result", status)
    obj.put("observedCol", observedCol)
    obj.put("expectedCol", expectedCol)

    val jsonStr = obj.toJSONString
    currentNode.setStatisticsData(jsonStr)
  }

  def rdd_StringToVector(rdd: RDD[String]): Vector = {
    val rddtovector = rdd.map(_.toDouble).collect()
    Vectors.dense(rddtovector)
  }

  def checkStatus(pValue: Double, colNumber: Int): String = {
    if (colNumber == 1) {
      if (pValue < 0.05) {
        "特征值呈非均匀分布"
      } else {
        "特征值呈均匀分布"
      }
    } else if (colNumber == 2) {
      if (pValue < 0.05) {
        "两特征分布情况不一致"
      } else {
        "两特征分布情况一致"
      }
    } else {
      null
    }
  }
}
