package com.zxsimple.dmls.operator.etl

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/10/27 0027.
  * * 特征选择
  * 界面输入参数
  * labelCol ：标签列
  * features : 特征列
  */
class LabelSelectedOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val labelCol = properties.get("labelCol").toString
    val featuresList = properties.get("features").toString.split(",").toList

    //自定义数据
    val tableName = "select_feature"
    data.registerTempTable(tableName)

    val buffer = new ArrayBuffer[String]()
    buffer += "hashkey"
    buffer += labelCol
    buffer ++= featuresList
    val selectedColsList: List[String] = buffer.toList

    val selectedSql = "select `" + selectedColsList.mkString("`,`") + "` from " + tableName
    //在df中找出被选择的列
    val result = hc.sql(selectedSql)
    println("选择标签、特征列算子结束")
    currentNode.setOutPortOneData(result)
  }
}
