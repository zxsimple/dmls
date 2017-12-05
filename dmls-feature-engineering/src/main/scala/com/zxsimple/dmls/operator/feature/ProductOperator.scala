package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class ProductOperator extends OperatorTemplate {
  /**
    * 验证参数
    *
    * @param properties
    */
  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    val colNamesList = data.columns
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val instanceId = properties.get("instanceId")
    val selectedCol_1 = properties.get("selectedCol_1")
    val selectedCol_2 = properties.get("selectedCol_2")
    val selectedCol_1List = selectedCol_1.toString.split(",").toList
    val selectedCol_2List = selectedCol_2.toString.split(",").toList
    //自定义数据
    val otherColsList = colNamesList.diff(selectedCol_1List).diff(selectedCol_2List)
    val tableName = "elementwiseproduct"
    data.registerTempTable(tableName)


    //创建DataFrame
    val otherSql = "select `" + otherColsList.mkString("`,`") + "` from " + tableName
    val otherDF = hc.sql(otherSql)

    val featureDF: DataFrame = data.selectExpr("`" + selectedCol_1 + "` * `" + selectedCol_2 + "` as " + instanceId + "_1")
    //合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)

    println("元素智能乘积结束")
    /*
      *将元素智能乘积的数据保存中间结果
      */
    currentNode.setOutPortOneData(result)

  }
}
