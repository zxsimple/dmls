package com.zxsimple.dmls.operator.datasource

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * @author zxsimple
  *
  *         Read data from mysql
  */
class MySQLSourceOperator extends OperatorTemplate {

  /**
    * 验证参数
    *
    * @param properties
    */
  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  /**
    * 读取MySQL数据源数据
    *
    * @param currentNode
    *
    */
  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {

    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val serverHost = properties.get("serverHost").toString
    val serverPort = properties.get("serverPort").toString
    val database = properties.get("database").toString
    val table = properties.get("table").toString
    val partitionColumn = properties.get("partitionColumn").toString
    val user = properties.get("user").toString
    val password = properties.get("password").toString

    val url = "jdbc:mysql://" + serverHost + ":" + serverPort + "/" + database
    val props = new Properties()
    props.put("user", user)
    props.put("password", password)

    val result = hc.read.jdbc(url, table, partitionColumn, Long.MinValue, Long.MaxValue, 10, props)

    currentNode.setOutPortOneData(result)
  }
}
