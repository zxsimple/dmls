package com.zxsimple.dmls.operator.datasource

import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.common.util.HiveUDF.HiveUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by Administrator on 2016/10/27
  */
class SinkOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame) {
    val data: DataFrame = inputPortOneData
    //获取元数据
    //获取参数
    val properties: Properties = currentNode.getProperties
    val tableName = properties.get("tableName")
    val userId = properties.get("userId").toString
    //自定义参数
    val dataBase = "userid_" + userId
    //    val tableName = "etldata_"+ etlFlowId
    //先删除表
    val hiveUtils: HiveUtils = new HiveUtils()
    hiveUtils.execHql("drop table " + dataBase + "." + tableName);
    //新建表保存数据到表中
    data.write.mode(SaveMode.Overwrite).saveAsTable(dataBase + "." + tableName)

    currentNode.setOutPortOneData(data)
  }


}
