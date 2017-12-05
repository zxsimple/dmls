package com.zxsimple.dmls.operator.etl

import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Sanders on 2016/10/25.
  */
class RepeatRemoveOperator extends OperatorTemplate {
  def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame) {
    val data: DataFrame = inputPortOneData
    //获取元数据
    val result: DataFrame = data.distinct
    println("去重算子结束")
    //保存中间结果
    currentNode.setOutPortOneData(result)
  }
}
