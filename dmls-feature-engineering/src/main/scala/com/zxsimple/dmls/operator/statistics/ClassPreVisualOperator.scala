package com.zxsimple.dmls.operator.statistics

import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.MLOperatorUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by lenovo on 2016/11/15.
  */
class ClassPreVisualOperator extends OperatorTemplate {
  /**
    * 验证参数
    *
    * @param properties
    */
  override def validate(properties: Properties): (Boolean, String) = {

    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {

    //获取参数
    val operatorName = currentNode.getOperatorName.toString
    val classificationVisualResult = MLOperatorUtil.classificationVisualResult(hc, operatorName, inputPortOneData)
    //可视化Json数据保存至nodeVertex表的StatisticsData中
    currentNode.setStatisticsData(classificationVisualResult)
  }
}


