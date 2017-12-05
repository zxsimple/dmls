package com.zxsimple.dmls.operator.etl

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class ColMappingOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    val colList = data.columns
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val srcColName = properties.get("srcColName").toString.trim
    val dstColName = properties.get("dstColName").toString.trim
    //自定义参数
    val tableName = "col_mapping"
    data.registerTempTable(tableName)

    if (colList.indexOf(srcColName) != -1) {
      colList(colList.indexOf(srcColName)) = srcColName + "` as `" + dstColName
    }

    val selectedSql = "select `" + colList.mkString("`,`") + "` from " + tableName

    val result = hc.sql(selectedSql)


    currentNode.setOutPortOneData(result)
  }
}
