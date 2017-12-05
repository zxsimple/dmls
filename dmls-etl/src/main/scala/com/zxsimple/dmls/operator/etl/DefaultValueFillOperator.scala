package com.zxsimple.dmls.operator.etl

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.model.{OriginalType, ReplacedType}
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class DefaultValueFillOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {

    val data = inputPortOneData
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList
    val originalType = OriginalType.valueOf(properties.get("originalType").toString)
    val originalCustomValue = properties.get("originalCustomValue").toString
    val replacedType = ReplacedType.valueOf(properties.get("replacedType").toString)
    val replacedCustomValue = properties.get("replacedCustomValue").toString
    //自定义参数
    val colNamesList = data.columns
    val colsAndType = new util.HashMap[String, String]()
    data.dtypes.map(line => {
      colsAndType.put(line._1, line._2)
    })


    //要保证顺序
    val beanList = mutable.LinkedHashMap[String, String]()

    for (colName <- colNamesList) {
      if (selectedColsList.indexOf(colName) != -1) {
        beanList.put(colName, "change")
      } else {
        beanList.put(colName, "hold")
      }
    }

    val stat = TaskUtils.describe(data, selectedColsList)
    val max = stat.getMax
    val min = stat.getMin
    val mean = stat.getMean

    val sqlList = ArrayBuffer[String]()

    var changeIndex = 0
    beanList.keys.map { col_name => {
      val value = beanList(col_name)
      val colType = colsAndType.get(col_name)

      if (value.equals("change")) {
        val maxValue = max.get(changeIndex)
        val minValue = min.get(changeIndex)
        val meanValue = mean.get(changeIndex)
        if (originalType.equals(OriginalType.EmptyOrNull)) {
          if (replacedType.equals(ReplacedType.CustomDefine)) {
            try {
              java.lang.Double.parseDouble(replacedCustomValue);
              sqlList += "CASE WHEN `" + col_name + "`='' or `" + col_name + "`='\"\"' or `" + col_name + "`='\\\"\\\"' or `" + col_name + "`=\"\\\'\\\'\" or `" + col_name + "`='NULL' or `" + col_name + "`='null' or `" + col_name + "` is null THEN " + replacedCustomValue + " else `" + col_name + "` end as `" + col_name + "`"
            } catch {
              case ex: Exception =>
                sqlList += "CASE WHEN `" + col_name + "`='' or `" + col_name + "`='\"\"' or `" + col_name + "`='\\\"\\\"' or `" + col_name + "`=\"\\\'\\\'\" or `" + col_name + "`='NULL' or `" + col_name + "`='null' or `" + col_name + "` is null THEN '" + replacedCustomValue + "' else `" + col_name + "` end as `" + col_name + "`"

            }
          } else if (replacedType.equals(ReplacedType.Max)) {
            if (colType.equals("IntegerType")) {
              sqlList += "CASE WHEN `" + col_name + "`='' or `" + col_name + "`='\"\"' or `" + col_name + "`='\\\"\\\"' or `" + col_name + "`=\"\\\'\\\'\" or `" + col_name + "`='NULL' or `" + col_name + "`='null' or `" + col_name + "` is null THEN " + maxValue.toInt + " else `" + col_name + "` end as `" + col_name + "`"
            } else {
              sqlList += "CASE WHEN `" + col_name + "`='' or `" + col_name + "`='\"\"' or `" + col_name + "`='\\\"\\\"' or `" + col_name + "`=\"\\\'\\\'\" or `" + col_name + "`='NULL' or `" + col_name + "`='null' or `" + col_name + "` is null THEN " + maxValue + " else `" + col_name + "` end as `" + col_name + "`"
            }
          } else if (replacedType.equals(ReplacedType.Min)) {
            if (colType.equals("IntegerType")) {
              sqlList += "CASE WHEN `" + col_name + "`='' or `" + col_name + "`='\"\"' or `" + col_name + "`='\\\"\\\"' or `" + col_name + "`=\"\\\'\\\'\" or `" + col_name + "`='NULL' or `" + col_name + "`='null' or `" + col_name + "` is null THEN " + minValue.toInt + " else `" + col_name + "` end as `" + col_name + "`"
            } else {
              sqlList += "CASE WHEN `" + col_name + "`='' or `" + col_name + "`='\"\"' or `" + col_name + "`='\\\"\\\"' or `" + col_name + "`=\"\\\'\\\'\" or `" + col_name + "`='NULL' or `" + col_name + "`='null' or `" + col_name + "` is null THEN " + minValue + " else `" + col_name + "` end as `" + col_name + "`"
            }
          } else if (replacedType.equals(ReplacedType.Avg)) {
            if (colType.equals("IntegerType")) {
              sqlList += "CASE WHEN `" + col_name + "`='' or `" + col_name + "`='\"\"' or `" + col_name + "`='\\\"\\\"' or `" + col_name + "`=\"\\\'\\\'\" or `" + col_name + "`='NULL' or `" + col_name + "`='null' or `" + col_name + "` is null THEN " + meanValue.toInt + " else `" + col_name + "` end as `" + col_name + "`"
            } else {
              sqlList += "CASE WHEN `" + col_name + "`='' or `" + col_name + "`='\"\"' or `" + col_name + "`='\\\"\\\"' or `" + col_name + "`=\"\\\'\\\'\" or `" + col_name + "`='NULL' or `" + col_name + "`='null' or `" + col_name + "` is null THEN " + meanValue + " else `" + col_name + "` end as `" + col_name + "`"
            }
          }

          //源数据类型为CustomDefine
        } else if (originalType.equals(OriginalType.CustomDefine)) {
          if (replacedType.equals(ReplacedType.CustomDefine)) {
            try {
              java.lang.Double.parseDouble(replacedCustomValue);
              sqlList += "CASE WHEN `" + col_name + "`='" + originalCustomValue + "' THEN " + replacedCustomValue + " else `" + col_name + "` end as `" + col_name + "`"
            } catch {
              case ex: Exception =>
                sqlList += "CASE WHEN `" + col_name + "`='" + originalCustomValue + "' THEN '" + replacedCustomValue + "' else `" + col_name + "` end as `" + col_name + "`"

            }
          } else if (replacedType.equals(ReplacedType.Max)) {
            if (colType.equals("IntegerType")) {
              sqlList += "CASE WHEN `" + col_name + "`='" + originalCustomValue + "' THEN " + maxValue.toInt + " else `" + col_name + "` end as `" + col_name + "`"
            } else {
              sqlList += "CASE WHEN `" + col_name + "`='" + originalCustomValue + "' THEN " + maxValue + " else `" + col_name + "` end as `" + col_name + "`"

            }
          } else if (replacedType.equals(ReplacedType.Min)) {
            if (colType.equals("IntegerType")) {
              sqlList += "CASE WHEN `" + col_name + "`='" + originalCustomValue + "' THEN " + minValue.toInt + " else `" + col_name + "` end as `" + col_name + "`"
            } else {
              sqlList += "CASE WHEN `" + col_name + "`='" + originalCustomValue + "' THEN " + minValue + " else `" + col_name + "` end as `" + col_name + "`"
            }
          } else if (replacedType.equals(ReplacedType.Avg)) {
            if (colType.equals("IntegerType")) {
              sqlList += "CASE WHEN `" + col_name + "`='" + originalCustomValue + "' THEN " + meanValue.toInt + " else `" + col_name + "` end as `" + col_name + "`"
            } else {
              sqlList += "CASE WHEN `" + col_name + "`='" + originalCustomValue + "' THEN " + meanValue + " else `" + col_name + "` end as `" + col_name + "`"
            }
          }
        }

        changeIndex += 1
      } else {
        sqlList += "`" + col_name + "`"
      }
    }
    }


    data.registerTempTable("def_fill")
    val caseWhenString = sqlList.mkString(",")
    val sqlString = "select " + caseWhenString + " from def_fill"
    println(sqlString)
    val result = hc.sql(sqlString)
    println("值填充算子结束")
    currentNode.setOutPortOneData(result)

  }
}
