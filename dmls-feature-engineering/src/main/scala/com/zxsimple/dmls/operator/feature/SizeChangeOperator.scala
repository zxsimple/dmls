package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class SizeChangeOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    val colNamesList = data.columns
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val selectedColsList: List[String] = properties.get("selectedCols").toString.split(",").toList
    val function = properties.get("function").toString
    //自定义数据
    val otherColsList = colNamesList.diff(selectedColsList)
    val tableName = "size_change"
    data.registerTempTable(tableName)

    val otherSql = "select `" + otherColsList.mkString("`,`") + "` from " + tableName
    val otherDF = hc.sql(otherSql)

    val selectedSql = "select `" + selectedColsList.mkString("`,`") + "` from " + tableName
    val selectedDF = hc.sql(selectedSql)

    val selectedVector = selectedDF.map(row => {
      //seq下标从1开始
      val seq = row.toSeq.toArray.map(_.toString.toDouble)
      Vectors.dense(seq)
    })


    //尺度变换算法
    val features_sizechanged: RDD[Vector] = change_Function(selectedVector, function, selectedDF, selectedColsList)
    //RDD[Vector]转换为DF
    //    val hashkeyDF =  data.select("hashkey")
    val featureDF = TaskUtils.rddVectorToDF(hc, features_sizechanged, selectedColsList)

    //两个RDD合并

    val rows = otherDF.rdd.zip(featureDF.rdd).map {
      case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)
    }

    val schema = StructType(otherDF.schema.fields ++ featureDF.schema.fields)
    val result = hc.createDataFrame(rows, schema)


    println("尺度变换算子结束")
    /*
     *将变换的数据保存中间结果
     */
    currentNode.setOutPortOneData(result)
  }

  /*
      *创建特征尺寸改变API:change_Function（features,function）
      */

  def change_Function(features: RDD[Vector], function: String, dataFrame: DataFrame, selectedColsList: List[String]): RDD[Vector] = {
    //将各列最小负值转化为其正值的两倍
    //    val matrix = new RowMatrix(features)
    //    val matrixSummary = matrix.computeColumnSummaryStatistics()
    //val min = matrixSummary.min.toArray
    val stat = TaskUtils.describe(dataFrame, selectedColsList)
    val min = stat.getMin.toArray.map(_.toString.toDouble)
    // val min = dataFrame.describe().filter("summary = 'min'").collect().map(x => x.toString()).map(x=>x.toDouble)
    val min_positive: Array[Double] = min.map { line =>
      val lined = if (line <= 0) (-2 * line) else 0.0
      lined
    }

    function match {
      case "log10" =>
        val feature_log = features.map { line =>
          val value = line.toArray
          val size = value.size
          var i = 0
          while (i < size) {
            value(i) = math.log10(value(i) + min_positive(i))
            i += 1
          }
          Vectors.dense(value)
        }
        feature_log

      case "ln(x+1)" =>
        val feature_log = features.map { line =>
          val value = line.toArray
          val size = value.size
          var i = 0
          while (i < size) {
            value(i) = math.log1p(value(i) + min_positive(i))
            i += 1
          }
          Vectors.dense(value)
        }
        feature_log

      case "ln" =>
        val feature_log = features.map { line =>
          val value = line.toArray
          val size = value.size
          var i = 0
          while (i < size) {
            value(i) = math.log(value(i) + min_positive(i))
            i += 1
          }
          Vectors.dense(value)
        }
        feature_log

      case "abs" =>
        val feature_log = features.map { line =>
          val value = line.toArray
          val size = value.size
          var i = 0
          while (i < size) {
            value(i) = math.abs(value(i) + min_positive(i))
            i += 1
          }
          Vectors.dense(value)
        }
        feature_log

      case "sqrt" =>
        val feature_log = features.map { line =>
          val value = line.toArray
          val size = value.size
          var i = 0
          while (i < size) {
            value(i) = math.sqrt(value(i) + min_positive(i))
            i += 1
          }
          Vectors.dense(value)
        }
        feature_log
    }
  }


}
