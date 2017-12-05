package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class FeatureExtractOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    val selectedColsListLength = properties.get("selectedCols").toString.split(",").length
    val k = properties.get("k").toString.toInt
    if (k < 0 || k > selectedColsListLength) {
      (false, "参数验证失败")
    }
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {

    val data = inputPortOneData
    val colNamesList = data.columns
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val instanceId = properties.get("instanceId")
    val labelCol = properties.get("labelCol").toString
    val k = properties.get("k").toString.toInt
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList

    //验证0<k<selectedCols.size()
    if (k > selectedColsList.size || k < 0) {
      throw new IllegalArgumentException("k的值需小于所选特征列的个数且应为大于0的整数，所选特征列的个数 : " + selectedColsList.size + ", k ：" + k)
    }

    //自定义数据
    val otherColsList = colNamesList.diff(selectedColsList)
    val tableName = "feature_extract"
    data.registerTempTable(tableName)

    val selectedSql = "select `" + labelCol + "`,`" + selectedColsList.mkString("`,`") + "` from " + tableName
    val otherSql = "select `" + otherColsList.mkString("`,`") + "` from " + tableName

    //    val labelDF=data.select(labelCol)
    val selectedDF = hc.sql(selectedSql)
    val otherDF = hc.sql(otherSql)

    val labeledpoint = selectedDF.map(row => {
      LabeledPoint(row.get(0).toString.toDouble,
        {
          //seq下标从1开始
          val seq = row.toSeq.drop(1).toArray.map(_.toString.toDouble)
          Vectors.dense(seq)
        }
      )
    })

    val selector = new ChiSqSelector(k)
    val selectorModel = selector.fit(labeledpoint)
    val filterFeatures = labeledpoint.map { line => LabeledPoint(line.label, selectorModel.transform(line.features)) }
    val newFeatures = filterFeatures.map { lp =>
      lp.features
    }
    val newColListBuffer = new ArrayBuffer[String]
    for (i <- 1 to k) {
      newColListBuffer += instanceId + "_" + i
    }
    val featureDF = TaskUtils.rddVectorToDF(hc, newFeatures, newColListBuffer.toList)

    //合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)

    println("特征抽取算子结束")
    //保存中间结果
    currentNode.setOutPortOneData(result)

  }
}
