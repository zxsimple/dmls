package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.feature.MDLPDiscretizer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class MDLPDiscretizerOperator extends OperatorTemplate {
  /**
    * 验证参数
    *
    * @param properties
    */
  override def validate(properties: Properties): (Boolean, String) = {
    val nBins = properties.get("nBins").toString.toInt
    val maxByPar = properties.get("maxByPar").toString.toInt
    if (nBins <= 0 || maxByPar <= 0) {
      (false, "参数验证失败")
    }
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    val colNamesList = data.columns
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val labelCol = properties.get("labelCol")
    val nBins = properties.get("nBins").toString.toInt
    val maxByPar = properties.get("maxByPar").toString.toInt
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList
    //自定义数据
    val otherColsList = colNamesList.diff(selectedColsList)
    val tableName = "MDLPDiscretizer"
    data.registerTempTable(tableName)

    //创建DataFrame

    val otherSql = "select `" + otherColsList.mkString("`,`") + "` from " + tableName
    val otherDF = hc.sql(otherSql)

    val selectedSql = "select `" + labelCol + "`,`" + selectedColsList.mkString("`,`") + "` from " + tableName
    val selectedDF = hc.sql(selectedSql)

    val labeledpoint = selectedDF.map(row => {
      LabeledPoint(row.get(0).toString.toDouble,
        {
          //seq下标从1开始
          val seq = row.toSeq.drop(1).toArray.map(_.toString.toDouble)
          Vectors.dense(seq)
        }
      )
    }).persist(StorageLevel.MEMORY_AND_DISK_SER).coalesce(1000, true)


    //    labeledpoint.foreach(println)
    /*
     *创建特征离散模型discretizer，并对特征进行基于熵信息的离散处理
     */
    val categoricalFeat: Option[Seq[Int]] = None
    val discretizer = MDLPDiscretizer.train(labeledpoint, categoricalFeat, nBins, maxByPar)
    val discrete = labeledpoint.map(i => LabeledPoint(i.label, discretizer.transform(i.features)))
    val newFeatures: RDD[Vector] = discrete.map { lp =>
      lp.features
    }

    val featureDF = TaskUtils.rddVectorToDF(hc, newFeatures, selectedColsList.toList)

    //合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)

    println("基于信息熵的特征离散算子结束")


    //保存中间结果
    currentNode.setOutPortOneData(result)
  }
}
