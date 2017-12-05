package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class PCAOperator extends OperatorTemplate {
  /**
    * 验证参数
    *
    * @param properties
    */
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
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList
    val k = properties.get("k").toString.toInt

    //验证0<k<selectedCols.size()
    //    if( k > selectedColsList.size || k<0 ) {
    //      throw new IllegalArgumentException("k的值需小于所选特征列的个数且应为大于0的整数，所选特征列的个数 : " + selectedColsList.size + ", k ：" + k )}

    //自定义数据
    val otherColsList = colNamesList.diff(selectedColsList)
    val tableName = "pca"
    data.registerTempTable(tableName)

    //创建DataFrame
    val otherSql = "select `" + otherColsList.mkString("`,`") + "` from " + tableName
    val otherDF = hc.sql(otherSql)

    val selectedSql = "select `" + selectedColsList.mkString("`,`") + "` from " + tableName
    val selectedDF = hc.sql(selectedSql)

    val selectedVector = selectedDF.map(row => {
      //seq下标从1开始
      val seq = row.toSeq.toArray.map(_.toString.toDouble)
      Vectors.dense(seq)
    })

    //创建降维模型并对所选特征降维至k维特征
    val pca = new PCA(k).fit(selectedVector)
    val features_pca: RDD[Vector] = pca.transform(selectedVector)

    //RDD[Vector]转换为DF
    val newColListBuffer = new ArrayBuffer[String]
    for (i <- 1 to k) {
      newColListBuffer += instanceId + "_" + i
    }

    val featureDF = TaskUtils.rddVectorToDF(hc, features_pca, newColListBuffer.toList)

    //合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)
    println("主成分分析算子结束")
    currentNode.setOutPortOneData(result)
  }
}
