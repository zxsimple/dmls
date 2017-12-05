package com.zxsimple.dmls.operator.feature

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.TaskUtils
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class SVDOperator extends OperatorTemplate {
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
    val instanceId = properties.get("instanceId").toString
    val k = properties.get("k").toString.toInt
    val selectedColsList = properties.get("selectedCols").toString.split(",").toList

    //    //验证0<k<selectedCols.size()
    //    if( k > selectedColsList.size || k<0 ) {
    //      throw new IllegalArgumentException("k的值需小于所选特征列的个数且应为大于0的整数，所选特征列的个数 : " + selectedColsList.size + ", k ：" + k )}

    //自定义数据
    val otherColsList = colNamesList.diff(selectedColsList)
    val tableName = "svd"
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

    val rowmatrix = new RowMatrix(selectedVector)
    val svd = rowmatrix.computeSVD(k, true)
    val u = svd.U
    val s = svd.s.toArray
    val size = s.size
    val s_arr = Array.ofDim[Double](size, size)
    for (i <- 0 until size) {
      s_arr(i)(i) = s(i)
    }
    val s_arrd = s_arr.flatten
    val s_matrix = Matrices.dense(size, size, s_arrd)
    val final_feature = u.multiply(s_matrix)
    val rddVertor = final_feature.rows

    //RDD[Vector]转换为DF
    val newColListBuffer = new ArrayBuffer[String]
    for (i <- 1 to k) {
      newColListBuffer += instanceId + "_" + i
    }
    val featureDF = TaskUtils.rddVectorToDF(hc, rddVertor, newColListBuffer.toList)

    //两个DF合并
    val result = TaskUtils.df1ZipDF2(hc, otherDF, featureDF)

    println("SVD算子结束")
    /*
      *将降维的数据保存中间结果
      */
    currentNode.setOutPortOneData(result)
  }
}
