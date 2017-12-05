package com.zxsimple.dmls.operator.ml

import java.util.Properties

import com.zxsimple.dmls.common.exception.ModelException
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.common.util.hbaseUtils.HBaseBean.HBaseByDataframeUtils
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.MLOperatorUtil
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType

/**
  * Created by lenovo on 2016/10/28.
  * 冥迭代聚类
  */
class PowerIterationClusteringOperator extends OperatorTemplate {
  /**
    * 验证参数
    *
    * @param properties
    */
  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  /**
    * Abstract method to be implemented in Concrete Class to handle the specific ML logic
    *
    * @param currentNode
    */
  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    //首先进入训练
    val result = run(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame)
    currentNode.setOutPortOneData(result)

    //如果第二个参数不为空，则抛出异常
    if (inputPortTwoData != null) {
      throw new ModelException(" The model only supports inputPortOneData!")
    }
  }

  def run(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame) = {
    val sc = hc.sparkContext
    val properties = currentNode.getProperties
    val nodeVertexInstanceId = currentNode.getInstanceId
    val processId = currentNode.getProcessId

    //算法参数
    val k = properties.getProperty("k").toDouble.toInt
    val maxIterations = properties.getProperty("maxIterations").toDouble.toInt
    val initializationModel = properties.getProperty("initializationModel")


    val trainInputDataTmp = inputPortOneData.drop("hashkey")
    val dataToRdd = TaskUtils.dfToRDD(trainInputDataTmp, ",")
    val similarities = dataToRdd.map { line =>
      val parts = line.split(",")
      (parts(0).toLong, parts(1).toLong, parts(2).toDouble)
    }

    val hbaseTableName = "process_" + processId + "_" + nodeVertexInstanceId
    //模型训练
    val pic = new PowerIterationClustering().setK(k).setMaxIterations(maxIterations).setInitializationMode(initializationModel)
    val model = pic.run(similarities)

    //将模型评估结果保存到Model数据库中
    val path = MLOperatorUtil.saveSpecificMetricResult("noMetricsResult", -100000, processId, nodeVertexInstanceId, false)

    //保存模型
    MLOperatorUtil.deletePath(path)
    model.save(hc.sparkContext, path)

    //各节点聚类后的分类结果及其id再在首列加上hashkey
    val clusterResult: RDD[String] = model.assignments.map { a => a.id + "," + a.cluster }
    val colList = List("result", "clusterResult");

    //生成新的DF
    val resultDF = TaskUtils.rddStringToDF(hc, clusterResult, colList, StringType, ",")
    val result: DataFrame = TaskUtils.df1ZipDF2(hc, inputPortOneData, resultDF)

    //将结果保存到hbase中，默认表名, 列族名是feature
    HBaseByDataframeUtils.writeHBase(hc.sparkContext, result, hbaseTableName, "default", "key", "feature")

    result


  }
}
