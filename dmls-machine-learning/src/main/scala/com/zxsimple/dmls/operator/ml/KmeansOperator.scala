package com.zxsimple.dmls.operator.ml

import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.util.hbase.HBaseByDataframeUtils
import com.zxsimple.dmls.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.MLOperatorUtil
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * Created by lenovo on 2016/10/28.
  * K_均值聚类模型
  */
class KmeansOperator extends OperatorTemplate {
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
    *
    */
  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val processId = currentNode.getProcessId
    val nodeVertexInstanceId = currentNode.getInstanceId
    val properties: Properties = currentNode.getProperties

    //首先进入训练
    val model = train(hc, processId, nodeVertexInstanceId, properties, inputPortOneData)

    //如果第二个参数不为空，则进入应用
    if (inputPortTwoData != null) {
      val result = predict(hc, processId, nodeVertexInstanceId: String, model, properties, inputPortTwoData)
      currentNode.setOutPortOneData(result)
    }

  }

  /**
    * 训练模型
    *
    * @return
    */
  def train(hc: HiveContext, processId: Long, nodeVertexInstanceId: String, properties: Properties, trainInputData: DataFrame): KMeansModel = {
    var model: KMeansModel = null

    //算法参数
    val numClusters = properties.getProperty("numClusters").toDouble.toInt
    val numIterations = properties.getProperty("numIterations").toDouble.toInt
    val runs = properties.getProperty("runs").toDouble.toInt
    val initializationMode = properties.getProperty("initializationMode")


    //组成训练数据
    val trainInputDataTmp = trainInputData.drop("hashkey")
    val dataToRdd = TaskUtils.dfToRDD(trainInputDataTmp, ",")
    val dateToVector = TaskUtils.rddStringToVector(dataToRdd, ",").cache()


    //模型训练
    model = KMeans.train(dateToVector, numClusters, numIterations, runs, initializationMode)

    //模型评估
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = model.computeCost(dateToVector)

    //将模型评估结果保存到Model数据库中
    val path = MLOperatorUtil.saveSpecificMetricResult("WSSSE", WSSSE, processId, nodeVertexInstanceId, true)

    //保存模型
    MLOperatorUtil.deletePath(path)
    model.save(hc.sparkContext, path)
    model
  }

  /**
    * 应用模型
    */
  def predict(hc: HiveContext, processId: Long, nodeVertexInstanceId: String, model: KMeansModel, properties: Properties, predictInputData: DataFrame) = {


    //将DF转化为RDD,含有第一列为hashkey
    val dataWithRowkey = TaskUtils.dfWithRowkeyToRDD(predictInputData, ",")

    val hbaseTableName = "process_" + processId + "_" + nodeVertexInstanceId

    //预测结果和原始数据
    val rowkeyAndPredcitAndOriginalData = dataWithRowkey.map { t =>
      (t._1, model.predict(t._2))
    }

    //使用模型测试模型
    val hashkeyOriginalPredictToString = rowkeyAndPredcitAndOriginalData.map(t => t._1 + "," + t._2)
    val predictSchema = StructField("predictResult", DoubleType, true)
    val schema: StructType = StructType(predictInputData.schema.fields :+ predictSchema)
    val resultDf = TaskUtils.rddStringToDF(hc, hashkeyOriginalPredictToString, schema, ",")

    //将预测结果保存到hbase上面
    HBaseByDataframeUtils.writeHBase(hc.sparkContext, resultDf, hbaseTableName, "default", "key", "feature")
    resultDf
  }
}

