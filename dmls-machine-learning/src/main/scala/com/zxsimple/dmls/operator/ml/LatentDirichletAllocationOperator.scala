package com.zxsimple.dmls.operator.ml

import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.common.util.hbaseUtils.HBaseBean.HBaseByDataframeUtils
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.MLOperatorUtil
import org.apache.spark.mllib.clustering.{LDA, LocalLDAModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType

/**
  * Created by lenovo on 2016/10/28.
  * 隐含狄利克雷聚类模型——用于文档主题分类
  */
class LatentDirichletAllocationOperator extends OperatorTemplate {
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
  def train(hc: HiveContext, processId: Long, nodeVertexInstanceId: String, properties: Properties, trainInputData: DataFrame): LocalLDAModel = {
    var model: LocalLDAModel = null

    //算法参数
    val processId = properties.getProperty("id").toDouble.toLong
    try {
      //算法参数
      val maxIterations = properties.getProperty("maxIterations").toDouble.toInt
      val k = properties.getProperty("k").toDouble.toInt
      val seed = properties.getProperty("seed").toDouble.toLong


      //组成训练数据
      val trainInputDataTmp = trainInputData.drop("hashkey")
      val dataToRdd = TaskUtils.dfToRDD(trainInputDataTmp, ",")
      val dateToVector = TaskUtils.rddStringToVector(dataToRdd, ",")

      // Index documents with unique IDs
      val corpus = dateToVector.zipWithIndex.map(_.swap).cache()
      val corpusSize = corpus.count
      val ldaModel = new LDA().setK(k).setOptimizer("online").setMaxIterations(maxIterations).setSeed(seed).run(corpus)
      model = ldaModel.asInstanceOf[LocalLDAModel]

      //模型评估
      //avgLogLikelihood(似然值均值)越大，模型越好
      val avgLogLikelihood = model.logLikelihood(corpus) / corpusSize.toDouble

      //将模型评估结果保存到Model数据库中
      val path = MLOperatorUtil.saveSpecificMetricResult("avgLogLikelihood", avgLogLikelihood, processId, nodeVertexInstanceId, true)

      //保存模型
      MLOperatorUtil.deletePath(path)
      model.save(hc.sparkContext, path)
      model
    }
  }

  /**
    * 应用模型
    */
  def predict(hc: HiveContext, processId: Long, nodeVertexInstanceId: String, model: LocalLDAModel, properties: Properties, predictInputData: DataFrame) = {

    //组成应用数据数据 有hashkey
    val dataToRddOri = TaskUtils.dfToRDD(predictInputData, ",")

    //去掉第一列hashkey
    val predictInputDataTmp = predictInputData.drop("hashkey")
    //组成应用数据数据 无hashkey
    val dataToRddPredict = TaskUtils.dfToRDD(predictInputDataTmp, ",")
    val dateToVector = TaskUtils.rddStringToVector(dataToRddPredict, ",")
    val hbaseTableName = "process_" + processId + "_" + nodeVertexInstanceId

    // Index documents with unique IDs
    val corpus: RDD[(Long, Vector)] = dateToVector.zipWithIndex.map(_.swap).cache()

    //应用模型
    val rddLabelPredict = model.topicDistributions(corpus).map(t => t._1 + "," + t._2.argmax)

    val colList = List("documentsId", "topic");

    //生成新的DF
    val resultDF = TaskUtils.rddStringToDF(hc, rddLabelPredict, colList, StringType, ",")
    val result: DataFrame = TaskUtils.df1ZipDF2(hc, predictInputData, resultDF)

    //将结果保存到hbase中，默认表名, 列族名是feature
    HBaseByDataframeUtils.writeHBase(hc.sparkContext, result, hbaseTableName, "default", "key", "feature")

    result


  }

}