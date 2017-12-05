package com.zxsimple.dmls.operator.ml

import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.common.util.hbaseUtils.HBaseBean.HBaseByDataframeUtils
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.MLOperatorUtil
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * Created by lenovo on 2016/10/28.
  * 决策树回归模型
  */
class DecisionTreeRegressionOperator extends OperatorTemplate {
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
    val decisionTreeRegressionModel = train(hc, processId, nodeVertexInstanceId, properties, inputPortOneData)

    //如果第二个参数不为空，则进入应用
    if (inputPortTwoData != null) {
      val result = predict(hc, processId, nodeVertexInstanceId: String, decisionTreeRegressionModel, properties, inputPortTwoData)
      currentNode.setOutPortOneData(result)
    }

  }

  def train(hc: HiveContext, processId: Long, nodeVertexInstanceId: String, properties: Properties, trainInputData: DataFrame): DecisionTreeModel = {
    var model: DecisionTreeModel = null
    //算法参数
    val maxDepth = properties.getProperty("maxDepth").toDouble.toInt
    val maxBins = properties.getProperty("maxBins").toDouble.toInt
    val categoricalFeaturesInfo = new java.util.HashMap[Integer, Integer]()
    val split = properties.getProperty("split").toDouble

    //      //模型保存地址
    //      val path = properties.getProperty("path")

    //组成训练数据
    val trainInputDataTmp = trainInputData.drop("hashkey")
    val dataToRdd = TaskUtils.dfToRDD(trainInputDataTmp, ",")
    val dateToLabeledPoint = TaskUtils.dataToLabeledPoint(dataToRdd, ",")
    val (trainData, testData) = MLOperatorUtil.getRegressionTrainsplit(dateToLabeledPoint, split)

    //模型训练
    model = DecisionTree.trainRegressor(trainData, categoricalFeaturesInfo,
      "variance", maxDepth, maxBins)

    //使用模型测试模型
    val predictionAndLabels = testData.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }
    //将模型评估结果保存到Model数据库中
    val path = MLOperatorUtil.saveMetricsResult("regressionMetrics", predictionAndLabels, processId, nodeVertexInstanceId, true)

    //保存模型
    MLOperatorUtil.deletePath(path)
    model.save(hc.sparkContext, path)
    model
  }

  /**
    * 应用模型
    */
  def predict(hc: HiveContext, processId: Long, nodeVertexInstanceId: String, decisionTreeModel: DecisionTreeModel, properties: Properties, predictInputData: DataFrame) = {

    //将DF转化为RDD,含有第一列为hashkey
    val dataWithRowkey = TaskUtils.dfWithRowkeyToRDD(predictInputData, ",")

    val hbaseTableName = "process_" + processId + "_" + nodeVertexInstanceId

    //预测结果和原始数据
    val rowkeyAndPredcitAndOriginalData = dataWithRowkey.map { t =>
      (t._1, decisionTreeModel.predict(t._2))
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
