package com.zxsimple.dmls.operator.ml

import java.util.Properties

import com.zxsimple.dmls.common.exception.ModelException
import com.zxsimple.dmls.common.metadata.dao.impl.ModelDaoImpl
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.mllib.classification.{LogisticRegressionModel, NaiveBayesModel, SVMModel}
import org.apache.spark.mllib.clustering.{GaussianMixtureModel, KMeansModel, LocalLDAModel}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.regression.{IsotonicRegressionModel, LinearRegressionModel}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zxsimple on 2016/11/1.
  */
class PredictModelOperator extends OperatorTemplate {
  /**
    * 验证参数
    *
    * @param properties
    */
  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  /**
    * Abstract method to be implemented in Concrete Class to handle the specific ETL logic
    *
    * @param currentNode
    *
    */
  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val processId = currentNode.getProcessId
    val properties: Properties = currentNode.getProperties
    val nodeVertexInstanceId = currentNode.getInstanceId
    val modelId = properties.get("modelId").toString.toLong
    val modelDao = new ModelDaoImpl
    val model = modelDao.find(modelId)
    val modelPath = model.getPath
    val algName = model.getAlg

    val result: DataFrame = algName match {
      //决策树分类算法
      case "DecisionTreeClassification" =>
        val model = DecisionTreeModel.load(hc.sparkContext, modelPath)
        new DecisionTreeClassificationOperator().predict(hc, processId, nodeVertexInstanceId, model, properties, inputPortOneData)

      case "DecisionTreeRegression" =>
        val model = DecisionTreeModel.load(hc.sparkContext, modelPath)
        new DecisionTreeRegressionOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "GBTClassification" =>
        val model = GradientBoostedTreesModel.load(hc.sparkContext, modelPath)
        new GBTClassificationOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "GBTRegression" =>
        val model = GradientBoostedTreesModel.load(hc.sparkContext, modelPath)
        new GBTRegressionOperator().predict(hc, processId,nodeVertexInstanceId,  model, properties, inputPortOneData)

      case "IsotonicRegression" =>
        val model = IsotonicRegressionModel.load(hc.sparkContext, modelPath)
        new IsotonicRegressionOperator().predict(hc, processId, nodeVertexInstanceId,model, properties, inputPortOneData)

      case "LinearRegressionWithSGD" =>
        val model = LinearRegressionModel.load(hc.sparkContext, modelPath)
        new LinearRegressionWithSGDOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "LogisticRegressionLBFGS" =>
        val model = LogisticRegressionModel.load(hc.sparkContext, modelPath)
        new LogisticRegressionLBFGSOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "NaiveBayes" =>
        val model = NaiveBayesModel.load(hc.sparkContext, modelPath)
        new NaiveBayesOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "RandomForestClassification" =>
        val model = RandomForestModel.load(hc.sparkContext, modelPath)
        new RandomForestClassificationOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "RandomForestRegression" =>
        val model = RandomForestModel.load(hc.sparkContext, modelPath)
        new RandomForestRegressionOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "SVMWithSGD" =>
        val model = SVMModel.load(hc.sparkContext, modelPath)
        new SVMWithSGDOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case " GaussianMixture" =>
        val model = GaussianMixtureModel.load(hc.sparkContext, modelPath)
        new GaussianMixtureOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "KMeans" =>
        val model = KMeansModel.load(hc.sparkContext, modelPath)
        new KmeansOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "LatentDirichletAllocation" =>
        val model = LocalLDAModel.load(hc.sparkContext, modelPath)
        new LatentDirichletAllocationOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case "ALS" =>
        val model = MatrixFactorizationModel.load(hc.sparkContext, modelPath)
        new ALSOperator().predict(hc, processId,nodeVertexInstanceId, model, properties, inputPortOneData)

      case _ => throw new ModelException("The model isn`t supported prediction or the model is being developed")
    }

    currentNode.setOutPortOneData(result)

    }




}
