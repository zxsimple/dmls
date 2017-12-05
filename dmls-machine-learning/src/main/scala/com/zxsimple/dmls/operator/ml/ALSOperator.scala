package com.zxsimple.dmls.operator.ml

import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.common.util.hbaseUtils.HBaseBean.HBaseByDataframeUtils
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.MLOperatorUtil
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * Created by lenovo on 2016/10/28.
  */
class ALSOperator extends OperatorTemplate {
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

  def train(hc: HiveContext, processId: Long, nodeVertexInstanceId: String, properties: Properties, trainInputData: DataFrame): MatrixFactorizationModel = {
    var model: MatrixFactorizationModel = null
    //算法参数
    val rank = properties.getProperty("rank").toDouble.toInt
    val numIterations = properties.getProperty("numIterations").toDouble.toInt
    val lambda = properties.getProperty("lambda").toDouble

    //组成训练数据
    val trainInputDataTmp = trainInputData.drop("hashkey")
    val dataToRdd = TaskUtils.dfToRDD(trainInputDataTmp, ",")
    val ratings = dataToRdd.map(_.trim.split(",") match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    model = ALS.train(ratings, rank, numIterations, lambda)

    //模型评估
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    //将模型评估结果保存到Model数据库中
    val path = MLOperatorUtil.saveSpecificMetricResult("MSE", MSE, processId, nodeVertexInstanceId, true)

    //保存模型
    MLOperatorUtil.deletePath(path)
    model.save(hc.sparkContext, path)
    model
  }

  /**
    * 应用模型
    */
  def predict(hc: HiveContext, processId: Long, nodeVertexInstanceId: String, model: MatrixFactorizationModel, properties: Properties, predictInputData: DataFrame) = {

//    //去掉第一列hashkey
//    val predictInputDataTmp = predictInputData.drop("hashkey")
    val hbaseTableName = "process_" + processId + "_" + nodeVertexInstanceId
    //组成应用数据数据 无hashkey
    val dataToRdd = TaskUtils.dfToRDD(predictInputData, ",")
    val parsedData  = dataToRdd.map { s =>
      val usersAndProduct = s.split(",")
      (s,usersAndProduct(0).toDouble.toInt, usersAndProduct(1).toDouble.toInt)
    }

    //应用模型
    val oriniginalDataWithHashkeyAndpredict = parsedData.map{ t => (t._1,model.predict(t._2,t._3).formatted("%.3f"))}

    //使用模型测试模型
    val hashkeyOriginalPredictToString = oriniginalDataWithHashkeyAndpredict.map(t => t._1 + "," + t._2)
    val predictSchema = StructField("predictResult", DoubleType, true)
    val schema: StructType = StructType( predictInputData.schema.fields  :+  predictSchema)
    val resultDf = TaskUtils.rddStringToDF(hc,hashkeyOriginalPredictToString,schema,",")

    //将预测结果保存到hbase上面
    HBaseByDataframeUtils.writeHBase(hc.sparkContext, resultDf, hbaseTableName, "default", "key", "feature")
    resultDf
  }
}

