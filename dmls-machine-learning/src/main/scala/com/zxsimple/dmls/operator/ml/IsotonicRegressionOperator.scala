package com.zxsimple.dmls.operator.ml

import java.util.Properties

import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.common.util.hbaseUtils.HBaseBean.HBaseByDataframeUtils
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.MLOperatorUtil
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * Created by lenovo on 2016/10/28.
  * 保序回归模型
  */
class IsotonicRegressionOperator extends OperatorTemplate  {
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
      val result = predict(hc, processId, nodeVertexInstanceId: String,model, properties, inputPortTwoData)
      currentNode.setOutPortOneData(result)
    }

  }

  /**
    * 训练模型
    *
    * @return
    */
  def train(hc: HiveContext, processId: Long, nodeVertexInstanceId: String, properties: Properties, trainInputData: DataFrame): IsotonicRegressionModel = {
    var model: IsotonicRegressionModel = null
      //算法参数
      val split = properties.getProperty("split").toDouble
      val sotonic = properties.getProperty("isotonic").toBoolean

      //组成训练数据
      val trainInputDataTmp = trainInputData.drop("hashkey")
      val datatordd = TaskUtils.dfToRDD(trainInputDataTmp , ",")
      //训练数据
      val parsedData = datatordd.map { line =>
        val parts = line.split(",").map(_.toDouble)
        (parts(0), parts(1), 1.0)
      }
      val tempData = parsedData.randomSplit(Array(split, 1 - split), seed = 11L)
      val (dataTrain, dataTest) = (tempData(0), tempData(1))

      //模型训练
      model = new IsotonicRegression().setIsotonic(sotonic).run(dataTrain)

      //使用模型测试模型
      val predictionAndLabels = dataTest.map { parts =>
        val predictedLabel = model.predict(parts._2)
        (predictedLabel, parts._1)
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
  def predict(hc: HiveContext, processId: Long,nodeVertexInstanceId: String, model: IsotonicRegressionModel, properties: Properties, predictInputData: DataFrame) = {

      //组成应用数据数据 无hashkey
      val dataToRddPredict = TaskUtils.dfToRDD(predictInputData,",")
      val dateToDouble = dataToRddPredict.map { line =>
        val linded  = line.split(",")
        (line,linded(1).trim.toDouble)

      }
      val hbaseTableName = "process_"+processId+"_"+ nodeVertexInstanceId

      //应用模型
      val rowkeyAndPredcitAndOriginalData = dateToDouble.map { t =>
        (t._1,model.predict(t._2))
      }


    //生成新的DF
    val hashkeyOriginalPredictToString = rowkeyAndPredcitAndOriginalData.map(t => t._1 + "," + t._2)
    val predictSchema = StructField("predictResult", DoubleType, true)
    val schema: StructType = StructType( predictInputData.schema.fields  :+  predictSchema)
    val resultDf = TaskUtils.rddStringToDF(hc,hashkeyOriginalPredictToString,schema,",")

    //将预测结果保存到hbase上面
    HBaseByDataframeUtils.writeHBase(hc.sparkContext, resultDf, hbaseTableName, "default", "key", "feature")
    resultDf

  }

}

