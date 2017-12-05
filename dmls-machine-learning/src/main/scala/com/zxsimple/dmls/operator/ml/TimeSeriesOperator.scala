package com.zxsimple.dmls.operator.ml

import java.util.Properties

import com.cloudera.sparkts.models.ARIMA
import com.zxsimple.dmls.common.exception.ModelException
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.common.util.HiveUDF.CreateHash
import com.zxsimple.dmls.common.util.hbaseUtils.HBaseBean.HBaseByDataframeUtils
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.MLOperatorUtil
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

/**
  * Created by lenovo on 2016/10/28.
  */
class TimeSeriesOperator extends OperatorTemplate {
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
  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame) = {
    //首先进入训练
    val result = run(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame)
    currentNode.setOutPortOneData(result)

    //如果第二个参数不为空，则抛出异常
    if (inputPortTwoData != null) {
      throw new ModelException(" The model only supports inputPortOneData!")
    }
  }

  def run(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame) = {
    val nodeVertexInstanceId = currentNode.getInstanceId
    val sc = hc.sparkContext
    val processId = currentNode.getProcessId
    //算法参数
    val properties: Properties = currentNode.getProperties
    val p = properties.getProperty("p").toDouble.toInt
    val d = properties.getProperty("d").toDouble.toInt
    val q = properties.getProperty("q").toDouble.toInt
    val nFeatures = properties.getProperty("nFeatures").toDouble.toInt

    //组成应用数据数据 有hashkey
    val dataToRddOri = TaskUtils.dfToRDD(inputPortOneData, ",")
    val hbaseTableName = "process_" + processId + "_" + nodeVertexInstanceId

    //组成训练数据
    val trainInputDataTmp = inputPortOneData.drop("hashkey")
    val dataToRdd = TaskUtils.dfToRDD(trainInputDataTmp, ",")
    //训练数据
    // val dataTrain = Vector(dataToRdd.map(_.toDouble).collect())
    val dataTrain = Vectors.dense(dataToRdd.map(_.toDouble).collect())
    //模型训练
    val model = ARIMA.fitModel(p, d, q, dataTrain)

    //模型评估
    val modelMetrics = model.logLikelihoodCSS(dataTrain)

    //将模型评估结果保存到Model数据库中
    MLOperatorUtil.saveSpecificMetricResult("logLikelihood", modelMetrics, processId, nodeVertexInstanceId, false)

    //模型预测
    val s = "0"
    val predict = model.forecast(dataTrain, nFeatures).toArray
    val hashkeyAndpredictRdd: RDD[(Integer, Double)] = hc.sparkContext.makeRDD(predict).map { t => (new CreateHash().evaluate(s), t) }

    //使用模型测试模型
    val hashkeyOriginalPredictToString = hashkeyAndpredictRdd.map(t => t._1 + "," + t._2)
    val predictSchema = StructField("predictResult", DoubleType, true)
    val schema: StructType = StructType(Array(StructField("hashkey", IntegerType, true)) :+ predictSchema)
    val resultDf = TaskUtils.rddStringToDF(hc, hashkeyOriginalPredictToString, schema, ",")

    //将预测结果保存到hbase上面
    HBaseByDataframeUtils.writeHBase(hc.sparkContext, resultDf, hbaseTableName, "default", "key", "feature")
    resultDf

  }
}
