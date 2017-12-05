package com.zxsimple.dmls.operator.ml

import java.util.Properties

import com.zxsimple.dmls.common.exception.ModelException
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.common.util.HiveUDF.CreateHash
import com.zxsimple.dmls.common.util.hbaseUtils.HBaseBean.HBaseByDataframeUtils
import com.zxsimple.dmls.manager.util.TaskUtils
import com.zxsimple.dmls.operator.OperatorTemplate
import com.zxsimple.dmls.util.MLOperatorUtil
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType

/**
  * Created by lenovo on 2016/10/28.
  */
class AssociationRulesOperator extends OperatorTemplate {
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
    val properties: Properties = currentNode.getProperties
    val nodeVertexInstanceId = currentNode.getInstanceId
    val processId = currentNode.getProcessId

    //算法参数
    val minConfidence = properties.getProperty("minConfidence").toDouble


    //组成训练数据
    val trainInputDataTmp = inputPortOneData.drop("hashkey")
    val dataToRdd = TaskUtils.dfToRDD(trainInputDataTmp, ",")
    val transactions: RDD[Array[String]] = dataToRdd.map(s => s.trim.split(","))
    val freqItemsets = transactions.flatMap(xs => (xs.combinations(1) ++ xs.combinations(2))
      .map(x => (x.toList, 1L))).reduceByKey(_ + _).map { case (xs, cnt) => new FreqItemset(xs.toArray, cnt) }

    //将模型评估结果保存到Model数据库中
    MLOperatorUtil.saveSpecificMetricResult("noMetricsResult", -100000, processId, nodeVertexInstanceId, false)

    val hbaseTableName = "process_" + processId + "_" + nodeVertexInstanceId
    //
    //      //保存频繁项集为DF格式
    //      val freqItemset: RDD[String] =  freqItemsets.map { itemset =>
    //        itemset.items.mkString(",") + " " + itemset.freq
    //      }
    //      val colItemList = List("items","freq")
    //      val freqItemsetsDF = TaskUtils.rddStringToDF(hc,freqItemset,colItemList,StringType," ")

    //保存关联规保存到Hbase中，首列加上haskkey
    val s = "2"
    val ruleString: RDD[Rule[String]] = new AssociationRules().setMinConfidence(minConfidence).run(freqItemsets)
    val resultsToRDDstring = ruleString.map { rule => new CreateHash().evaluate(s) + " " + rule.antecedent.mkString(",") + " " + rule.consequent.mkString(",") + " " + rule.confidence }
    //antecedent表示前项
    //consequent表示后项
    //confidence表示置信度

    //生成新的DF
    val colList = List("hashkey", "antecedent", "consequent", "confidence")
    val resultDF = TaskUtils.rddStringToDF(hc, resultsToRDDstring, colList, StringType, " ")
    //    val result: DataFrame = TaskUtils.df1ZipDF2(hc,inputPortOneData, resultDF)

    //将结果保存到hbase中，默认表名, 列族名是feature
    HBaseByDataframeUtils.writeHBase(hc.sparkContext, resultDF, hbaseTableName, "default", "key", "feature")

    resultDF

    //      +----------+----------+----------+
    //      |antecedent|consequent|confidence|
    //      +----------+----------+----------+
    //      |         a|         b|       1.0|
    //        |         d|         e|       1.0|
    //        |         d|         f|       1.0|
    //        |         f|         e|       1.0|
    //        +----------+----------+----------


  }
}
