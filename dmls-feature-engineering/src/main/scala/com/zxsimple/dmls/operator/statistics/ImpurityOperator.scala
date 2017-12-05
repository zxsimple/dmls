package com.zxsimple.dmls.operator.statistics

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2016/10/27 0027.
  */
class ImpurityOperator extends OperatorTemplate {
  /**
    * 验证参数
    *
    * @param properties
    */
  override def validate(properties: Properties): (Boolean, String) = {
    val selectedColLength = properties.get("selectedCol").toString.split(",").length
    if (selectedColLength != 1) {
      (false, "参数验证失败")
    }
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {
    val data = inputPortOneData
    //获取参数
    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val labelCol = properties.get("labelCol").toString
    val selectedCol = properties.get("selectedCol").toString
    //自定义数据
    val featuredf = data.select(selectedCol)
    val labelRDDDouble = data.select(labelCol).rdd.map(_.get(0).toString.toDouble)
    val featureRDDDouble = featuredf.rdd.map(_.get(0).toString.toDouble)

    /*
     *离散值特征分析
     */
    //判断特征是否为离散型特征
    if (featuredf.count().toInt > featuredf.distinct().count().toInt) {
      val fea_arry = featureRDDDouble.mapPartitions { row =>
        val values = row.map { case a => (a, 1) }
        values
      }.reduceByKey(_ + _).values.map(_.toDouble).collect()
      val fea_count = fea_arry.sum

      //feature_gini——离散特征的基尼系数
      val feature_gini = impurity(fea_arry, fea_count, "gini")

      //feature_entropy——离散特征的熵
      val feature_entropy = impurity(fea_arry, fea_count, "entropy")

      //增益计算
      val feature_lable = featureRDDDouble.zip(labelRDDDouble).map { case (a, b) => ((a, b), 1) }.reduceByKey(_ + _)
      val label_fea = feature_lable.map { case ((a, b), c) => (a, c) }.groupByKey().map { case (a, c) => (a, c.map(_.toDouble).toArray) }.collect()
      val totalcount = feature_lable.map { case ((a, b), c) => c }.collect().sum.toDouble

      //gini_gain——基尼增益
      val gini_gain = feature_gini - gain(label_fea, totalcount, "gini_gain")
      //info_gain——信息增益
      val info_gain = feature_entropy - gain(label_fea, totalcount, "info_gain")

      //info_gain_ratio——信息增益率
      val info_gain_ratio = info_gain / feature_entropy

      println("离散特征的基尼系数：" + feature_gini)
      println("离散特征的熵：" + feature_entropy)
      println("基尼增益：" + gini_gain)
      println("信息增益：" + info_gain)
      println("信息增益率：" + info_gain_ratio)

      //保存统计数据.并更新MySql状态
      val obj = new JSONObject()
      obj.put("taskName", "Impurity")
      obj.put("标签列", labelCol)
      obj.put("选中特征列", selectedCol)
      obj.put("离散特征的基尼系数", feature_gini)
      obj.put("离散特征的熵", feature_entropy)
      obj.put("基尼增益", gini_gain)
      obj.put("信息增益", info_gain)
      obj.put("信息增益率", info_gain_ratio)
      val jsonStr = obj.toJSONString
      currentNode.setStatisticsData(jsonStr)
    } else throw new IllegalArgumentException(" The feature should not be continuous!")

  }

  //计算不纯度，包括基尼系数和信息熵
  def impurity(fea_arry: Array[Double], fea_count: Double, function: String): Double = {
    function match {
      case "gini" => Gini.calculate(fea_arry, fea_count)
      case "entropy" => Entropy.calculate(fea_arry, fea_count)
    }
  }

  //计算信息增益，包括基尼增益和信息增益
  def gain(label_fea: Array[(Double, Array[Double])], totalcount: Double, function: String): Double = {
    val num = label_fea.length
    function match {
      case "gini_gain" =>
        var newImpurity = 0.0
        var i = 0
        while (i < num) {
          val p_i = label_fea(i)._2
          val cout_i = p_i.sum
          val e_i = Gini.calculate(p_i, cout_i)
          val p_e_i = cout_i / totalcount
          newImpurity += p_e_i * e_i
          i += 1
        }
        newImpurity

      case "info_gain" =>
        var newImpurity = 0.0
        var i = 0
        while (i < num) {
          val p_i = label_fea(i)._2
          val cout_i = p_i.sum
          val e_i = Entropy.calculate(p_i, cout_i)
          val p_e_i = cout_i / totalcount
          newImpurity += p_e_i * e_i
          i += 1
        }
        newImpurity
    }
  }
}
