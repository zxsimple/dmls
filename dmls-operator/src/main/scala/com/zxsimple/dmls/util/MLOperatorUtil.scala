package com.zxsimple.dmls.util

import java.sql.Timestamp
import java.text.NumberFormat
import java.util

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.metadata.dao.impl._
import com.zxsimple.dmls.common.metadata.model._
import com.zxsimple.dmls.common.util.HDFSUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zxsimple on 2016/11/1.
  */
class MLOperatorUtil {
  val modelDao = new ModelDaoImpl()
  val nodeVertexDao = new NodeVertexDaoImpl()
  /**
    * 划分训练集和测试集 （公用方法）
    *
    * @param input 划分的数据集
    * @param array 划分数组
    * @param seed  随机种子
    */
  def randomSplit(input: RDD[LabeledPoint], array: Array[Double], seed: Long): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val split = input.randomSplit(array, seed)
    (split(0), split(1))
  }

  /**
    * 分类算法，划分数据集和训练集，返回分类个数
    */
  def getClassificationTrainsplit(data: RDD[LabeledPoint], sampleSplit: Double): (RDD[LabeledPoint], RDD[LabeledPoint], Int) = {
    val numClasses = data.map(_.label).distinct().count().toInt
    //划分数据集
    val (dataTrain, dataTest) = randomSplit(data, Array(sampleSplit, 1 - sampleSplit), seed = 11L)
    (dataTrain, dataTest, numClasses)
  }

  /**
    * 回归算法，划分数据集和训练集
    */
  def getRegressionTrainsplit(data: RDD[LabeledPoint], sampleSplit: Double): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val (dataTrain, dataTest) = randomSplit(data, Array(sampleSplit, 1 - sampleSplit), seed = 11L)
    (dataTrain, dataTest)
  }

  //=============================================模型评估公共方法============================================================
  /**
    * 执行分类的模型评估
    */
  def evaluateClassificationAlg(processId: Long, nodeVertexInstanceId: String, predictionAndLabels: RDD[(Double, Double)], numClasses: Int, canUse: Boolean) = {
    //将模型评估结果保存到Model数据库中
    if (numClasses == 2) {
      saveMetricsResult("binaryClassMetrics", predictionAndLabels, processId, nodeVertexInstanceId, canUse)
    } else {
      saveMetricsResult("multiClassMetrics", predictionAndLabels, processId, nodeVertexInstanceId, canUse)
    }
  }

  /**
    * 保存模型数据
    *
    * @param canUse  模型能不能使用
    *                true：表示能使用，model的path存储路径为/Ainspir/" + userId + "/model/" + modelId + "/"
    *                false：表示不能使用，无法出现在应用列表里面，path的值为nouse
    * @return
    */
  def saveModel(processId: Long, nodeVertexInstanceId: String, resultJson:String, canUse:Boolean): String = {
    val processDao = new ProcessDaoImpl()
    //    val modelMetrics = new ModelMetrics
    val nodeVertex = nodeVertexDao.find(nodeVertexInstanceId)
    val alg = nodeVertex.getOperatorName
    var model: Model = modelDao.findModelByProidAndInsid(processId, nodeVertexInstanceId)
    val process = processDao.find(processId)

    //没有model就新建，有model就更新
    if (model == null) {
      model = new Model
      model.setNodeVertex(nodeVertex)
      model.setProcess(process)
      modelDao.create(model)
    }
    val userId = process.getUserId;
    val modelId = model.getId
    val path = if(canUse)"/Ainspir/" + userId + "/model/" + modelId + "/" else "nouse"
    model.setAlg(alg.toString)
    model.setPath(path)
    model.setIsDeleted(0)
    model.setEvaluation(resultJson)
    model.setUpdateTime(new Timestamp(System.currentTimeMillis()))
    modelDao.update(model)
    path
  }

  /**
    * 将具体算法评估结果的统计信息保存到model表中（评估方法为特定的方法）
    *
    * @param metricsName   评估方法名称（binaryClassMetrics，multiClassMetrics，regressionMetrics）
    * @param metricsResult 评估结果
    * @param processId     方案id
    */
  def saveSpecificMetricResult(metricsName: String, metricsResult: Double, processId: Long, nodeVertexInstanceId: String, canUse: Boolean) = {
    //将模型评估结果保存到Model数据库中
    var resultJson: String = null
    if (metricsResult == -100000) resultJson = "{ " + metricsName + "}"
    else
      resultJson = "{ " + metricsName + ":" + metricsResult.formatted("%.3f").toString + "}"

    saveModel(processId,nodeVertexInstanceId,resultJson,canUse)

  }

  /**
    * 将算法评估结果的统计信息保存到model表中 （公用方法）
    *
    * @param metricsType         评估结果类型（binaryClassMetrics，multiClassMetrics，regressionMetrics）
    * @param predictionAndLabels 测试结果
    * @param processId           方案id
    */
  def saveMetricsResult(metricsType: String, predictionAndLabels: RDD[(Double, Double)], processId: Long, nodeVertexInstanceId: String, canUse: Boolean):String = {
    //将模型评估结果保存到Model数据库中
    var resultJson = evaluateModel(metricsType, predictionAndLabels,0)
    if (metricsType.equals("binaryClassMetrics")) {
      //如果是二进制就是
      val nums = this.modelTrueFalseNum(predictionAndLabels)
      resultJson = resultJson.substring(0, resultJson.length - 1) + "," + nums
    }
    saveModel(processId,nodeVertexInstanceId,resultJson,canUse)
  }

  /**
    * 模型真假阴阳个数
    */
  def modelTrueFalseNum(data: RDD[(Double, Double)]): String = {
    val evaluation = EvaluationUtils.binaryClassMetrics(data)
    //每个标签的个数
    val value1 = evaluation.pr().collect()(0)._1
    val value2 = evaluation.pr().collect()(0)._2
    val count0 = data.filter(_._1 == value1).count
    val count1 = data.filter(_._1 == value2).count
    val size = data.count()
    val precision = evaluation.pr().collect()(1)._2
    val recall = evaluation.pr().collect()(1)._1
    val TT = (count1 * precision).round
    val FT = count1 - TT
    val TF = (count1 / recall * (1 - recall)).round
    val FF = count0 + count1 - TT - TF - FT
    val nums = "TT:" + TT + ",FT:" + FT + ",TF:" + TF + ",FF:" + FF + "}"
    nums
  }

  /**
    * 模型评估
    */
  def evaluateModel(modelType: String, inputData: RDD[(Double, Double)], evaluateData: Double): String = {
    //保留3位小数
    def optimizeDouble(data: Double) = {
      data.formatted("%.3f").toDouble.toString
    }

    modelType match {
      case "binaryClassMetrics" =>
        val metrics = EvaluationUtils.binaryClassMetrics(inputData)
        val evaluationJson = "{" + "type:" + "\"binaryClass\"" + "," +
          "PR:" + optimizeDouble(metrics.areaUnderPR()) + "," +
          "ROC:" + optimizeDouble(metrics.areaUnderROC()) + "}"
        evaluationJson

      case "multiClassMetrics" =>
        val metrics = EvaluationUtils.multiClassMetrics(inputData)
        val evaluationJson = "{" + "type:" + "\"multiClass\"" + "," +
          "recall:" + optimizeDouble(metrics.recall) + "," +
          "precision:" + optimizeDouble(metrics.precision) + "," +
          "weightedFalsePositiveRate:" + optimizeDouble(metrics.weightedFalsePositiveRate) + "," +
          "weightedFMeasure:" + optimizeDouble(metrics.weightedFMeasure) + "," +
          "weightedPrecision:" + optimizeDouble(metrics.weightedPrecision) + "," +
          "weightedRecall:" + optimizeDouble(metrics.weightedRecall) + "," +
          "weightedTruePositiveRate:" + optimizeDouble(metrics.weightedTruePositiveRate) + "}"
        evaluationJson

      case "regressionMetrics" =>
        val metrics = EvaluationUtils.regressionMetrics(inputData)
        val evaluationJson = "{" + "type:" + "\"regression\"" + "," +
          "rootMeanSquaredError:\"" + (metrics.rootMeanSquaredError).formatted("%.3f").toDouble + "\"," +
          "explainedVariance:\"" + (metrics.explainedVariance).formatted("%.3f").toDouble + "\"," +
          "meanSquaredError:\"" + (metrics.meanSquaredError).formatted("%.3f").toDouble + "\"," +
          "r2:\"" + (metrics.r2).formatted("%.3f").toDouble + "\"," +
          "meanAbsoluteError:\"" + (metrics.meanAbsoluteError).formatted("%.3f").toDouble + "\"}"
        evaluationJson

      case "MSE" =>
        val metrics = EvaluationUtils.regressionMetrics(inputData)
        val evaluationJson = "{" + "type:" + "\"MSE\"" + "," +
          "MSE:\"" + evaluateData.formatted("%.3f").toDouble + "\"}"
        evaluationJson

      case "logLikelihood" =>
        val metrics = EvaluationUtils.regressionMetrics(inputData)
        val evaluationJson = "{" + "type:" + "\"logLikelihood\"" + "," +
          "logLikelihood:\"" + evaluateData.formatted("%.3f").toDouble + "\"}"
        evaluationJson

      case "WSSSE" =>
        val metrics = EvaluationUtils.regressionMetrics(inputData)
        val evaluationJson = "{" + "type:" + "\"WSSSE\"" + "," +
          "WSSSE:\"" + evaluateData.formatted("%.3f").toDouble + "\"}"
        evaluationJson

      case "avgLogLikelihood" =>
        val metrics = EvaluationUtils.regressionMetrics(inputData)
        val evaluationJson = "{" + "type:" + "\"MSE\"" + "," +
          "MSE:\"" + evaluateData.formatted("%.3f").toDouble + "\"}"
        evaluationJson


      case "noAssess" =>
        val evaluationJson = "{type:\"noAssess\"}"
        evaluationJson
    }
  }

  //======================================================预测可视化======================================================
  /**
    * 回归预测结果可视化中，转化为统一json
    */
  def regressionVisualResult( operatorName:String, dfWithHashkey: DataFrame) = {

    val regressionVisualJson = new JSONObject
    regressionVisualJson.put("name", operatorName)
    regressionVisualJson.put("type", "regression")
    regressionVisualJson.put("attrs", null)
    val childrenArray = statisticsRegressionResult(dfWithHashkey)

    regressionVisualJson.put("children", childrenArray)
    regressionVisualJson.toJSONString
  }
  /**
    * 分类预测结果中可视化化，转化为统一json
    */
  def LabelAndFeatureDfToJsonobject(hc: HiveContext, labelSum:Int,labelArray:Array[Double],df: DataFrame, split: Array[Double], featureName: String): JSONObject = {

    //格式化数据
    val nf = NumberFormat.getInstance()

    //每一列的json对象
    val children = new JSONObject
    children.put("name", featureName)
    children.put("type", "feature")
    //本列的区间数组
    val children1 = new JSONObject()
    children1.put("name", "")
    children1.put("type", "range")
    val childrenAttrs = new JSONObject()
    childrenAttrs.put("rangenum", "3")
    children1.put("attrs", childrenAttrs)

    val child1Arrays: Array[JSONObject] = new Array[JSONObject](3)

    //保存结果的区间
    def featureRegion(featureIndex: Int): String = {
      var region = ""
      val nf = NumberFormat.getInstance()
      region = nf.format("%1.3f".format(split(featureIndex)).toDouble) + "-" + nf.format("%1.3f".format(split(featureIndex + 1)).toDouble)
      region
    }

    val featureRangeCount = new util.HashMap[String, Long]()
    df.collect().foreach({ row=>
      val count = row.get(0).toString.toDouble.toLong
      val label = row.get(1).toString.toDouble
      val featureIndex = row.get(2).toString.toDouble.toInt
      //        val featureRange = featureRegion(featureIndex)
      featureRangeCount.put((featureIndex + "_" + label), count)
    })

    for (i <- 0 until 3) {
      val featureRange = featureRegion(i)
      val children11 = new JSONObject()
      children11.put("name", featureRange)
      children11.put("type", "range")
      var nums = 0
      val children12 = new JSONObject()
      val children13 = new JSONObject()
      for (j <- 0 until labelSum) {
        val labelled = labelArray(j)
        val nums = featureRangeCount.getOrDefault((i + "_" + labelled), 0)
        children13.put(nf.format(labelArray(j)), nums)
      }

      children12.put("labelcount", children13)
      children11.put("attrs", children12)
      child1Arrays(i) = children11
    }
    children1.put("children", child1Arrays)
    val child1ArraysLabel: Array[JSONObject] = Array {
      children1
    }
    children.put("children", child1ArraysLabel)
    children
  }


  /**
    * 分类预测结果可视化中label信息，统一成json
    */
  def statisticsLabelNumber(labelsReducekey: RDD[(Double, Int)],calssLabel:Array[Double],LabelSum:Int): JSONObject = {
    val child1Arrays: Array[JSONObject] = new Array[JSONObject](LabelSum)
    val nf = NumberFormat.getInstance()

    //每一列的json对象
    val labelJson = new JSONObject
    labelJson.put("name", "label")
    labelJson.put("type", "label")

    val children1 = new JSONObject
    children1.put("name", "label")
    children1.put("type", "class")
    val children12 = new JSONObject
    children12.put("classnum", LabelSum)
    children1.put("attrs", children12)

    for (i <- 0 until LabelSum) {
      val children13 = new JSONObject
      val name = calssLabel(i)
      children13.put("name", nf.format(name))
      children13.put("label", nf.format(name))
      children13.put("type", "class")
      val children14 = new JSONObject
      children14.put("count", labelsReducekey.filter(_._1 == calssLabel(i)).first()._2)
      children13.put("attrs", children14)
      child1Arrays(i) = children13
    }
    children1.put("children", child1Arrays)
    val child1ArraysLabel: Array[JSONObject] = Array {
      children1
    }
    labelJson.put("children", child1ArraysLabel)
    labelJson
  }

  /**
    * 分类预测结果可视化，统一成json
    */
  def classificationVisualResult(hc: HiveContext,operatorName: String,dfWithKey: DataFrame): String = {
    //标签默认位置在第一列
    val df =dfWithKey.drop("hashkey").cache()
    val labelDf = df.select("predictResult")
    val colNameArray: Array[String] = df.columns

    val classificationVisualJson = new JSONObject
    classificationVisualJson.put("name",operatorName )
    classificationVisualJson.put("type", "classification")
    val predictTitleJson = new JSONObject
    predictTitleJson.put("titles", colNameArray.slice(0,colNameArray.length-1))
    classificationVisualJson.put("attrs", predictTitleJson)



    //每一列特征的子节点
    val child1Arrays: ArrayBuffer[JSONObject] = new ArrayBuffer[JSONObject]()
    labelDf.registerTempTable("labledf")
    val labelsum = hc.sql("select count(*),predictResult from labledf group by predictResult")
    val lableAndCount= labelsum.map(row => (row.get(1).toString.toDouble,row.get(0).toString.toDouble.toInt))
    val labelArray = lableAndCount.map(_._1).collect().sorted
    val labelSum = labelArray.size
    val childerdJson = statisticsLabelNumber(lableAndCount,labelArray,labelSum)
    child1Arrays += childerdJson

    colNameArray.filter(_ != "predictResult").foreach(col => {
      val splits: Array[Double] = df.select(col).map { case Row(value: Double) => value }.histogram(3)._1
      if (splits.length > 2) {
        val bucketizer = new Bucketizer().setInputCol(col).setOutputCol("bucketed_" + col).setSplits(splits)
        val bucketedData = bucketizer.transform(df)
        bucketedData.registerTempTable("bucketed")
        val sum = hc.sql("select count(*), predictResult,`bucketed_" + col + "` from bucketed group by predictResult,`bucketed_" + col + "`")
        val sortedByFeature = sum.sort("bucketed_" + col)
        val childColJosn = LabelAndFeatureDfToJsonobject(hc,labelSum,labelArray,sortedByFeature, splits, col)
        child1Arrays += childColJosn
      }
    })

    //样例数据{name:"企业金融危机预警模型应用结果",type:"statistics",attrs:{titles:["label","行业风险","管理风险","财务弹性","信用","竞争力","经营风险"]},children:[{name:"label",type:"label",attrs:null,children:[{name:"label",type:"class",attrs:{classnum:2},children:[{name:"0",type:"class",attrs:{count:8}},{name:"1",type:"class",attrs:{count:13}}]}]},{name:"行业风险",type:"feature",attrs:null,children:[{name:"",type:"range",attrs:{rangenum:10},children:[{name:"0-1",type:"range",attrs:{labelcount:{0:4,1:2}},},{name:"1-2",type:"range",attrs:{labelcount:{0:0,1:3}}},{name:"2以上",type:"range",attrs:{labelcount:{0:4,1:8}}}]}]}}]}
    classificationVisualJson.put("children", child1Arrays.toArray)
    classificationVisualJson.toJSONString

  }

  /**
    * 分类预测结果中label信息，统一成json
    */
  def statisticsFeatureLabelNum(sc: SparkContext, labelIndex: Int, data: RDD[String]): JSONObject = {
    //如果数据有（）包括就去掉
    val dataTmp = data.map { t =>
      if (t.contains("(") || t.contains(")")) {
        t.substring(1, t.length - 1)
      } else {
        t
      }
    }

    //格式化数据
    val nf = NumberFormat.getInstance()

    //取得每一列特征数据
    val labels = dataTmp.map { t =>
      val array = t.split(",")
      nf.format(array(labelIndex).toDouble)
    }

    //标签分类
    val calssNums = labels.distinct().collect().sorted
    val LabelSum = calssNums.size

    val labelsReducekey = labels.map((_, 1)).reduceByKey(_ + _)
    labelsReducekey.foreach(println)
    val child1Arrays: Array[JSONObject] = new Array[JSONObject](LabelSum)

    //每一列的json对象
    val labelJson = new JSONObject
    labelJson.put("name", "label")
    labelJson.put("type", "label")

    val children1 = new JSONObject
    children1.put("name", "label")
    children1.put("type", "class")
    val children12 = new JSONObject
    children12.put("classnum", LabelSum)
    children1.put("attrs", children12)

    for (i <- 0 until calssNums.size) {
      val children13 = new JSONObject
      val name = calssNums(i)
      children13.put("name", name)
      children13.put("label", name)
      children13.put("type", "class")
      val children14 = new JSONObject
      children14.put("count", labelsReducekey.filter(_._1 == calssNums(i)).first()._2)
      children13.put("attrs", children14)
      child1Arrays(i) = children13
    }
    children1.put("children", child1Arrays)
    val child1ArraysLabel: Array[JSONObject] = Array {
      children1
    }
    labelJson.put("children", child1ArraysLabel)
    labelJson
  }

  /**
    * 分类预测结果中统计预测结果的特征个数，转化为统一json（8.19）
    */
  def statisticsFeatureNum(sc: SparkContext, labelIndex: Int, featureIndex: Int, featrueName: String, data: RDD[String]): JSONObject = {
    //展示最大的区间个数
    val maxFeature = 10

    //如果数据有（）包括就去掉
    val dataTmp = data.map { t =>
      if (t.contains("(") || t.contains(")")) {
        t.substring(1, t.length - 1)
      } else {
        t
      }
    }

    //格式化数据
    val nf = NumberFormat.getInstance()

    //取得每一列特征数据
    val labelAndFeature = dataTmp.map { t =>
      val array = t.split(",")
      //      (array(labelIndex).substring(1, array(labelIndex).length), array(featureIndex).toInt)
      (array(labelIndex).toDouble, array(featureIndex).toDouble)
    }

    //标签分类
    val labelArray = labelAndFeature.map(_._1).distinct().collect().sorted
    val LabelSum = labelArray.size

    //特征分类
    val featureCount = labelAndFeature.map(_._2).distinct().count()
    val featureSum = if (featureCount >= maxFeature) maxFeature else featureCount.toInt

    //每一列的json对象
    val children = new JSONObject
    children.put("name", featrueName)
    children.put("type", "feature")
    //本列的区间数组
    val children1 = new JSONObject()
    children1.put("name", "")
    children1.put("type", "range")
    val childrenAttrs = new JSONObject()
    childrenAttrs.put("rangenum", "10")
    children1.put("attrs", childrenAttrs)

    val child1Arrays: Array[JSONObject] = new Array[JSONObject](featureSum)

    //获得最大值最小值
    val maxNum = labelAndFeature.map(_._2).max().toDouble
    val minNum = labelAndFeature.map(_._2).min().toDouble
    //      val mean = (maxNum-minNum)/featureSum

    //获取区间数据
    val featurePieces = pieceByNum(maxNum, minNum, featureSum)

    //保存结果的数组
    def featureNums(labelIndex: Int, featureIndex: Int): Long = {
      var featureNum: Long = 0L
      if (featureIndex != featureSum - 1) {
        featureNum = labelAndFeature.filter(
          t => (t._2 >= featurePieces.apply(featureIndex) && t._2 < featurePieces.apply(featureIndex + 1)) && t._1.equals(labelArray(labelIndex))).count()
      } else {
        featureNum = labelAndFeature.filter(
          t => (t._2 >= featurePieces.apply(featureIndex)) && t._1.equals(labelArray(labelIndex))).count()
      }
      featureNum
    }

    //保存结果的区间
    def featureRegion(featureIndex: Int): String = {
      var region = ""
      val nf = NumberFormat.getInstance()
      if (featureIndex == featureSum - 1) {
        //只保留3位小数，如果是3.000就转换为3
        region = nf.format("%1.3f".format(featurePieces.apply(featureIndex)).toDouble) + "以上"
      } else if (featureIndex < featureSum - 1) {
        region = nf.format("%1.3f".format(featurePieces.apply(featureIndex)).toDouble) + "-" + nf.format("%1.3f".format(featurePieces.apply(featureIndex + 1)).toDouble)
      }
      region
    }

    for (i <- 0 until featureSum) {
      val featureRange = featureRegion(i)
      val children11 = new JSONObject()
      children11.put("name", featureRange)
      children11.put("type", "range")

      val children12 = new JSONObject()
      val children13 = new JSONObject()
      for (j <- 0 until LabelSum) {
        val nums = featureNums(j, i)

        children13.put(nf.format(labelArray(j)), nums)
      }
      children12.put("labelcount", children13)
      children11.put("attrs", children12)
      child1Arrays(i) = children11
    }
    children1.put("children", child1Arrays)
    val child1ArraysLabel: Array[JSONObject] = Array {
      children1
    }
    children.put("children", child1ArraysLabel)
    children
  }

  /**
    * 根据最大值最小值划分区间
    */
  def pieceByNum(max: Double, min: Double, piece: Int): List[Double] = {
    println("最大值：" + max, "最小值：" + min)
    var pieceNum = 0.0
    if (min <= 0) {
      pieceNum = (max + min) / (piece - 1)
    } else {
      pieceNum = (max - min) / (piece - 1)
    }
    var piectList = List(min)
    for (index <- 1 until (piece - 1)) {
      piectList = piectList :+ (min + pieceNum * index).toDouble
    }
    piectList = piectList :+ max
    piectList
  }

  /**
    * 回归预测结果中分区后，统一为Json格式
    */
  def statisticsRegressionResult(dfWithHashkey: DataFrame) = {
    val dataPredictResult = dfWithHashkey.select("predictResult").map{ Row => Row.get(0).toString.toDouble }
    var bucketCount = 0
    val maxFeature = 10
    val numPredictUniqueReuslt = dataPredictResult.distinct.count().toInt
    if (numPredictUniqueReuslt < maxFeature)
      bucketCount = numPredictUniqueReuslt
    else bucketCount = maxFeature

    val reusltNumRegionAndCount: (Array[Double], Array[Long]) = dataPredictResult.histogram(bucketCount)
    val childrenArrays: Array[JSONObject] = new Array[JSONObject](bucketCount)
    for (i <- 0 until bucketCount) {
      val json = new JSONObject
      json.put("name", region(i, reusltNumRegionAndCount))
      json.put("type", "range")
      val jsonattrs = new JSONObject
      jsonattrs.put("labelcount", reusltNumRegionAndCount._2(i).toInt)
      json.put("attrs", jsonattrs)
      childrenArrays(i) = json
    }
    childrenArrays
  }

  //保存回归结果的区间
  def region(resultIndex: Int, ReusltNumRegionAndCount: (Array[Double], Array[Long])) = {
    //只保留3位小数，如果是3.000就转换为3
    ReusltNumRegionAndCount._1(resultIndex).formatted("%.3f").toDouble + "-" + ReusltNumRegionAndCount._1(resultIndex + 1).formatted("%.3f").toDouble
  }

  /**
    * 如果路径存在就删除
    *
    * @param path
    * @return
    */
  def deletePath(path: String) = {
    //删除路径
    val hdfs: FileSystem = HDFSUtil.getFileSystem
    if (hdfs != null) {
      if (hdfs.exists(new Path(path))) {
        hdfs.delete(new Path(path), true)
      }
    }
  }

}

  object MLOperatorUtil {

    def regressionVisualResult(operatorName:String, dfWithHashkey: DataFrame):String ={
      new MLOperatorUtil().regressionVisualResult(operatorName,dfWithHashkey)
    }

    def classificationVisualResult(hc: HiveContext,operatorName: String,dfWithKey: DataFrame): String ={
      new MLOperatorUtil().classificationVisualResult(hc,operatorName,dfWithKey)
    }
    def LabelAndFeatureDfToJsonobject(hc: HiveContext, labelSum:Int,labelArray:Array[Double],df: DataFrame, split: Array[Double], featureName: String): JSONObject ={
      new MLOperatorUtil().LabelAndFeatureDfToJsonobject(hc,labelSum,labelArray,df,split,featureName)
    }

    def statisticsLabelNumber(labelsReducekey: RDD[(Double, Int)],calssLabel:Array[Double],LabelSum:Int): JSONObject={
     new MLOperatorUtil().statisticsLabelNumber(labelsReducekey,calssLabel,LabelSum)
    }

    def deletePath(path: String) = {
      new MLOperatorUtil().deletePath(path)
    }

    def getClassificationTrainsplit(data: RDD[LabeledPoint], sampleSplit: Double): (RDD[LabeledPoint], RDD[LabeledPoint], Int) = {
      new MLOperatorUtil().getClassificationTrainsplit(data, sampleSplit)
    }

    def getRegressionTrainsplit(data: RDD[LabeledPoint], sampleSplit: Double): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
      new MLOperatorUtil().getRegressionTrainsplit(data, sampleSplit)
    }

    def saveMetricsResult(metricsType: String, predictionAndLabels: RDD[(Double, Double)], processId: Long, nodeVertexInstanceId: String, noUse:Boolean):String = {
      new MLOperatorUtil().saveMetricsResult(metricsType, predictionAndLabels, processId, nodeVertexInstanceId ,noUse)
    }

    def saveSpecificMetricResult(metricsName: String, metricsResult: Double, processId: Long, nodeVertexInstanceId: String, noUse:Boolean) = {
      new MLOperatorUtil().saveSpecificMetricResult(metricsName, metricsResult, processId, nodeVertexInstanceId, noUse)
    }

    def evaluateClassificationAlg(processId: Long, nodeVertexInstanceId: String, predictionAndLabels: RDD[(Double, Double)], numClasses: Int, noUse:Boolean) = {
      new MLOperatorUtil().evaluateClassificationAlg(processId, nodeVertexInstanceId, predictionAndLabels, numClasses, noUse)
    }

  }


