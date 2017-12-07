package com.zxsimple.dmls.manager.util

import java.util
import java.util.regex.Pattern

import com.zxsimple.dmls.common.util.HiveUDF.CreateHash
import com.zxsimple.dmls.model.DFStats
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

/**
  * Created by destra on 2016/8/4.
  */
class TaskUtils {

  /**
    * 将带有rowKey的DataFrom格式的数据转化为 RDD[(String, String, Vector)] ，第一列是rowKey
    *
    * @param df
    * @param separator
    * @return
    */
  def dfWithRowkeyToRDD(df: DataFrame, separator: String) = {

    val dataNew = df.rdd.map(_.mkString(separator)).map { line =>
      val lined = line.split(separator)

      (line, Vectors.dense(lined.slice(1, lined.size).map(_.toDouble)))
    }
    dataNew
  }

  /**
    * rddDOuble to DF
    *
    * @param hc
    * @param rdd
    * @param colList
    * @return
    */
  def rddDoubleToDF(hc: HiveContext, rdd: RDD[Double], colList: List[String]) = {
    val rddRow = rdd.map(line => Row(line))
    val schema =
      StructType(
        colList.map(fieldName => StructField(fieldName, DoubleType, true)))
    hc.createDataFrame(rddRow, schema)
  }


  /**
    * 两个dataframe拼接
    *
    * @param df1
    * @param df2
    * @return
    */
  def df1ZipDF2(hc: HiveContext, df1: DataFrame, df2: DataFrame): DataFrame = {
    val ziprdd = df1.rdd.repartition(1).zip(df2.rdd.repartition(1))
    val rows = ziprdd.map {
      case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)
    }
    val schema = StructType(df1.schema.fields ++ df2.schema.fields)
    hc.createDataFrame(rows, schema).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }


  /**
    * rdd Vertor 转换成DF
    *
    * @param hc
    * @param rdd
    * @param colList
    * @return
    */
  def rddVectorToDF(hc: HiveContext, rdd: RDD[Vector], colList: List[String]): DataFrame = {
    val rddRow = rdd.map(line => {
      Row.fromSeq(line.toArray)
    })
    val schema =
      StructType(
        colList.map(fieldName => StructField(fieldName, DoubleType, true)))
    hc.createDataFrame(rddRow, schema)
  }

  /**
    * 替换空值
    *
    * @param str
    * @return
    */
  def replaceStr(str: String): String = {
    val pattern = Pattern.compile("^[']{1}|[']{1}$");
    val matcher = pattern.matcher(str);
    matcher.replaceAll("");
  }

  /**
    * 创建hash值
    *
    * @param s
    * @return
    */
  def CreateHash(s: String): Int = {
    val createHash = new CreateHash
    createHash.evaluate(s)
  }

  /**
    * 创建DataFrame
    *
    * @param colNamesList
    * @param rDD
    * @param delimiter
    * @param tableName
    * @return
    */
  def createDataFrame(sqlContext: SQLContext, colNamesList: List[String], rDD: RDD[String], delimiter: String, tableName: String): DataFrame = {
    val schema =
      StructType(
        colNamesList.map(fieldName => StructField(fieldName, StringType, true)))
    val seq = rDD.map(s => s.split(delimiter)).map(_.map(f => {
      TaskUtils.replaceStr(f)
    }))
    val rowRDD = seq.map(p =>
      Row.fromSeq(p)
    )
    val dataFrame = sqlContext.createDataFrame(rowRDD, schema)
    dataFrame.registerTempTable(tableName)
    dataFrame
  }

  /**
    * select DataFrame
    *
    * @param sqlContext
    * @param colList
    * @param tableName
    * @return
    */
  def selectedDataFrame(sqlContext: SQLContext, colList: List[String], tableName: String): DataFrame = {
    val featureSql = "select " + colList.mkString(",") + " from " + tableName
    sqlContext.sql(featureSql)
  }

  /**
    * dataFrame 转 RDD
    *
    * @param df
    * @param delimiter
    * @return
    */
  def dfToRDD(df: DataFrame, delimiter: String): RDD[String] = {
    df.rdd.map(row => row.mkString(delimiter))
  }

  /**
    * 输入的RDD[String]转换为RDD[Vector]
    */
  def rddStringToVector(rdd: RDD[String], delimiter: String): RDD[Vector] = {
    rdd.map { line =>
      val data = line.trim.split(delimiter).map(_.toDouble)
      Vectors.dense(data)
    }
  }

  /**
    * 输入的RDD[String]转换为DataFrame
    */
  def rddStringToDF(hc: HiveContext, rdd: RDD[String], colList: List[String], datatype: DataType, delimiter: String): DataFrame = {
    val rddRow = rdd.map(line => Row.fromSeq(line.split(delimiter)))
    val schema = StructType(
      colList.map(fieldName => StructField(fieldName, datatype, true)))
    hc.createDataFrame(rddRow, schema)
  }

  def rddStringToDF(hc: HiveContext, rdd: RDD[String], schema: StructType, delimiter: String): DataFrame = {
    val rddRow = rdd.map(line => Row.fromSeq(line.split(delimiter)))
    hc.createDataFrame(rddRow, schema)
  }

  /**
    * RDD[Vector]转换为RDD[String]
    */
  def rddVectorToString(rdd: RDD[Vector], delimiter: String): RDD[String] = {
    rdd.map(_.toArray.mkString(delimiter))
  }

  /**
    * 两个RDD合并成一个根据分隔符合并成一个
    */
  def rdd1DelimiterRDD2(rdd1: RDD[String], rdd2: RDD[String], delimiter: String): RDD[String] = {
    val rdd1p1 = rdd1.repartition(1)
    val rdd2p1 = rdd2.repartition(1)

    rdd1p1.zipPartitions(rdd2p1) {
      (rdd1Iter, rdd2Iter) => {
        var result = List[String]()
        while (rdd1Iter.hasNext && rdd2Iter.hasNext) {
          result ::= (rdd1Iter.next() + delimiter + rdd2Iter.next())
        }
        result.iterator
      }
    }

    //      .map(line=>{
    //      line._1+delimiter+line._2
    //    })
    //  }
  }

  /**
    * 统计信息
    */
  def describe(df: DataFrame, colNames: List[String]): DFStats = {
    val stats = new DFStats()
    val it = df.describe(colNames: _*).collect().iterator
    while (it.hasNext) {
      val row = it.next()
      val key = row.getString(0)
      var value = new util.ArrayList[java.lang.Double]()
      try {
        for (i <- 1 to colNames.size) {
          value.add(row.getString(i).toDouble)
        }
      } catch {
        case ex: Exception =>
          value.add(0.0)
          println("max = 0.0,min = 0.0,mean = 0.0")
      }
      if (key.equals("max")) {
        stats.setMax(value)
      } else if (key.equals("min")) {
        stats.setMin(value)
      } else if (key.equals("mean")) {
        stats.setMean(value)
      } else if (key.equals("stddev")) {
        stats.setStddev(value)
      } else if (key.equals("count")) {
        stats.setCount(value)
      }
    }
    stats
  }

  /**
    * 将输入的数据RDD[String]转化为RDD[LabelPoint]格式,默认第一行是label¬
    *
    * @return
    */
  def dataToLabeledPoint(data: RDD[String], delimiter: String): RDD[LabeledPoint] = {
    val lablepoint = data.map { t =>
      val array = t.trim.split(delimiter).map(_.toDouble)
      LabeledPoint(array(0), Vectors.dense(array.slice(1, array.size)))
    }
    lablepoint
  }

}

object TaskUtils {

  def dfWithRowkeyToRDD(df: DataFrame, separator: String) = {
    new TaskUtils().dfWithRowkeyToRDD(df, separator)
  }

  def rddDoubleToDF(hc: HiveContext, rate: RDD[Double], colList: List[String]) = {
    new TaskUtils().rddDoubleToDF(hc, rate, colList)
  }

  def df1ZipDF2(hc: HiveContext, df1: DataFrame, df2: DataFrame) = {
    new TaskUtils().df1ZipDF2(hc, df1, df2)
  }


  def replaceStr(str: String): String = {
    new TaskUtils().replaceStr(str)
  }

  def createDataFrame(sqlContext: SQLContext, colNamesList: List[String], rDD: RDD[String], delimiter: String, tableName: String): DataFrame = {
    val dataFrame = new TaskUtils().createDataFrame(sqlContext, colNamesList, rDD, delimiter, tableName)
    dataFrame
  }

  def selectedDataFrame(sqlContext: SQLContext, colList: List[String], tableName: String): DataFrame = {
    new TaskUtils().selectedDataFrame(sqlContext, colList, tableName)
  }

  def dfToRDD(df: DataFrame, delimiter: String): RDD[String] = {
    new TaskUtils().dfToRDD(df, delimiter)
  }

  def selectedDataFrameToRDD(sqlContext: SQLContext, colList: List[String], tableName: String, delimiter: String): RDD[String] = {
    if (colList.size != 0 && colList != null) {
      val df: DataFrame = new TaskUtils().selectedDataFrame(sqlContext, colList, tableName)
      new TaskUtils().dfToRDD(df, delimiter)
    } else {
      null
    }

  }

  def rddStringToVector(rdd: RDD[String], delimiter: String): RDD[Vector] = {
    new TaskUtils().rddStringToVector(rdd, delimiter)
  }

  def rddVectorToString(rdd: RDD[Vector], delimiter: String): RDD[String] = {
    new TaskUtils().rddVectorToString(rdd, delimiter)
  }

  def rddStringToDF(hc: HiveContext, rdd: RDD[String], colList: List[String], datatype: DataType, delimiter: String): DataFrame = {
    new TaskUtils().rddStringToDF(hc, rdd, colList, datatype, delimiter)
  }

  def rddStringToDF(hc: HiveContext, rdd: RDD[String], schema: StructType, delimiter: String): DataFrame = {
    new TaskUtils().rddStringToDF(hc, rdd, schema, delimiter)
  }

  def rddVectorToDF(hc: HiveContext, rdd: RDD[Vector], colList: List[String]): DataFrame = {
    new TaskUtils().rddVectorToDF(hc, rdd, colList)
  }

  def rdd1DelimiterRDD2(rdd1: RDD[String], rdd2: RDD[String], delimiter: String): RDD[String] = {
    if (rdd1 != null) {
      new TaskUtils().rdd1DelimiterRDD2(rdd1, rdd2, delimiter)
    } else {
      rdd2
    }
  }

  def describe(df: DataFrame, colNames: List[String]): DFStats = {
    new TaskUtils().describe(df, colNames)
  }

  def dataToLabeledPoint(data: RDD[String], delimiter: String): RDD[LabeledPoint] = {
    new TaskUtils().dataToLabeledPoint(data, delimiter)
  }

  def CreateHash(s: String): Int = {
    new TaskUtils().CreateHash(s)
  }
}

