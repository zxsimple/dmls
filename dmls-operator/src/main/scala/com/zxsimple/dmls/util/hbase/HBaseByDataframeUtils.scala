package com.zxsimple.dmls.util.hbase

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by zxsimple on 2016/8/3.
  */
class HBaseByDataframeUtils{

  /**
    * 将rowkeyAndPredict保存到Hbase中
    */
  def writeHBase(sc: SparkContext, df: DataFrame, tableName: String, nameSpace: String, rowKey: String, cf: String) = {
    val featrueName = df.columns.mkString(",")
    //生成catalog
    val catalog = catalogCreate(tableName, nameSpace, rowKey, cf, featrueName)
    df.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "3"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def dataFrameCatalogCreate(dataFrame: DataFrame,tableName: String, nameSpace: String, rowKey: String, cf: String): String = {
    val hTable = new HTable()
    hTable.setRowkey(rowKey)

    val table = new Table()
    table.setNamespace("default")
    table.setName(tableName)
    hTable.setTable(table)

    val typeMap = Map("IntegerType" -> "int", "DoubleType" -> "double","StringType"->"string")

    val dtype = dataFrame.dtypes
    val columsMap = new java.util.HashMap[String, Columns]()
    val columsTmp = new Columns("rowkey", dtype(0)._1, typeMap(dtype(0)._2))
    columsMap.put(dtype(0)._1, columsTmp)

    for (i <- 1 until (dtype.length)) {
      val colums = new Columns(cf, dtype(i)._1, typeMap(dtype(i)._2))
      columsMap.put(dtype(i)._1, colums)
    }
    hTable.setColumns(columsMap)
    JSON.toJSON(hTable).toString
  }

  //生成catalog
  def catalogCreate(tableName: String, nameSpace: String, rowKey: String, cf: String, featrue: String): String = {
    val hTable = new HTable()
    hTable.setRowkey(rowKey)

    val table = new Table()
    table.setNamespace("default")
    table.setName(tableName)
    hTable.setTable(table)

    val features = featrue.split(",")
    val columsMap = new java.util.HashMap[String, Columns]()
    val columsTmp = new Columns("rowkey", "key", "string")
    columsMap.put(features(0), columsTmp)

    for (i <- 1 until (features.length)) {
      val colums = new Columns(cf, features(i), "string")
      columsMap.put(features(i), colums)
    }
    hTable.setColumns(columsMap)
    JSON.toJSON(hTable).toString
  }

  /**
    * spark dataframe直接存到hbase
    *
    * @param tableName
    * @param nameSpace
    * @param rowKey
    * @param cf
    * @param featrueName
    */
  def writeHBaseByHiveContext(hc: HiveContext, inputData: RDD[String], tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String):Unit = {

    //生成schema
    val schema = StructType(featrueName.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = inputData.map(line=>{
      //为防止空值出现
      val colSize = featrueName.split(",").size
      val array =new Array[String](colSize)
      val lineSplit = line.split(",")
      val lineSize = lineSplit.size
      if(colSize==lineSize){
        for(i<-0 until colSize) {
          array(i)=lineSplit(i)
        }
      }else{
        for(i<-0 until lineSize) {
          array(i)=lineSplit(i)
        }
        for(i<-lineSize until colSize){
          array(i)=""
        }
      }
      array
    }).map(p => Row.fromSeq(p.toSeq))

    // Apply the schema to the RDD.
    val peopleDataFrame = hc.createDataFrame(rowRDD, schema)

    //生成catalog
    val catalog = catalogCreate(tableName, nameSpace, rowKey, cf, featrueName)
    peopleDataFrame.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "3"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  /**
    * spark集群运用dataframe写HBase
    *
    * @return
    */
  def writeHBase(sc: SparkContext, inputData: String, tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String):Unit = {
    val data = sc.textFile(inputData)
    writeHBase(sc,data,tableName,nameSpace,rowKey,cf,featrueName)
  }

  /**
    * spark集群运用dataframe写HBase
    *
    * @return
    */
  def writeHBase(sc: SparkContext, inputData: RDD[String], tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String):Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //生成schema
    val schema = StructType(featrueName.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = inputData.map(line=>{
      //为防止空值出现
      val colSize = featrueName.split(",").size
      val array =new Array[String](colSize)
      val lineSplit = line.split(",")
      val lineSize = lineSplit.size
      if(colSize==lineSize){
        for(i<-0 until colSize) {
          array(i)=lineSplit(i)
        }
      }else{
        for(i<-0 until lineSize) {
          array(i)=lineSplit(i)
        }
        for(i<-lineSize until colSize){
          array(i)=""
        }
      }
      array
    }).map(p => Row.fromSeq(p.toSeq))

    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    peopleDataFrame.show()
    //生成catalog
    val catalog = catalogCreate(tableName, nameSpace, rowKey, cf, featrueName)
    peopleDataFrame.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "3"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  /**
    * spark集群运用dataframe读HBase
    *
    * @return
    */
  def readHBase(sc: SparkContext,tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String) = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //生成catalog
    val catalog = catalogCreate(tableName, nameSpace, rowKey, cf, featrueName)

    //读数据
    val data = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    data
  }
//未用到
//  def importTxtToHbase(inputData: String, preRowKey:Long, tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String)={
//    //判断表是否存在，存在就删除，不存在就新建
//    if(HBaseUtils.tableIsExist(tableName)){
//        HBaseUtils.deleteTable(tableName)
//    }else{
//      HBaseUtils.createTable(tableName,featrueName.split(","))
//    }
//
//    val conf = new SparkConf()
//    conf.setMaster("local[3]")
//      .setAppName("hbaseImport")
//      .set("spark.hbase.host", "vm-hdp001.stormorai.com,vm-hdp002.stormorai.com,vm-hdp004.stormorai.com")
//    val sc = new SparkContext(conf)
//    val data = sc.textFile(inputData)
//    //给新数据加rowkey
//    val dataNew =  data.zipWithIndex().map(t => rowkeyNew(preRowKey+t._2)+","+t._1)
//    //写入HBase表
//    writeHBase(sc,dataNew,tableName,nameSpace,rowKey,cf,featrueName)
//  }

  def rowkeyNew(data:Long) = MD5Hash.getMD5AsHex(Bytes.toBytes(data)).substring(0, 8)
}

object HBaseByDataframeUtils {

  def writeHBaseByHiveContext(sc: HiveContext, inputData: RDD[String], tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String) = {
    new HBaseByDataframeUtils().writeHBaseByHiveContext(sc,inputData,tableName,nameSpace,rowKey,cf,featrueName)
  }
  def writeHBase(sc: SparkContext, inputData: String, tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String) = {
    new HBaseByDataframeUtils().writeHBase(sc,inputData,tableName,nameSpace,rowKey,cf,featrueName)
  }

  def writeHBase(sc: SparkContext, inputData: RDD[String], tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String) = {
    new HBaseByDataframeUtils().writeHBase(sc,inputData,tableName,nameSpace,rowKey,cf,featrueName)
  }

  def readHBase(sc: SparkContext, tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String) = {
    new HBaseByDataframeUtils().readHBase(sc,tableName,nameSpace,rowKey,cf,featrueName)
  }

  def writeHBase(sc: SparkContext, df: DataFrame, tableName: String, nameSpace: String, rowKey: String, cf: String) = {
    new HBaseByDataframeUtils().writeHBase(sc,df,tableName,nameSpace,rowKey,cf)
  }

//  def importTxtToHbase(inputData: String, preRowKey:Long, tableName: String, nameSpace: String, rowKey: String, cf: String, featrueName: String)={
//    new com.zxsimple.dmls.util.HBaseBean.HBaseByDataframeUtils().importTxtToHbase(inputData, preRowKey, tableName, nameSpace, rowKey, cf, featrueName)
//  }
}
