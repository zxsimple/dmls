package com.zxsimple.dmls.operator.datasource

import java.util
import java.util.Properties

import com.zxsimple.dmls.common.metadata.dao.impl.ImportedDatasetDaoImpl
import com.zxsimple.dmls.common.metadata.model.{ImportedDataset, NodeVertex}
import com.zxsimple.dmls.operator.OperatorTemplate
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zxsimple on 2016/10/26.
  */
class LoadOperator extends OperatorTemplate {

  override def validate(properties: Properties): (Boolean, String) = {
    (true, null)
  }

  override def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame): Unit = {

    val properties: util.Hashtable[AnyRef, AnyRef] = currentNode.getProperties
    val datasetId = properties.get("datasetId").toString.toLong
    val datasetDao = new ImportedDatasetDaoImpl
    val importedDataset: ImportedDataset = datasetDao.find(datasetId)
    val hiveDatabase: String = importedDataset.getHiveDatabase
    val hiveTableName = importedDataset.getHiveTableName
    val colNamesList = importedDataset.getColNames.split(",")


    val sql = "select * from " + hiveDatabase + "." + hiveTableName
    println("====================SQL语句为：" + sql)
    val df = hc.sql(sql)
    df.show

    val schemaList: List[StructField] = df.schema.toList
    val newSchemaList = new ArrayBuffer[StructField]()
    //hashkey
    newSchemaList.+=(StructField("hashkey", LongType, true))
    for (i <- 0 until schemaList.length) {
      newSchemaList.+=(StructField(colNamesList(i), schemaList(i).dataType, schemaList(i).nullable)) //.+= StructField("a",schemaList(i).dataType,schemaList(i).nullable)
    }

    val schema = StructType(newSchemaList)

    val indexedRDD = df.rdd.zipWithIndex.map { case (v, i) => Row.fromSeq(Seq(i + 1) ++ v.toSeq) }
    indexedRDD.foreach(println)

    val result = hc.createDataFrame(indexedRDD, schema)

    result.show()

    result.persist(StorageLevel.MEMORY_AND_DISK_SER)

    currentNode.setOutPortOneData(result)
  }
}
