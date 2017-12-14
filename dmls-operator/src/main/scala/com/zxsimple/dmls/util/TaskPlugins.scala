package com.zxsimple.dmls.util

import com.zxsimple.dmls.common.metadata.dao.impl.ETLTaskDaoImpl
import com.zxsimple.dmls.common.metadata.model.ETLTask
import com.zxsimple.dmls.util.hbase.HBaseByDataframeUtils
import com.zxsimple.dmls.node.Node
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by d\estra on 2016/8/15.
  */
class TaskPlugins {
  val logger = Logger.getLogger(TaskPlugins.getClass)

  /**
    * 更新中间结果数据
    *
    * @param currentNode
    * @param etlTaskDao
    * @return
    */
  def updateMetadata(currentNode: Node, etlTaskDao: ETLTaskDaoImpl) = {
    val currentTask = currentNode.getEtlTask
    etlTaskDao.update(currentTask)
  }


  def saveSampleData(hc:HiveContext, currentNode: Node,etlTaskDao: ETLTaskDaoImpl): Unit = {
    val data = currentNode.getEtlTask.getData
    val sampleData = data.limit(100)
    val rowKeyRDDRow = rowKeyCount(currentNode.getEtlTask,sampleData)
    val sampleRDD = rowKeyRDDRow.map(row=>{
            row.mkString(",").toString
          })
    val rowKeyColName = "key,"+data.columns.mkString(",")
    val hbaseUtil = new HBaseByDataframeUtils;
    hbaseUtil.writeHBaseByHiveContext(hc,sampleRDD,"etl_middle_data","default","key","data", rowKeyColName)
    changeSampleStatus(etlTaskDao,currentNode);
  }

  /**
    * 拼接RowKey到Count
    *       拼RowKey  eltFlowId+etlTaskId+0000~0099
    *                     6位     8位         4位
    */
  def rowKeyCount(etlTask:ETLTask, data : DataFrame): RDD[Row] ={
    val etlFlowIdStr: String = etlTask.getEtlFlowId.toString
    val etlTaskIdStr: String = String.format("%08d",etlTask.getId)
    val rowKeyBuffer = new ArrayBuffer[String]()
      for(i<-0 until 100){
        val row = etlFlowIdStr+etlTaskIdStr+String.format("%04d", Integer.valueOf(i))
        rowKeyBuffer += row
      }
    val rowKeyIterator = rowKeyBuffer.toSeq.toIterator

    data.map(line=>{
      Row.fromSeq(Seq(rowKeyIterator.next) ++ line.toSeq)
    })
//    val hashkeyRDDRow: RDD[Row] = data.rdd
//    hashkeyRDDRow.map(row=>{
//      Row.fromSeq(Seq(rowKeyIterator.next) ++ row.toSeq)
//    })
  }

  def changeSampleStatus(etlTaskDao: ETLTaskDaoImpl, currentNode: Node) = {
    val etlTask = currentNode.getEtlTask
    etlTask.setSampleStatus(1)
    updateMetadata(currentNode,etlTaskDao)
  }

  /**
    * 保存统计数据，并更新统计状态
    *
    * @param etlTaskDao
    * @param currentNode
    */
  def saveStatisticsData(currentNode: Node,statisticsData:String,etlTaskDao: ETLTaskDaoImpl) = {
    val etlTask = currentNode.getEtlTask
    etlTask.setStatisticsData(statisticsData)
    etlTask.setStatisticsStatus(1)
    updateMetadata(currentNode,etlTaskDao)
  }

  def updateTaskStatus(etlTaskDao: ETLTaskDaoImpl,currentNode: Node, status:Int,ex:Exception) = {
    val etlTask = currentNode.getEtlTask
    etlTask.setTaskStatus(status)
    println(etlTask.getEtlTaskName.toString+"Task总状态为："+status)
    if(ex!=null){
      println("异常信息："+ex)
      val errorStr =ex.toString
      var res:String = null
      if(errorStr.length>500){
        res = errorStr.substring(0,500);
      }else{
        res = errorStr
      }
      etlTask.setLog(res)
    }
    updateMetadata(currentNode,etlTaskDao)
  }

}

object TaskPlugins{

  val etlTaskDao = new ETLTaskDaoImpl;

  def updateMetadata(currentNode:Node)={
    new TaskPlugins().updateMetadata(currentNode,etlTaskDao)
  }

  def saveSampleData(hc:HiveContext, currentNode:Node) = {
    new TaskPlugins().saveSampleData(hc,currentNode,etlTaskDao)
  }

//  def saveSampleData(sc:SparkContext, currentNode: Node, data : DataFrame) = {
//    new TaskPlugins().saveSampleData(sc, currentNode, data,etlTaskDao)
//  }

  def saveStatisticsData(currentNode:Node,statisticsData:String) = {
    new TaskPlugins().saveStatisticsData(currentNode,statisticsData,etlTaskDao)
  }

  def updateTaskStatus(currentNode: Node,status:Int,ex:Exception) = {
    new TaskPlugins().updateTaskStatus(etlTaskDao,currentNode,status,ex)
  }
}
