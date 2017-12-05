package com.zxsimple.dmls.operator

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.zxsimple.dmls.common.exception.ServiceException
import com.zxsimple.dmls.common.metadata.dao.impl.NodeVertexDaoImpl
import com.zxsimple.dmls.common.metadata.enums.Status
import com.zxsimple.dmls.common.metadata.model.NodeVertex
import com.zxsimple.dmls.common.util.hbaseUtils.HBaseBean.HBaseByDataframeUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer


/**
  * Created by Sanders on 2016/9/9.
  */
abstract class OperatorTemplate {
  val nodeVertexDao = new NodeVertexDaoImpl


  def execute(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame) = {
    try {
      // readData(hc, currentNode, parentNode)
      updateOperatorStatus(currentNode, Status.RUNNING, null)
      validate(currentNode.getProperties)
      runTask(hc, currentNode, inputPortOneData, inputPortTwoData)
      val data: DataFrame = updateMetaData(hc, currentNode)
      if(data!=null){
        val instanceId = currentNode.getInstanceId
        takeSample(hc, data, instanceId)
      }
      updateOperatorStatus(currentNode, Status.SUCCEED, null)

    } catch {
      case ex: Exception =>
        //出异常,更新总状态为失败
        updateOperatorStatus(currentNode, Status.FAILED, ex)
        throw new ServiceException(ex)
    }
  }

  def readData(hiveContext: HiveContext, currentNode: NodeVertex, parentNode: NodeVertex) = {

    /*if(currentNode.getEtlTask.getEtlTaskName != ETLTaskName.Load) {
      val parentTask = parentNode.getEtlTask
      val df = parentTask.getData
      currentNode.getEtlTask.setData(df)
    }*/
  }

  /**
    * 验证参数
    *
    * @param properties
    */
  def validate(properties: Properties): (Boolean, String)

  /**
    * Abstract method to be implemented in Concrete Class to handle the internal logic
    *
    * @param currentNode
    *
    */
  def runTask(hc: HiveContext, currentNode: NodeVertex, inputPortOneData: DataFrame, inputPortTwoData: DataFrame)

  /*def takeSample(hc: HiveContext, currentNode: Node): Unit = {
    TaskPlugins.saveSampleData(hc, currentNode)
  }*/

  def updateMetaData(sc: HiveContext, currentNode: NodeVertex) = {
    var data: DataFrame = null
    val oneData = currentNode.getOutPortOneData
    val twoData = currentNode.getOutPortTwoData
    if (oneData != null || twoData != null) {
      if (oneData != null) {
        data = oneData
      } else if (twoData != null) {
        data = twoData
      }

    val colAndType: Array[(String, String)] = data.dtypes
    val obj = new JSONObject()
    for(coltypes<-colAndType){
      obj.put(coltypes._1,coltypes._2)
    }
    val jsonStr = obj.toJSONString
      currentNode.setColumnsAndTypeStr(jsonStr)
      nodeVertexDao.update(currentNode)
    }

    data

  }

  /**
    * 保存中间数据
    *
    * @param hc
    * @return
    */
  def takeSample(hc: HiveContext, data: DataFrame, instanceId: String) = {

    val sampleData = data.limit(100)
    val count = data.count

    val rowKeyBuffer = new ArrayBuffer[String]()
    for (i <- 0 until count.toInt) {
      val row = instanceId + String.format("%02d", Integer.valueOf(i))
      rowKeyBuffer += row
    }
    val rowKeyIterator = rowKeyBuffer.toSeq.toIterator

    val rowKeyRDDRow = data.rdd.map(row => {
      Row.fromSeq(Seq(rowKeyIterator.next) ++ row.toSeq)
    })

    val sampleRDD = rowKeyRDDRow.map(row => {
      row.mkString(",").toString
    })

    val rowKeyColName = "key," + data.columns.mkString(",")
    val hbaseUtil = new HBaseByDataframeUtils;
    hbaseUtil.writeHBaseByHiveContext(hc, sampleRDD, "etl_middle_data", "default", "key", "data", rowKeyColName)
    //    hbaseUtil.writeDF2HBase(sc,middleData,"etl_middle_data","default","key","data", rowKeyColName)

  }


  def updateOperatorStatus(currentNode: NodeVertex, status: Status, ex: Exception): Unit = {

    currentNode.setStatus(status)
    println(currentNode.getOperatorName.toString+"Task总状态为："+status)
    if(ex!=null){
      println("异常信息："+ex)
      val errorStr =ex.toString
      var res:String = null
      if(errorStr.length>500){
        res = errorStr.substring(0,500);
      }else{
        res = errorStr
      }
      currentNode.setLogs(res)
    }
    nodeVertexDao.update(currentNode)
  }

}
