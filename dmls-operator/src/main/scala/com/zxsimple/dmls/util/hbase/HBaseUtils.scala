package com.zxsimple.dmls.util.hbase

import com.zxsimple.dmls.common.config.SystemConfig
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by zxsimple on 2016/8/9.
  */
class HBaseUtils {

  def getConnection: Connection = {
    val myConf = HBaseConfiguration.create()
    myConf.set("hbase.zookeeper.quorum", SystemConfig.getHbaseZooQuorum)
    myConf.set("hbase.zookeeper.property.clientPort", SystemConfig.getHbaseZooClientPort)
    myConf.set("hbase.defaults.for.version.skip", "true")
    myConf.set("zookeeper.znode.parent", SystemConfig.getHbaseZooZnodeParent)
    val connection = ConnectionFactory.createConnection(myConf);
    connection
  }

  //判断hbase表是否存在
  def tableIsExist(tableName: String): Boolean = {
    val admin = getConnection.getAdmin
    val table = TableName.valueOf(tableName)
    if (admin.tableExists(table)) {
      true
    } else {
      false
    }
  }

  //创建hbase表
  def createTable(tableName: String, arrayFamily: Array[String]) = {
    val admin = getConnection.getAdmin
    val tableDescr = new HTableDescriptor(TableName.valueOf(tableName))
    arrayFamily.foreach{ familyName =>
      tableDescr.addFamily(new HColumnDescriptor(familyName.getBytes))
    }
    admin.createTable(tableDescr)
  }

  //删除hbase表
  def deleteTable(tableName: String) = {
    val admin = getConnection.getAdmin
    val table = TableName.valueOf(tableName)
    if (admin.tableExists(table)) {
      admin.disableTable(table)
      admin.deleteTable(table)
    }
  }

  //删除hbase的Family
  def deleteTableFamilys(tableName: String,arrayFamily: Array[String]) = {
    val admin = getConnection.getAdmin
    val table = TableName.valueOf(tableName)
    admin.disableTable(table)
    val newtd = admin.getTableDescriptor(table)
    arrayFamily.foreach{ familyName =>
      newtd.removeFamily(familyName.getBytes)
    }
    admin.modifyTable(table,newtd)
    admin.enableTable(table)
  }

  //添加hbase的Family
  def addTableFamilys(tableName: String,arrayFamily: Array[String]) = {
    val admin = getConnection.getAdmin
    val table = TableName.valueOf(tableName)
    admin.disableTable(table)
    val newtd = admin.getTableDescriptor(table)
    arrayFamily.foreach{ familyName =>
      val newhcd =  new  HColumnDescriptor(familyName);
      newtd.addFamily(newhcd)
    }
    admin.modifyTable(table,newtd)
    admin.enableTable(table)
  }

  //插入单条数据
  def putSingleValue(tableName: String, rowKey: String, familyName: String, qualifiers: String, value: String) = {
    val myTable = getConnection.getTable(TableName.valueOf(tableName));
    val p = new Put(rowKey.getBytes)
    p.addColumn(familyName.getBytes, qualifiers.getBytes, value.getBytes)
    myTable.put(p)
  }

  //根据rowKey插入批量数据
  def putValue(tableName: String, rowKey: String, family: String, qualifierValue: Array[(String, String)]) {
    val myTable = getConnection.getTable(TableName.valueOf(tableName));
    val new_row = new Put(Bytes.toBytes(rowKey))
    qualifierValue.map(x => {
      var column = x._1
      val value = x._2
      val tt = column.split("\\.")
      if (tt.length == 2) column = tt(1)
      if (!(value.isEmpty))
        new_row.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
    })
    myTable.put(new_row)
  }

  //查询单条数据
  def getSingleValue(tableName: String, rowKey: String, familyName: String, qualifiers: String) = {
    val myTable = getConnection.getTable(TableName.valueOf(tableName));
    val get = new Get(rowKey.getBytes)
    val result = myTable.get(get)
    val testValue = result.getValue(familyName.getBytes, qualifiers.getBytes)
    Bytes.toString(testValue)
  }

  //查询指定列族批量数据 默认显示10条
  def scanValueByLimit(tableName: String, familyName: String, colNames:String, limit: Long) = {
    val myTable = getConnection.getTable(TableName.valueOf(tableName))
    val scanResult = ArrayBuffer[String]()
    val s = new Scan()
    s.setFilter(new PageFilter(limit))
    val scanner = myTable.getScanner(s)
    val qualifiers: Array[String] =colNames.split(",")
    try {
      val r = scanner.iterator()
      while (r.hasNext) {
        val result = r.next()
        //确保随机十条每条数据都有值
        if (!(result.getValue(Bytes.toBytes("feature"), Bytes.toBytes("predictResult")) == null)){
            scanResult += qualifiers.map(c =>
            Bytes.toString(result.getValue(Bytes.toBytes("feature"), Bytes.toBytes(c)))).mkString(",")
        }
      }
      import scala.collection.JavaConverters._
      val length = scanResult.length
      if(scanResult.length <= 10){
        scanResult.toList.asJava
      }else{
        //随机循环取是个数
        var resultList:List[Int]=Nil
        while(resultList.length < 10 ){
          val randomNum=(new Random).nextInt(length)
          if(!resultList.exists(s=>s==randomNum)){
            resultList=resultList:::List(randomNum)
          }
        }
        val scanResultNew = ArrayBuffer[String]()
        for (elem <- resultList) {
          scanResultNew += scanResult(elem)
        }
        scanResultNew.toList.asJava
      }
    } finally {
      //确保scanner关闭
      scanner.close()
    }
  }

  //查询指定列族的名称  //Col_2,Col_3,Col_4,Col_2
  def getColName(tableName: String): String = {
    val myTable = getConnection.getTable(TableName.valueOf(tableName))
    val s = new Scan()
    val scanner = myTable.getScanner(s)
    //hbase表的列名
    var cols = ""
    try {
      val result = scanner.next()
      result.raw().foreach { kv =>
        cols = cols + Bytes.toString(kv.getQualifier()) +","
      }
    } finally {
      //确保scanner关闭
      scanner.close()
    }
    cols.substring(0,cols.length-1)
  }

  //查询指定列族 指定开始startRowkey和stopRowkey
  def scanValueStartStop(tableName: String, familyName: String, colNames:String, startRowkey: String, stopRowkey: String) = {
    val myTable = getConnection.getTable(TableName.valueOf(tableName))
    val scanResult = ArrayBuffer[String]()
    val s = new Scan()
    val qualifiers: Array[String] =colNames.split(",")
    s.setStartRow(Bytes.toBytes(startRowkey))
    s.setStopRow(Bytes.toBytes(stopRowkey))
    val scanner = myTable.getScanner(s)
    try {
      val r = scanner.iterator()
      while (r.hasNext) {
        val result = r.next()
        scanResult += qualifiers.map(c =>
          Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(c)))).mkString(",")
      }
      import scala.collection.JavaConverters._
      scanResult.toList.asJava
    } finally {
      //确保scanner关闭
      scanner.close()
    }
  }

  //获取列族数据
  def getValue(tableName: String, rowKey: String, family: String, qualifiers: Array[String]): Array[(String, String)] = {
    var result: AnyRef = null
    val myTable = getConnection.getTable(TableName.valueOf(tableName));
    val row1 = new Get(Bytes.toBytes(rowKey))
    val HBaseRow = myTable.get(row1)
    if (HBaseRow != null && !HBaseRow.isEmpty) {
      result = qualifiers.map(c => {
        (tableName + "." + c, Bytes.toString(HBaseRow.getValue(Bytes.toBytes(family), Bytes.toBytes(c))))
      })
    }
    else {
      result = qualifiers.map(c => {
        (tableName + "." + c, "null")
      })
    }
    result.asInstanceOf[Array[(String, String)]]
  }

  //删除单条数据
  def deleteValue(tableName: String, rowKey: String, familyName: String, qualifiers: String) = {
    val myTable = getConnection.getTable(TableName.valueOf(tableName));
    val d = new Delete(rowKey.getBytes)
    d.addColumn(familyName.getBytes, familyName.getBytes)
    myTable.delete(d)
  }
}

object HBaseUtils{
  //判断hbase表是否存在
  def tableIsExist(tableName: String): Boolean = {
    new HBaseUtils().tableIsExist(tableName)
  }

  //创建hbase表
  def createTable(tableName: String, arrayFamily: Array[String]) = {
    new HBaseUtils().createTable(tableName,arrayFamily)
  }

  //删除hbase表
  def deleteTable(tableName: String) = {
    new HBaseUtils().deleteTable(tableName)
  }

  //删除hbase的Family
  def deleteTableFamilys(tableName: String,arrayFamily: Array[String]) = {
    new HBaseUtils().deleteTableFamilys(tableName,arrayFamily)
  }

  //添加hbase的Family
  def addTableFamilys(tableName: String,arrayFamily: Array[String]) = {
    new HBaseUtils().addTableFamilys(tableName,arrayFamily)
  }

  //插入单条数据
  def putSingleValue(tableName: String, rowKey: String, familyName: String, qualifiers: String, value: String) = {
    new HBaseUtils().putSingleValue(tableName,rowKey,familyName,qualifiers,value)
  }

  //根据rowKey插入批量数据
  def putValue(tableName: String, rowKey: String, family: String, qualifierValue: Array[(String, String)]) = {
    new HBaseUtils().putValue(tableName,rowKey,family,qualifierValue)
  }

  //查询单条数据
  def getSingleValue(tableName: String, rowKey: String, familyName: String, qualifiers: String) = {
    new HBaseUtils().getSingleValue(tableName, rowKey, familyName, qualifiers)
  }
  //查询批量数据
  def scanValueByLimit(tableName: String, familyName: String, colNames:String, limit: Long) = {
    new HBaseUtils().scanValueByLimit(tableName,familyName,colNames,limit)
  }

  //查询指定列族 指定开始startRowkey和stopRowkey
  def scanValueStartStop(tableName: String, familyName: String, colNames:String, startRowkey: String, stopRowkey: String) = {
    new HBaseUtils().scanValueStartStop(tableName, familyName, colNames, startRowkey, stopRowkey)
  }

  //获取列族数据
  def getValue(tableName: String, rowKey: String, family: String, qualifiers: Array[String]): Array[(String, String)] = {
    new HBaseUtils().getValue(tableName, rowKey, family, qualifiers)
  }

  //删除单条数据
  def deleteValue(tableName: String, rowKey: String, familyName: String, qualifiers: String) = {
    new HBaseUtils().deleteValue(tableName, rowKey, familyName, qualifiers)
  }
}

