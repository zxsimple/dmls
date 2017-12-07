package com.zxsimple.dmls.util

import com.zxsimple.dmls.common.metadata.dao.impl.NodeVertexDaoImpl
import com.zxsimple.dmls.common.metadata.enums.Status

/**
  * Created by zxsimple on 2016/11/1.
  */
class OperatorUtil {

  /**
    * 在process中的NodeVertex的状态
    */
  def postNodeVertexStatus(processId: Long,status: Status, instanceId: String): Unit = {
    val NodeVertex = new NodeVertexDaoImpl()
    val nodevertex  = NodeVertex.findByProcessIdAndInstanceId(processId,instanceId)
    nodevertex.setStatus(status)
    NodeVertex.update(nodevertex)
  }
}

object OperatorUtil {
  def postNodeVertexStatus(processId: Long, status: Status, instanceId: String): Unit = {
    new OperatorUtil().postNodeVertexStatus(processId, status,instanceId)
  }
}


