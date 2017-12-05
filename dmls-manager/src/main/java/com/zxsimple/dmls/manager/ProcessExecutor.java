package com.zxsimple.dmls.manager;

import com.zxsimple.dmls.common.exception.DaoException;
import com.zxsimple.dmls.common.exception.ScheduleException;
import com.zxsimple.dmls.common.metadata.dao.ProcessDao;
import com.zxsimple.dmls.common.metadata.dao.impl.ProcessDaoImpl;
import com.zxsimple.dmls.common.metadata.enums.Status;
import com.zxsimple.dmls.common.metadata.model.NodeEdge;
import com.zxsimple.dmls.common.metadata.model.NodeVertex;
import com.zxsimple.dmls.common.metadata.model.Process;
import com.zxsimple.dmls.manager.dag.DAGScheduler;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.util.List;

/**
 * Created by Sanders on 2017/10/20.
 */
public class ProcessExecutor {

    private static Logger logger = Logger.getLogger(ProcessExecutor.class);

    ProcessDao processDao = new ProcessDaoImpl();

    public void executeProcess(long processId) {

        Process process = null;
        try {

            // 根据processId获取对应process的节点和边界

            process = processDao.find(processId);
            List<NodeEdge> edges = process.getNodeEdges();
            List<NodeVertex> nodeVertexs = process.getNodeVertexs();

            // 构建DAG拓扑结构
            DAGScheduler scheduler = new DAGScheduler();
            scheduler.buildToplogy(nodeVertexs, edges);
            // scheduler.buildToplogy(MockData.getNodes(),MockData.getEdges());

            if (!scheduler.compile()) {
                return;
            }

            SparkConf conf = new SparkConf()
                    .setAppName("Data_Pipeline_Job");

            SparkContext sc = new SparkContext(conf);
            updateProcessStatus(process, Status.RUNNING, null);
            scheduler.executeDag(sc);
            updateProcessStatus(process, Status.SUCCEED, null);

        } catch (Exception e) {
            try {
                updateProcessStatus(process, Status.FAILED, e);
            } catch (DaoException e1) {
                logger.error("ProcessExecutor error : ", e);
            }
            logger.error("ProcessExecutor error : ", e);
        }
    }

    private void updateProcessStatus(Process process, Status status, Exception ex) throws DaoException {
        process.setStatus(status);

        processDao.update(process);
        // TODO
        // report error message

    }


    public static void main(String[] args) throws ScheduleException {

        if (args.length != 1)
            throw new ScheduleException("");
        new ProcessExecutor().executeProcess(Long.parseLong(args[0]));
    }
}
