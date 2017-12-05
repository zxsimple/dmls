package com.zxsimple.dmls.manager.dag;

import com.zxsimple.dmls.common.exception.ServiceException;
import com.zxsimple.dmls.common.metadata.enums.OperatorName;
import com.zxsimple.dmls.common.metadata.enums.OperatorType;
import com.zxsimple.dmls.common.metadata.model.NodeEdge;
import com.zxsimple.dmls.common.metadata.model.NodeVertex;
import com.zxsimple.dmls.operator.OperatorTemplate;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by Sanders on 2016/10/19.
 */
public class DAGScheduler {


    private static Logger logger = Logger.getLogger(DAGScheduler.class);

    DirectedAcyclicGraph<NodeVertex, NodeEdge> dag = new DirectedAcyclicGraph(NodeEdge.class);

    public void buildToplogy(List<NodeVertex> nodes, List<NodeEdge> edges) {

        for (NodeVertex vertex : nodes) {
            dag.addVertex(vertex);
        }

        for (NodeEdge edge : edges) {
            try {
                dag.addDagEdge(edge.getFromNode(), edge.getToNode(), edge);
            } catch (DirectedAcyclicGraph.CycleFoundException e) {
                logger.error("Build DAG graph error : ", e);
            }
        }
    }

    public boolean compile() {

        boolean valid = true;
        Iterator<NodeVertex> it = dag.iterator();

        while (it.hasNext()) {

            NodeVertex node = it.next();
            if (!validateNode(node)) {
                valid = false;
                break;
            }
        }

        return valid;
    }


    /**
     * Execute pipeline DAG from the root task
     *
     * @param sc
     * @throws ServiceException
     */
    public void executeDag(SparkContext sc) throws ServiceException {

        Iterator<NodeVertex> it = dag.iterator();
        HiveContext hc = new HiveContext(sc);

        while (it.hasNext()) {

            NodeVertex node = it.next();
            System.out.println("############# Execute " + node.getOperatorName().name() + " #################");
            executeNode(hc, node);
            System.out.println("############# Finish executing " + node.getOperatorName().name() + " #################");
        }
    }

    /**
     * 执行DAG图上的节点
     *
     * @param hc
     * @param node
     */
    private void executeNode(HiveContext hc, NodeVertex node) throws ServiceException {

        // 获取父节点的数据输出作为当前节点的数据输入
        DataFrame inputPortOneData = null;
        DataFrame inputPortTwoData = null;

        Set<NodeEdge> edges = dag.edgesOf(node);

        if(edges != null && edges.size() > 0) {

            for(Object element : edges.toArray()) {
                NodeEdge edge = (NodeEdge) element;
                NodeVertex parent = dag.getEdgeSource(edge);
                if(parent != node) {

                    if (edge.getToPortNum() == NodeEdge.PORT_NUM_ONE && edge.getFromPortNum() == NodeEdge.PORT_NUM_ONE) {
                        inputPortOneData = parent.getOutPortOneData();
                    } else if (edge.getToPortNum() == NodeEdge.PORT_NUM_ONE && edge.getFromPortNum() == NodeEdge.PORT_NUM_TWO) {
                        inputPortOneData = parent.getOutPortTwoData();
                    } else if (edge.getToPortNum() == NodeEdge.PORT_NUM_TWO && edge.getFromPortNum() == NodeEdge.PORT_NUM_ONE) {
                        inputPortTwoData = parent.getOutPortOneData();
                    } else if (edge.getToPortNum() == NodeEdge.PORT_NUM_TWO && edge.getFromPortNum() == NodeEdge.PORT_NUM_TWO) {
                        inputPortTwoData = parent.getOutPortTwoData();
                    }
                }
            }
        }

        // 动态执行Operator
        OperatorType operatorType = node.getOperatorType();
        OperatorName operatorName = node.getOperatorName();
        try {
            OperatorTemplate operator = (OperatorTemplate) Class.forName("com.zxsimple.dmls.operator." +
                    operatorType.name().toLowerCase() + "." +
                    operatorName + "Operator").newInstance();
            operator.execute(hc, node, inputPortOneData, inputPortTwoData);
        } catch (Exception e) {
            logger.error("Execute DAG graph error : ", e);
            throw new ServiceException(e);
        }

    }

    private boolean validateNode(NodeVertex node) {

        boolean valid = false;

        // 动态执行Operator
        OperatorType operatorType = node.getOperatorType();
        OperatorName operatorName = node.getOperatorName();
        try {
            OperatorTemplate operator = (OperatorTemplate) Class.forName("com.dmls.core.operator." +
                    operatorType + "." +
                    operatorName + "Operator").newInstance();

            if ((Boolean) operator.validate(node.getProperties())._1()) {
                valid = true;
            } else {
                // put message into queue
            }


        } catch (InstantiationException e) {
            logger.error("Execute DAG graph error : ", e);
        } catch (IllegalAccessException e) {
            logger.error("Execute DAG graph error : ", e);
        } catch (ClassNotFoundException e) {
            logger.error("Execute DAG graph error : ", e);
        }

        return valid;
    }
}
