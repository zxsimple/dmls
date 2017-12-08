package com.zxsimple.dmls.node;


import com.zxsimple.dmls.common.metadata.model.ETLTask;
import java.util.ArrayList;

/**
 * DAG拓扑节点
 * Created by Sanders on 2016/3/31.
 */
public class Node {
    private ETLTask etlTask;
    private ArrayList<Node> parent;
    private ArrayList<Node> descendants;

    public Node() {
    }

    public Node(ETLTask etlTask) {
        this(etlTask,new ArrayList<Node>(),new ArrayList<Node>());
    }

    public Node(ETLTask etlTask, ArrayList<Node> parent, ArrayList<Node> descendants) {
        this.etlTask = etlTask;
        this.parent = parent;
        this.descendants = descendants;
    }

    public ETLTask getEtlTask() {
        return etlTask;
    }

    public void setEtlTask(ETLTask etlTask) {
        this.etlTask = etlTask;
    }

    public ArrayList<Node> getParent() {
        return parent;
    }

    public void setParent(ArrayList<Node> parent) {
        this.parent = parent;
    }

    public ArrayList<Node> getDescendants() {
        return descendants;
    }

    public void setDescendants(ArrayList<Node> descendants) {
        this.descendants = descendants;
    }

}
