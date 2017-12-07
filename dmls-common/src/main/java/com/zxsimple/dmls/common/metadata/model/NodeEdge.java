package com.zxsimple.dmls.common.metadata.model;


import javax.persistence.*;
import java.sql.Timestamp;

/**
 * Created by Sanders on 2016/10/19.
 */
@Entity
@Table(name = "node_edge")
public class NodeEdge {

    public static final int PORT_NUM_ONE = 1;
    public static final int PORT_NUM_TWO = 2;

    @Id
    @GeneratedValue
    private Long id;

    @Column(name = "process_id")
    private Long processId;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "from_instance_id")
    private NodeVertex fromNode;     // 父算子节点

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "to_instance_id")
    private NodeVertex toNode;        // 子算子节点

    @Column(name = "from_port_num")
    private int fromPortNum;     // 父算子输出端口
    @Column(name = "to_port_num")
    private int toPortNum;        // 子算子输入端口

    @Column(name = "update_time")
    private Timestamp updateTime;

    @Column(name = "is_deleted")
    private int isDeleted;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getProcessId() {
        return processId;
    }

    public void setProcessId(Long processId) {
        this.processId = processId;
    }

    public NodeVertex getFromNode() {
        return fromNode;
    }

    public void setFromNode(NodeVertex fromNode) {
        this.fromNode = fromNode;
    }

    public NodeVertex getToNode() {
        return toNode;
    }

    public void setToNode(NodeVertex toNode) {
        this.toNode = toNode;
    }

    public int getFromPortNum() {
        return fromPortNum;
    }

    public void setFromPortNum(int fromPortNum) {
        this.fromPortNum = fromPortNum;
    }

    public int getToPortNum() {
        return toPortNum;
    }

    public void setToPortNum(int toPortNum) {
        this.toPortNum = toPortNum;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }

    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }
}
