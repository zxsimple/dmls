package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;
import java.sql.Timestamp;

/**
 * Created by zxsimple on 2016/10/26.
 */
@Entity
@Table(name = "model")
public class Model {
    @Id
    @GeneratedValue
    private Long id;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "process_id")
    private Process process;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "node_vertex_instance_id")
    private NodeVertex nodeVertex;

    @Column(name = "alg")
    private String alg;

    @Column(name = "evaluation")
    private String evaluation;

    @Column(name = "path")
    private String path;

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

    public Process getProcess() {
        return process;
    }

    public void setProcess(Process process) {
        this.process = process;
    }

    public NodeVertex getNodeVertex() {
        return nodeVertex;
    }

    public void setNodeVertex(NodeVertex nodeVertex) {
        this.nodeVertex = nodeVertex;
    }

    public String getAlg() {
        return alg;
    }

    public void setAlg(String alg) {
        this.alg = alg;
    }

    public String getEvaluation() {
        return evaluation;
    }

    public void setEvaluation(String evaluation) {
        this.evaluation = evaluation;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
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
