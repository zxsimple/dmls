package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lxf on 2016/6/12.
 */
@Entity
@Table(name = "etl_flow")
public class ETLFlow implements Serializable {

    @Id
    @GeneratedValue
    private Long id;

    @Column(name = "flow_name")
    private String flowName;

    @Column(name = "flow_desc")
    private String flowDesc;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "path")
    private String path;

    @OneToMany(fetch=FetchType.LAZY)
    @JoinColumn(name="etl_flow_id")
    private List<ETLTask> etlTasks = new ArrayList<ETLTask>();

    @Column(name = "is_deleted")
    private int isDeleted;

    @Column(name = "update_time")
    private Timestamp updateTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    public String getFlowDesc() {
        return flowDesc;
    }

    public void setFlowDesc(String flowDesc) {
        this.flowDesc = flowDesc;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public List<ETLTask> getEtlTasks() {
        return etlTasks;
    }

    public void setEtlTasks(List<ETLTask> etlTasks) {
        this.etlTasks = etlTasks;
    }

    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }
}
