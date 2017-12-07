package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zxsimple on 2016/5/4.
 */
@Entity
@Table(name = "import_flow")
public class ImportFlow {
    @Id
    @GeneratedValue
    private Long id;

    @Column(name = "flow_name")
    private String flowName;

    @Column(name = "flow_desc")
    private String flowDesc;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "is_deleted")
    private int isDeleted;

    @Column(name = "update_time")
    private Timestamp updateTime;

    @OneToMany(fetch=FetchType.LAZY)
    @JoinColumn(name="import_flow_id")
    private List<ImportTask> importTasks = new ArrayList<ImportTask>();



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

    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }

    public List<ImportTask> getImportTasks() {
        return importTasks;
    }

    public void setImportTasks(List<ImportTask> importTasks) {
        this.importTasks = importTasks;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }
}
