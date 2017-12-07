package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zxsimple on 2016/5/4.
 */
@Entity
@Table(name = "ml_flow")
public class MLFlow implements Serializable {

    @Id
    @GeneratedValue
    private Long id;

    @Column(name = "flow_name")
    private String flowName;

    @Column(name = "flow_desc")
    private String flowDesc;

    @Column(name = "train_app_id")
    private Long trainAppId;

    @OneToMany(fetch=FetchType.LAZY)
    @JoinColumn(name="ml_flow_id")
    private List<MLTask> mlTasks = new ArrayList<MLTask>();

    @Column(name = "is_deleted")
    private int isDeleted;


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


    public Long getTrainAppId() {
        return trainAppId;
    }

    public void setTrainAppId(Long trainAppId) {
        this.trainAppId = trainAppId;
    }


    public List<MLTask> getMlTasks() {
        return mlTasks;
    }

    public void setMlTasks(List<MLTask> mlTasks) {
        this.mlTasks = mlTasks;
    }

    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }
}
