package com.zxsimple.dmls.common.metadata.model;

import com.zxsimple.dmls.common.metadata.enums.MLTaskName;
import com.alibaba.fastjson.JSONObject;

import javax.persistence.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zxsimple on 2016/4/13.
 */
@Entity
@Table(name = "ml_task")
public class MLTask implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "ml_flow_id")
    private Long mlFlowId;

    @Column(name = "ml_task_name")
    @Enumerated(EnumType.STRING)
    private MLTaskName mlTaskName;

    @Column(name = "is_deleted")
    private int isDeleted;

    @Column(name = "prop_str")
    private String propStr;

    @Transient
    private HashMap<String,String> taskProperties = new HashMap<String,String>();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getMlFlowId() {
        return mlFlowId;
    }

    public void setMlFlowId(Long mlFlowId) {
        this.mlFlowId = mlFlowId;
    }

    public MLTaskName getMlTaskName() {
        return mlTaskName;
    }

    public void setMlTaskName(MLTaskName mlTaskName) {
        this.mlTaskName = mlTaskName;
    }

    public String getPropStr() {
        return propStr;
    }

    public void setPropStr(String propStr) {
        this.propStr = propStr;
    }

    public HashMap<String, String> getTaskProperties() {
        if(propStr!=null && !propStr.trim().equals("")){
            JSONObject jsonObject = JSONObject.parseObject(propStr);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                taskProperties.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return taskProperties;
    }

    public void setTaskProperties(HashMap<String, String> taskProperties) {
        this.taskProperties = taskProperties;
    }

    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }


}
