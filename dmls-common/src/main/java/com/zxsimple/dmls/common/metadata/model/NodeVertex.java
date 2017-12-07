package com.zxsimple.dmls.common.metadata.model;

import com.zxsimple.dmls.common.metadata.enums.OperatorName;
import com.zxsimple.dmls.common.metadata.enums.OperatorType;
import com.zxsimple.dmls.common.metadata.enums.Status;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.spark.sql.DataFrame;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Sanders on 2016/10/19.
 */
@Entity
@Table(name = "node_vertex")
public class NodeVertex {

    @Id
    @Column(name = "instance_id")
    private String instanceId;

    @Column(name = "process_id")
    private Long processId;

    @Column(name = "operator_type")
    @Enumerated(EnumType.STRING)
    private OperatorType operatorType;

    @Column(name = "operator_name")
    @Enumerated(EnumType.STRING)
    private OperatorName operatorName;

    @Column(name = "operator_index")
    private int operatorIndex;

    @Column(name = "properties_str")
    private String propertiesStr;

    @JsonIgnore
    @Transient
    private Properties properties = new Properties();


    @Column(name = "columns_and_types_str")
    private String columnsAndTypeStr;

    @JsonIgnore
    @Transient
    private HashMap<String,String> colAndTypes = new HashMap<String,String>();


    @Column(name = "statistics_data")
    private String statisticsData;

    @Column(name = "status")
    @Enumerated(EnumType.STRING)
    private Status status;

    @Column(name = "logs")
    private String logs;

    @Column(name = "update_time")
    private Timestamp updateTime;

    @Column(name = "is_deleted")
    private int isDeleted;

    @Transient
    @JsonIgnore
    private DataFrame outPortOneData;

    @Transient
    @JsonIgnore
    private DataFrame outPortTwoData;

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public void setOperatorType(OperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public OperatorName getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(OperatorName operatorName) {
        this.operatorName = operatorName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public Long getProcessId() {
        return processId;
    }

    public void setProcessId(Long processId) {
        this.processId = processId;
    }

    public int getOperatorIndex() {
        return operatorIndex;
    }

    public void setOperatorIndex(int operatorIndex) {
        this.operatorIndex = operatorIndex;
    }

    public String getPropertiesStr() {
        return propertiesStr;
    }

    public void setPropertiesStr(String propertiesStr) {
        this.propertiesStr = propertiesStr;
    }

    public String getColumnsAndTypeStr() {
        return columnsAndTypeStr;
    }

    public void setColumnsAndTypeStr(String columnsAndTypeStr) {
        this.columnsAndTypeStr = columnsAndTypeStr;
    }

    public String getStatisticsData() {
        return statisticsData;
    }

    public void setStatisticsData(String statisticsData) {
        this.statisticsData = statisticsData;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getLogs() {
        return logs;
    }

    public void setLogs(String logs) {
        this.logs = logs;
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

    public DataFrame getOutPortOneData() {
        return outPortOneData;
    }

    public void setOutPortOneData(DataFrame outPortOneData) {
        this.outPortOneData = outPortOneData;
    }

    public DataFrame getOutPortTwoData() {
        return outPortTwoData;
    }

    public void setOutPortTwoData(DataFrame outPortTwoData) {
        this.outPortTwoData = outPortTwoData;
    }

    public Properties getProperties() {
        if(propertiesStr!=null && !propertiesStr.trim().equals("")){
            JSONObject jsonObject = JSONObject.parseObject(propertiesStr);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                properties.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public HashMap<String, String> getColAndTypes() {
        if(columnsAndTypeStr!=null && !columnsAndTypeStr.trim().equals("")){
            JSONObject jsonObject = JSONObject.parseObject(columnsAndTypeStr);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                colAndTypes.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return colAndTypes;
    }

    public void setColAndTypes(HashMap<String, String> colAndTypes) {
        this.colAndTypes = colAndTypes;
    }

}
