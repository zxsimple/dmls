package com.zxsimple.dmls.common.metadata.model;

/**
 * Created by zxsimple on 2016/4/12.
 */
import com.zxsimple.dmls.common.metadata.enums.ETLTaskName;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.spark.sql.DataFrame;
import javax.persistence.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by lxf on 2016/6/12.
 */
@Entity
@Table(name = "etl_task")
public class ETLTask implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "etl_flow_id")
    private Long etlFlowId;

    @Column(name = "etl_task_name")
    @Enumerated(EnumType.STRING)
    private ETLTaskName etlTaskName;

    @Column(name = "parent_etl_task_id")
    private Long parentETLTaskId;

    @Column(name = "is_deleted")
    private int isDeleted;

    @JsonIgnore
    @Column(name = "prop_str")
    private String propStr;

    @Column(name = "col_names")
    private String colNames;

    @Column(name = "col_names_type")
    private String colNamesType;

    @Column(name = "delimiter")
    private String delimiter;

    @Column(name = "statistics_data")
    private String statisticsData;

    @Column(name = "statistics_status")
    private int statisticsStatus;

    @Column(name = "sample_status")
    private int sampleStatus;

    @Column(name = "task_status")
    private int taskStatus;
    
    @Column(name = "log")
    private String log;

    @Transient
    @JsonIgnore
    private DataFrame data;

    @Transient
    @JsonIgnore
    private LinkedHashMap<String,String> colsAndType;

    @Transient
    private HashMap<String,String> taskProperties = new HashMap<String,String>();

    @Transient
    private String instanceId;

    @Transient
    private String parentETLTaskInstanceId;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }



    public ETLTaskName getEtlTaskName() {
        return etlTaskName;
    }

    public void setEtlTaskName(ETLTaskName etlTaskName) {
        this.etlTaskName = etlTaskName;
    }



    public Long getParentETLTaskId() {
        return parentETLTaskId;
    }

    public void setParentETLTaskId(Long parentETLTaskId) {
        this.parentETLTaskId = parentETLTaskId;
    }

    public Long getEtlFlowId() {
        return etlFlowId;
    }

    public void setEtlFlowId(Long etlFlowId) {
        this.etlFlowId = etlFlowId;
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

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getParentETLTaskInstanceId() {
        return parentETLTaskInstanceId;
    }

    public void setParentETLTaskInstanceId(String parentETLTaskInstanceId) {
        this.parentETLTaskInstanceId = parentETLTaskInstanceId;
    }

    public String getColNames() {
        return colNames;
    }

    public void setColNames(String colNames) {
        this.colNames = colNames;
    }

    public String getColNamesType() {
        return colNamesType;
    }

    public void setColNamesType(String colNamesType) {
        this.colNamesType = colNamesType;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public int getStatisticsStatus() {
        return statisticsStatus;
    }

    public void setStatisticsStatus(int statisticsStatus) {
        this.statisticsStatus = statisticsStatus;
    }

    public int getSampleStatus() {
        return sampleStatus;
    }

    public void setSampleStatus(int sampleStatus) {
        this.sampleStatus = sampleStatus;
    }

    public int getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(int taskStatus) {
        this.taskStatus = taskStatus;
    }

    public DataFrame getData() {
        return data;
    }

    public void setData(DataFrame data) {
        this.data = data;
    }

    public LinkedHashMap<String, String> getColsAndType() {
        String[] c = colNames.split(",");
        String[] ct = colNamesType.split(",");
        int length = c.length;
        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
        for (int i = 0; i < length; i++) {
            map.put(c[i], ct[i]);
        }
        return map;
    }
    public void setColsAndType(LinkedHashMap<String, String> colsAndType) {
        this.colsAndType = colsAndType;
    }

    public void updataMetadata(String colNames, String colNamesType) {
        this.colNames = colNames;
        this.colNamesType = colNamesType;
    }

    public String getStatisticsData() {
        return statisticsData;
    }

    public void setStatisticsData(String statisticsData) {
        this.statisticsData = statisticsData;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }


}
