package com.zxsimple.dmls.common.metadata.model;

import com.zxsimple.dmls.common.metadata.enums.Recurring;
import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by zxsimple on 2016/6/12.
 * 应用方案模型表
 */
@Entity
@Table(name = "predict_app")
public class PredictApp implements Serializable {

    @Id
    @GeneratedValue
    private Long id;

    @Column(name = "train_app_id")
    private Long trainAppId;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "etl_flow_id")
    private ETLFlow etlFlow;

    @Column(name = "app_name")
    private String appName;

    @Column(name = "app_desc")
    private String appDesc;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "is_deleted")
    private int isDeleted;

    @Column(name = "update_time")
    private Timestamp updateTime;

    @Column(name = "model_predict_id")
    private Long modelPredictId;


    @Column(name = "model_result_path")
    private String modelResultPath;

    @Column(name = "exec_type")
    private int execType;

    @Column(name = "schedule_time")
    private String scheduleTime;

    @Column(name = "schedule_date")
    private String scheduleDate;

    @Column(name = "is_recurring")
    @Enumerated(EnumType.STRING)
    private Recurring isRecurring;

    @Column(name = "period")
    private String period;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getTrainAppId() {
        return trainAppId;
    }

    public void setTrainAppId(Long trainAppId) {
        this.trainAppId = trainAppId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppDesc() {
        return appDesc;
    }

    public void setAppDesc(String appDesc) {
        this.appDesc = appDesc;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
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

    public String getModelResultPath() {
        return modelResultPath;
    }

    public void setModelResultPath(String modelResultPath) {
        this.modelResultPath = modelResultPath;
    }

    public Long getModelPredictId() {
        return modelPredictId;
    }

    public void setModelPredictId(Long modelPredictId) {
        this.modelPredictId = modelPredictId;
    }

    public ETLFlow getEtlFlow() {
        return etlFlow;
    }

    public void setEtlFlow(ETLFlow etlFlow) {
        this.etlFlow = etlFlow;
    }

    public int getExecType() {
        return execType;
    }

    public void setExecType(int execType) {
        this.execType = execType;
    }

    public String getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(String scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public String getScheduleDate() {
        return scheduleDate;
    }

    public void setScheduleDate(String scheduleDate) {
        this.scheduleDate = scheduleDate;
    }

    public Recurring getIsRecurring() {
        return isRecurring;
    }

    public void setIsRecurring(Recurring isRecurring) {
        this.isRecurring = isRecurring;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }

    @Override
    public String toString() {
        return "PredictApp{" +
                "id=" + id +
                ", trainAppId=" + trainAppId +
                ", etlFlow=" + etlFlow +
                ", appName='" + appName + '\'' +
                ", appDesc='" + appDesc + '\'' +
                ", userId=" + userId +
                ", isDeleted=" + isDeleted +
                ", modelResultPath='" + modelResultPath + '\'' +
                ", execType=" + execType +
                ", scheduleTime='" + scheduleTime + '\'' +
                ", scheduleDate='" + scheduleDate + '\'' +
                ", isRecurring=" + isRecurring +
                ", period='" + period + '\'' +
                '}';
    }
}
