package com.zxsimple.dmls.common.metadata.model;

import com.zxsimple.dmls.common.metadata.enums.Recurring;
import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by zxsimple on 4/12/2016.
 */
@Entity
@Table(name = "train_app")
public class TrainApp implements Serializable {
    @Id
    @GeneratedValue
    private Long id;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "etl_flow_id")
    private ETLFlow etlFlow;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "ml_flow_id")
    private MLFlow mlFlow;

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

    @Column(name = "model_status")
    private int modelStatus;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "model_metrics_id")
    private ModelMetrics modelMetrics;

    @Column(name = "model_save_path")
    private String modelSavePath;

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

    public ETLFlow getEtlFlow() {
        return etlFlow;
    }

    public void setEtlFlow(ETLFlow etlFlow) {
        this.etlFlow = etlFlow;
    }

    public MLFlow getMlFlow() {
        return mlFlow;
    }

    public void setMlFlow(MLFlow mlFlow) {
        this.mlFlow = mlFlow;
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

    public int getModelStatus() {
        return modelStatus;
    }

    public void setModelStatus(int modelStatus) {
        this.modelStatus = modelStatus;
    }

    public ModelMetrics getModelMetrics() {
        return modelMetrics;
    }

    public void setModelMetrics(ModelMetrics modelMetrics) {
        this.modelMetrics = modelMetrics;
    }

    public String getModelSavePath() {
        return modelSavePath;
    }

    public void setModelSavePath(String modelSavePath) {
        this.modelSavePath = modelSavePath;
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

    public int getExecType() {
        return execType;
    }

    public void setExecType(int execType) {
        this.execType = execType;
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
}
