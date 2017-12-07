package com.zxsimple.dmls.common.metadata.model;

import com.zxsimple.dmls.common.metadata.enums.AppType;

import javax.persistence.*;
import java.sql.Timestamp;

/**
 * Created by zxsimple on 6-2.
 */
@Entity
@Table(name = "azkaban_job_history")
public class AzkabanJobHistory {
    @Id
    @GeneratedValue
    private long id;

    @Column(name="app_type")
    @Enumerated(EnumType.STRING)
    private AppType appType;

    @Column(name="train_or_predict_app_id")
    private long trainOrPredictAppId;

    @Column(name="user_id")
    private long userId;

    @Column(name="app_name")
    private String appName;

    @Column(name="project_name")
    private String projectName;

    @Column(name="exec_id")
    private int execId;

    @Column(name="project_id")
    private int projectId;

    @Column(name="flow_id")
    private String flowId;

    @Column(name="job_ids")
    private String jobIds;

    @Column(name="submit_user")
    private String submitUser;

    @Column(name="submit_time")
    private long submitTime;

    @Column(name="start_time")
    private long startTime;

    @Column(name="end_time")
    private long endTime;

    @Column(name="exec_type")
    private int execType;


    @Column(name="status")
    private String status;

    @Column(name="times")
    private int times;

    @Column(name = "is_deleted")
    private int isDeleted;

    @Column(name = "update_time")
    private Timestamp updateTime;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public AppType getAppType() {
        return appType;
    }

    public void setAppType(AppType appType) {
        this.appType = appType;
    }

    public long getTrainOrPredictAppId() {
        return trainOrPredictAppId;
    }

    public void setTrainOrPredictAppId(long trainOrPredictAppId) {
        this.trainOrPredictAppId = trainOrPredictAppId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public int getExecId() {
        return execId;
    }

    public void setExecId(int execId) {
        this.execId = execId;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    public String getJobIds() {
        return jobIds;
    }

    public void setJobIds(String jobIds) {
        this.jobIds = jobIds;
    }

    public String getSubmitUser() {
        return submitUser;
    }

    public void setSubmitUser(String submitUser) {
        this.submitUser = submitUser;
    }

    public long getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(long submitTime) {
        this.submitTime = submitTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int getExecType() {
        return execType;
    }

    public void setExecType(int execType) {
        this.execType = execType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getTimes() {
        return times;
    }

    public void setTimes(int times) {
        this.times = times;
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

    @Override
    public String toString() {
        return "AzkabanJobHistory{" +
                "id=" + id +
                ", appType=" + appType +
                ", trainOrPredictAppId=" + trainOrPredictAppId +
                ", userId=" + userId +
                ", appName='" + appName + '\'' +
                ", projectName='" + projectName + '\'' +
                ", execId=" + execId +
                ", projectId=" + projectId +
                ", flowId='" + flowId + '\'' +
                ", submitUser='" + submitUser + '\'' +
                ", submitTime=" + submitTime +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", execType=" + execType +
                ", status='" + status + '\'' +
                ", isDeleted=" + isDeleted +
                '}';
    }

}
