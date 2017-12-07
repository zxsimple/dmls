package com.zxsimple.dmls.common.metadata.model;

import com.zxsimple.dmls.common.metadata.enums.Recurring;
import com.zxsimple.dmls.common.metadata.enums.Status;
import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by zxsimple on 2016/10/25.
 */
@Entity
@Table(name = "process")
public class Process {
    @Id
    @GeneratedValue
    private Long id;

    @Column(name = "name")
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "user_id")
    private Long userId;

    @OneToMany(fetch=FetchType.EAGER)
    @JoinColumn(name="process_id")
    private List<NodeEdge> nodeEdges = new ArrayList<NodeEdge>();

    @JsonIgnore
    @Transient
    private List<NodeVertex> nodeVertexs = new ArrayList<NodeVertex>();

    @Column(name = "graph_data")
    private String graphData;

    @Column(name = "status")
    @Enumerated(EnumType.STRING)
    private Status status;

    @Column(name = "schedule_type")
    private int scheduleType;

    @Column(name = "schedule_time")
    private String scheduleTime;

    @Column(name = "schedule_date")
    private String scheduleDate;

    @Column(name = "is_recurring")
    @Enumerated(EnumType.STRING)
    private Recurring isRecurring;

    @Column(name = "period")
    private String period;

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public List<NodeEdge> getNodeEdges() {
        return nodeEdges;
    }

    public void setNodeEdges(List<NodeEdge> nodeEdges) {
        this.nodeEdges = nodeEdges;
    }

    public String getGraphData() {
        return graphData;
    }

    public void setGraphData(String graphData) {
        this.graphData = graphData;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int getScheduleType() {
        return scheduleType;
    }

    public void setScheduleType(int scheduleType) {
        this.scheduleType = scheduleType;
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

    public List<NodeVertex> getNodeVertexs() {
        HashSet<NodeVertex> vertexsSet = new HashSet<NodeVertex>();
        for (NodeEdge edge : nodeEdges) {
            vertexsSet.add(edge.getFromNode());
            vertexsSet.add(edge.getToNode());
        }
        nodeVertexs = new ArrayList<NodeVertex>(vertexsSet);
        return nodeVertexs;
    }
}
