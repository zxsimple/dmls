package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;

/**
 * Created by zxsimple on 5/22/2016.
 */
@Entity
@Table(name = "dataset_statistics")
public class DatasetStatistics {

    @Id
    @GeneratedValue
    private Long id;

    @Column(name="column_name")
    private String columnName;

    @Column(name="min_value")
    private double minValue;

    @Column(name="max_value")
    private double maxValue;

    @Column(name="average")
    private double average;

    @Column(name="variance")
    private double variance;

    @Column(name="STD")
    private double STD;

    @Column(name="records")
    private double records;

    @Column(name="lost_records")
    private double lostRecords;

    @Column(name="dataset_id")
    private double datasetId;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public double getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public double getVariance() {
        return variance;
    }

    public void setVariance(double variance) {
        this.variance = variance;
    }

    public double getSTD() {
        return STD;
    }

    public void setSTD(double STD) {
        this.STD = STD;
    }

    public double getRecords() {
        return records;
    }

    public void setRecords(double records) {
        this.records = records;
    }

    public double getLostRecords() {
        return lostRecords;
    }

    public void setLostRecords(double lostRecords) {
        this.lostRecords = lostRecords;
    }

    public double getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(double datasetId) {
        this.datasetId = datasetId;
    }
}
