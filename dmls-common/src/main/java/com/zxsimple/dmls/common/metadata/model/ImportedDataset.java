package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by zxsimple on 4/12/2016.
 */
@Entity
@Table(name = "imported_dataset")
public class ImportedDataset implements Serializable {
    @Id
    @GeneratedValue
    private Long id;

    @Column(name="file_name")
    private String fileName;

    @Column(name="file_time")
    private Timestamp fileTime;

    @Column(name="file_loaded_time")
    private Timestamp fileLoadedTime;

    @Column(name="file_size")
    private String fileSize;

    @Column(name="number_of_records")
    private long numberOfRecords;

    @Column(name="col_names")
    private String colNames;

    @Column(name = "col_names_type")
    private String colNamesType;

    @Column(name = "first_line")
    private String firstLine;

    @Column(name="delimiter")
    private String delimiter;

    @Column(name="dataset_path")
    private String datasetPath;

    @Column(name="hive_database")
    private String hiveDatabase;

    @Column(name="hive_table_name")
    private String hiveTableName;

    @Column(name="hbase_database")
    private String hbaseDatabase;

    @Column(name="hbase_table_name")
    private String hbaseTableName;

    @Column(name="data_type")
    private int dataType;

    @Column(name="user_id")
    private Long userId;

    @Column(name="imported_dataset_desc")
    private String importedDatasetDesc;

    @Column(name="status")
    private int status;

    @Column(name="is_statistics")
    private int isStatistics;

    @Column(name = "is_deleted")
    private int isDeleted;

    @Column(name = "update_time")
    private Timestamp updateTime;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Timestamp getFileTime() {
        return fileTime;
    }

    public void setFileTime(Timestamp fileTime) {
        this.fileTime = fileTime;
    }

    public Timestamp getFileLoadedTime() {
        return fileLoadedTime;
    }

    public void setFileLoadedTime(Timestamp fileLoadedTime) {
        this.fileLoadedTime = fileLoadedTime;
    }

    public String getFileSize() {
        return fileSize;
    }

    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }

    public long getNumberOfRecords() {
        return numberOfRecords;
    }

    public void setNumberOfRecords(long numberOfRecords) {
        this.numberOfRecords = numberOfRecords;
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

    public String getFirstLine() {
        return firstLine;
    }

    public void setFirstLine(String firstLine) {
        this.firstLine = firstLine;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public void setDatasetPath(String datasetPath) {
        this.datasetPath = datasetPath;
    }

    public int getDataType() {
        return dataType;
    }

    public String getHiveDatabase() {
        return hiveDatabase;
    }

    public void setHiveDatabase(String hiveDatabase) {
        this.hiveDatabase = hiveDatabase;
    }

    public String getHiveTableName() {
        return hiveTableName;
    }

    public void setHiveTableName(String hiveTableName) {
        this.hiveTableName = hiveTableName;
    }

    public String getHbaseDatabase() {
        return hbaseDatabase;
    }

    public void setHbaseDatabase(String hbaseDatabase) {
        this.hbaseDatabase = hbaseDatabase;
    }

    public String getHbaseTableName() {
        return hbaseTableName;
    }

    public void setHbaseTableName(String hbaseTableName) {
        this.hbaseTableName = hbaseTableName;
    }

    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getImportedDatasetDesc() {
        return importedDatasetDesc;
    }

    public void setImportedDatasetDesc(String importedDatasetDesc) {
        this.importedDatasetDesc = importedDatasetDesc;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getIsStatistics() {
        return isStatistics;
    }

    public void setIsStatistics(int isStatistics) {
        this.isStatistics = isStatistics;
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
}
