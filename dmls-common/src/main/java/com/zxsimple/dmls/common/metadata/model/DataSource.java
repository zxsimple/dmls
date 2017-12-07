package com.zxsimple.dmls.common.metadata.model;

import com.zxsimple.dmls.common.config.SystemConfigConstants;
import com.zxsimple.dmls.common.metadata.enums.DataSourceSubType;
import com.zxsimple.dmls.common.metadata.enums.DataSourceType;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zxsimple on 4/12/2016.
 */
@Entity
@Table(name = "data_source")
public class DataSource {
    @Id
    @GeneratedValue
    private Long id;

    @Column(name = "data_source_name")
    private String dataSourceName;

    @Column(name = "data_source_type")
    @Enumerated(EnumType.STRING)
    private DataSourceType dataSourceType;

    @Column(name = "data_source_subtype")
    @Enumerated(EnumType.STRING)
    private DataSourceSubType dataSourceSubtype;

    @Column(name = "server_name")
    private String serverName;

    @Column(name = "conn_user")
    private String connUser;

    @Column(name = "conn_pwd")
    private String connPwd;

    @Column(name = "ip")
    private String ip;

    @Column(name = "port")
    private int port;

    @Column(name = "database_name")
    private String databaseName;

    @Column(name = "select_sql")
    private String selectSql;

    @Column(name = "txt_input_path")
    private String txtInputPath;

    @Column(name = "txt_encoding")
    private String txtEncoding;

    @Column(name = "first_line_is_col_name")
    private int firstLineIsColName;

    @Column(name = "broker_list")
    private String brokerList;

    @Column(name = "topic")
    private String topic;

    @Column(name = "delimiter")
    private String delimiter;

    @Column(name = "col_names")
    private String colNames;

    @Column(name = "col_names_type")
    private String colNamesType;

    @Column(name = "first_line")
    private String firstLine;

    @Column(name = "output_path")
    private String outputPath;

    @Column(name = "output_name")
    private String outputName;

    @Column(name = "is_deleted")
    private int isDeleted;

    @Transient
    private ArrayList<ArrayList> rows = new ArrayList<ArrayList>();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }


    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }


    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }


    public String getConnUser() {
        return connUser;
    }

    public void setConnUser(String connUser) {
        this.connUser = connUser;
    }


    public String getConnPwd() {
        return connPwd;
    }

    public void setConnPwd(String connPwd) {
        this.connPwd = connPwd;
    }


    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }


    public String getTxtInputPath() {
        return txtInputPath;
    }

    public void setTxtInputPath(String txtInputPath) {
        this.txtInputPath = txtInputPath;
    }

    public String getTxtEncoding() {
        return txtEncoding;
    }

    public void setTxtEncoding(String txtEncoding) {
        this.txtEncoding = txtEncoding;
    }

    public int getFirstLineIsColName() {
        return firstLineIsColName;
    }

    public void setFirstLineIsColName(int firstLineIsColName) {
        this.firstLineIsColName = firstLineIsColName;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getOutputPath() {
        return outputPath;
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

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }


    public String getOutputName() {
        return outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }


    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public DataSourceSubType getDataSourceSubtype() {
        return dataSourceSubtype;
    }

    public void setDataSourceSubtype(DataSourceSubType dataSourceSubtype) {
        this.dataSourceSubtype = dataSourceSubtype;
    }


    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }

    public ArrayList<ArrayList> getRows() {
        List<String> titles = new ArrayList<String>();
        List<String> first = new ArrayList<String>();
        List<String> colType = new ArrayList<String>();
        if(dataSourceType.equals(DataSourceType.TXT)&&txtInputPath!=null && !txtInputPath.trim().equals("")&&delimiter!=null && !delimiter.equals("")&&firstLineIsColName!= SystemConfigConstants.FIRSTLINE_INITVALUE){
            String[] colNamesArr = colNames.split(",");
            int length = colNamesArr.length;
            titles=  Arrays.asList(colNamesArr);
            colType = Arrays.asList(colNamesType.split(","));
            String[] firstSplit = new String[length];
            String[] split = firstLine.split(",");
            if(length>split.length){
                for(int i=0;i<split.length;i++){
                    firstSplit[i]=split[i];
                }
            }else{
                for(int i=0;i<length;i++){
                    firstSplit[i]=split[i];
                }
            }

            first= Arrays.asList(firstSplit);
            if(titles.size()!=0&&titles!=null){
                if(titles.size()<1000){
                    for(int i=0;i<titles.size();i++){
                        ArrayList<String> every = new ArrayList<String>();
                        every.add(first.get(i));
                        every.add(titles.get(i));
                        every.add(colType.get(i));
                        rows.add(every);
                    }
                }else{
                    for(int i=0;i<1000;i++){
                        ArrayList<String> every = new ArrayList<String>();
                        every.add(first.get(i));
                        every.add(titles.get(i));
                        every.add(colType.get(i));
                        rows.add(every);
                    }
                }
            }
        }
        return rows;
    }

    public void setRows(ArrayList<ArrayList> rows) {
        this.rows = rows;
    }
}
