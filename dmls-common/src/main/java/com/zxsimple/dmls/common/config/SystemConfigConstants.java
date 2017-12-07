package com.zxsimple.dmls.common.config;

/**
 * Created by zxsimple on 5-9.
 */
public class SystemConfigConstants {

    public static final String AZKABAN_URI = "azkaban.url";
    public static final String AZKABANUSER = "azkaban.user";
    public static final String AZKABANPWD = "azkaban.password";
    public static final String NAMENODE_RUI = "hdp.namenode.url";
    public static final String SQOOP_URL = "sqoop2.url";
    public static final String HBASE_ZOO_QUORUM = "hbase.zoo.quorum";
    public static final String HBASE_ZOO_CLIENT_PORT = "hbase.zoo.client.port";
    public static final String HBASE_ZOO_ZNODE_PARENT = "hbase.zoo.znode.parent";
    public static final String HIVE_SERVER2_IP = "hiveserver2.ip";
    public static final String HIVE_SERVER2_PORT = "hiveserver2.port";
    public static final String HIVE_SERVER2_USER = "hiveserver2.user";
    public static final String HIVE_SERVER2_PASSWORD = "hiveserver2.password";
    public static final String IMPORT_SQOOP_IP = "sqoop1.import.ip";
    public static final String IMPORT_SQOOP_USER = "sqoop1.import.user";
    public static final String IMPORT_SQOOP_PASSWORD = "sqoop1.import.password";
    public static final String TOMCAT_IP = "tomcat.ip";
    public static final String TOMCAT_STARTUP_USER = "tomcat.startup.user";
    public static final String TOMCAT_STARTUP_PASSWORD = "tomcat.startup.password";
    public static final String HADOOP_NAMENODE_URL_HOSTNAME = "hdp.namenode.url.hostname";

    public static final String HADOOP_VERSION = "hdp.version";


    public static final int NOTICE_VIEWED = 1;

    public static final int DATASET_IMPORT_ING = 2;
    public static final int DATASET_IMPORT_FAIL = 3;

    public static final int DELETE = 1;
    public static final int UNDELETE = 0;

    public static final int FIRSTLINE_INITVALUE = 0;
    public static final int FIRSTLINE_IS_COLNAME = 1;
    public static final int FIRSTLINE_NOT_COLNAME = 2;

    public static final int SUCCESS = 1;
    public static final int FAIL = 2;
}
