package com.zxsimple.dmls.manager;

/**
 * Created by zxsimple on 2016/10/11.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

public class YarnJobSubmitter {

    public static void execute(long processId) {
        String[] argsNew = new String[] {
                // the name of your application
                "--name",
                "ProcessExector",

                // memory for driver (optional)
                "--executor-memory",
                "3G",

                // path to your application's JAR file
                // required in yarn-cluster mode
                // "--jar",
                // "/home/dmls-spark/dmls-core-0.0.1-SNAPSHOT-jar-with-dependencies.jar",

                // name of your application's main class (required)
                "--class",
                "com.zxsimple.dmls.manager.ProcessExecutor",

                // comma separated list of local jars that want
                // SparkContext.addJar to work with
                "--addJars",
                "/usr/hdp/2.4.0.0-169/spark/lib/datanucleus-api-jdo-3.2.6.jar," +
                        "/usr/hdp/2.4.0.0-169/spark/lib/datanucleus-core-3.2.10.jar," +
                        "/usr/hdp/2.4.0.0-169/spark/lib/datanucleus-rdbms-3.2.9.jar," +
                        "/usr/hdp/2.4.0.0-169/tez/tez-api-0.7.0.2.4.0.0-169.jar," +
                        "/usr/hdp/2.4.0.0-169/hive/lib/hive-hbase-handler-1.2.1000.2.4.0.0-169.jar",

                // argument 1 to your Spark program (SparkFriendRecommendation)
                "--arg",
                String.valueOf(processId),

                "--arg",
                "yarn-client"
        };

        // create a Hadoop Configuration object
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        // identify that you will be using Spark as YARN mode
        System.setProperty("SPARK_YARN_MODE", "true");

        // create an instance of SparkConf object
        SparkConf sparkConf = new SparkConf();

        // create ClientArguments, which will be passed to Client
        ClientArguments cArgs = new ClientArguments(argsNew, sparkConf);

        // create an instance of yarn Client client
        Client client = new Client(cArgs, config, sparkConf);

        ApplicationId appId = client.submitApplication();
        scala.Tuple2<YarnApplicationState, FinalApplicationStatus> state = client.monitorApplication(appId, false, false);
    }

    public static void main(String [] args) {

        execute(Long.parseLong(args[0]));
    }
}