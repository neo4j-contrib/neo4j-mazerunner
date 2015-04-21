package org.mazerunner.core.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Copyright (C) 2014 Kenny Bastani
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
public class ConfigurationLoader {

    public static boolean testPropertyAccess = false;

    public static final String HADOOP_CORE_SITE_KEY = "org.mazerunner.hadoop.core.path";
    public static final String HADOOP_HDFS_SITE_KEY = "org.mazerunner.hadoop.hdfs.path";
    public static final String HADOOP_HDFS_URI = "org.mazerunner.hadoop.hdfs.uri";
    public static final String MAZERUNNER_RELATIONSHIP_TYPE_KEY = "org.mazerunner.job.relationshiptype";
    public static final String RABBITMQ_NODENAME_KEY = "org.mazerunner.rabbitmq.nodename";
    public static final String SPARK_HOST = "org.mazerunner.spark.host";
    private String hadoopSitePath;
    private String hadoopHdfsPath;
    private String hadoopHdfsUri;
    private String mazerunnerRelationshipType;

    public void setRabbitmqNodename(String rabbitmqNodename) {
        this.rabbitmqNodename = rabbitmqNodename;
    }

    private String rabbitmqNodename;

    public String getDriverHost() {
        return driverHost;
    }

    public void setDriverHost(String driverHost) {
        this.driverHost = driverHost;
    }

    private String driverHost;

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    private String executorMemory;
    private String appName;

    private String sparkHost;

    private static ConfigurationLoader instance = null;

    protected ConfigurationLoader() {

    }

    public static ConfigurationLoader getInstance() {
        if(instance == null)
        {
            instance = new ConfigurationLoader();
            try {
                if(!testPropertyAccess) {
                    instance.initialize();
                } else {
                    instance.initializeTest();
                }
            }
            catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return instance;
    }

    public void initialize() throws IOException {
        Properties prop = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream stream = loader.getResourceAsStream("spark.properties");
        prop.load(stream);

        hadoopSitePath = prop.getProperty(HADOOP_CORE_SITE_KEY);
        hadoopHdfsPath = prop.getProperty(HADOOP_HDFS_SITE_KEY);
        mazerunnerRelationshipType = prop.getProperty(MAZERUNNER_RELATIONSHIP_TYPE_KEY);
        rabbitmqNodename = prop.getProperty(RABBITMQ_NODENAME_KEY);
    }

    public void initializeTest()
    {
        hadoopSitePath = "/hadoop-2.4.1/conf/core-site.xml";
        hadoopHdfsPath = "/hadoop-2.4.1/conf/hdfs-site.xml";
        hadoopHdfsUri = "hdfs://0.0.0.0:9000";
        mazerunnerRelationshipType = "CONNECTED_TO";
        rabbitmqNodename = "localhost";
        sparkHost = "local";
        appName = "mazerunner";
        executorMemory = "13g";
    }

    public String getMazerunnerRelationshipType() {
        return mazerunnerRelationshipType;
    }

    public String getHadoopSitePath() {
        return hadoopSitePath;
    }

    public String getHadoopHdfsPath() {
        return hadoopHdfsPath;
    }

    public String getHadoopHdfsUri() {
        return hadoopHdfsUri;
    }

    public String getRabbitmqNodename() {
        return rabbitmqNodename;
    }

    public String getSparkHost() {
        return sparkHost;
    }


    public void setSparkHost(String sparkHost) {
        this.sparkHost = sparkHost;
    }

    public void setHadoopHdfsUri(String hadoopHdfsUri) {
        this.hadoopHdfsUri = hadoopHdfsUri;
    }
}
