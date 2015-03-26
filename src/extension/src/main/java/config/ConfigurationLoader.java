package config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
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
    private String hadoopSitePath;
    private String hadoopHdfsPath;
    private String hadoopHdfsUri;
    private String mazerunnerRelationshipType;
    private String rabbitmqNodename;

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
        System.out.println(new File("../").getAbsolutePath());
        InputStream stream = new FileInputStream("conf/mazerunner.properties");
        prop.load(stream);

        hadoopSitePath = prop.getProperty(HADOOP_CORE_SITE_KEY);
        hadoopHdfsPath = prop.getProperty(HADOOP_HDFS_SITE_KEY);

        // Inherit the environment variable for the HDFS path
        Map<String, String> env = System.getenv();
        hadoopHdfsUri = java.net.URLDecoder.decode(prop.getProperty(HADOOP_HDFS_URI), "UTF-8");
        if(env.containsKey("HDFS_HOST"))
            hadoopHdfsUri = env.get("HDFS_HOST");

        mazerunnerRelationshipType = prop.getProperty(MAZERUNNER_RELATIONSHIP_TYPE_KEY);
        rabbitmqNodename = prop.getProperty(RABBITMQ_NODENAME_KEY);

        stream.close();
    }

    public void initializeTest()
    {
        hadoopSitePath = "/etc/hadoop/core-site.xml";
        hadoopHdfsPath = "/etc/hadoop/hdfs-site.xml";
        hadoopHdfsUri = "hdfs://0.0.0.0:9000";
        mazerunnerRelationshipType = "CONNECTED_TO";
        rabbitmqNodename = "localhost";
    }

    public String getMazerunnerRelationshipType() {
        return mazerunnerRelationshipType;
    }

    public void setMazerunnerRelationshipType(String relationshipType) {
        mazerunnerRelationshipType = relationshipType;
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
}
