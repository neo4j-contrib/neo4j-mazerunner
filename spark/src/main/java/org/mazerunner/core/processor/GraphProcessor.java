package org.mazerunner.core.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.mazerunner.core.config.ConfigurationLoader;
import org.mazerunner.core.algorithms.*;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Copyright (C) 2014 Kenny Bastani
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
public class GraphProcessor {

    public static final String PROPERTY_GRAPH_UPDATE_PATH = "/neo4j/mazerunner/propertyUpdateList.txt";

    public static void processEdgeList(String hdfsPath) throws IOException, URISyntaxException {
        String appName = "mazerunner";
        SparkConf conf = new SparkConf().setAppName(appName).set("spark.master", "local[8]")
                .set("spark.locality.wait", "3000")
                .set("spark.executor.memory", "13g")
                .set("spark.cores.max", "8")
                .set("spark.executor.extraClassPath", "/spark-1.1.0/lib/*")
                .set("spark.files.userClassPathFirst", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String results = RunPageRank.pageRank(sc.sc(), hdfsPath);

        // Write results to HDFS
        org.mazerunner.core.hdfs.FileUtil.writePropertyGraphUpdate(ConfigurationLoader.getInstance().getHadoopHdfsUri() + PROPERTY_GRAPH_UPDATE_PATH, results);
    }
}
