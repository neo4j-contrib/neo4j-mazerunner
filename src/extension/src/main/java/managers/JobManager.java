package managers;

import com.google.gson.Gson;
import messaging.Worker;
import models.JobRequest;
import models.ProcessorMessage;
import models.ProcessorMode;
import org.apache.hadoop.fs.Path;
import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.util.Assert;
import translation.Writer;

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
public class JobManager {
    private JobRequest jobRequest;
    private GraphDatabaseService graphDatabaseService;

    public JobManager(JobRequest jobRequest, GraphDatabaseService graphDatabaseService) {
        this.jobRequest = jobRequest;
        this.graphDatabaseService = graphDatabaseService;
    }

    public JobRequest getJobRequest() {
        return jobRequest;
    }

    public void setJobRequest(JobRequest jobRequest) {
        this.jobRequest = jobRequest;
    }

    public GraphDatabaseService getGraphDatabaseService() {
        return graphDatabaseService;
    }

    public void setGraphDatabaseService(GraphDatabaseService graphDatabaseService) {
        this.graphDatabaseService = graphDatabaseService;
    }

    public void startJob() {
        Assert.notNull(jobRequest, "jobRequest must not be null");
        Assert.notNull(graphDatabaseService, "graphDatabaseService must not be null");
        Assert.notNull(jobRequest.getCypherQuery(), "jobRequest.cypherQuery must not be null");
        Assert.notNull(jobRequest.getJobRequestType(), "jobRequest.jobRequestType must not be null");

        // Query and write to HDFS
        try {
            Path path = Writer.exportCypherQueryToHDFSParallel(graphDatabaseService,
                    jobRequest.getCypherQuery(), jobRequest.getJobRequestType());

            // Serialize processor message
            ProcessorMessage message = new ProcessorMessage(path.toString(),
                    jobRequest.getJobRequestType().toString().toLowerCase(), ProcessorMode.Unpartitioned);

            Gson gson = new Gson();
            String strMessage = gson.toJson(message);

            // Send message to the Spark graph processor
            Worker.sendMessage(strMessage);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
