package extension;


import config.ConfigurationLoader;
import jobs.PartitionedAnalysis;
import managers.JobManager;
import messaging.BatchWriterService;
import models.ErrorInfo;
import models.JobRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.http.HttpStatus;
import translation.Writer;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URISyntaxException;

@Path("/mazerunner")
public class MazerunnerService {

    private static final BatchWriterService batchWriterService = BatchWriterService.INSTANCE;

    public MazerunnerService(@Context GraphDatabaseService db)
    {
        batchWriterService.SetGraphDatabase(db);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/warmup")
    public Response warmup(@Context GraphDatabaseService db) {
        return Response.status(200)
                .entity("{ \"result\": \"success\" }")
                .type(MediaType.APPLICATION_JSON).build();
    }

    /**
     * Submit a PageRank job to Spark for processing.
     * @param relationship The name of the relationship type to extract from Neo4j.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/analysis/{type}/{relationship}")
    public Response analysis(@PathParam("type") String type, @PathParam("relationship") String relationship, @Context GraphDatabaseService db) {

        // Update relationship configuration
        if(relationship != null && !relationship.isEmpty())
            ConfigurationLoader.getInstance().setMazerunnerRelationshipType(relationship);

        // Export graph to HDFS and send message to Spark when complete
        try {
            Writer.dispatchJob(db, type);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return Response.status(200)
                .entity("{ \"result\": \"success\" }")
                .type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/group/analysis/{type}/{label}/{groupRelationship}/{targetRelationship}")
    public Response partitionedAnalysis(@PathParam("type") String type, @PathParam("label") String label, @PathParam("groupRelationship") String groupRelationship, @PathParam("targetRelationship") String targetRelationship, @Context GraphDatabaseService db) {



        // Export graph to HDFS and send message to Spark when complete
        PartitionedAnalysis partitionedAnalysis = new PartitionedAnalysis(type, label, groupRelationship, targetRelationship, db);
        partitionedAnalysis.analyzePartitions();

        return Response.status(200)
                .entity("{ \"result\": \"success\" }")
                .type(MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/jobs")
    public Response submitJob(String requestBody, @Context GraphDatabaseService db) {
        ObjectMapper objectMapper = new ObjectMapper();
        JobRequest jobRequest;
        try {
            jobRequest = objectMapper.readValue(requestBody, JobRequest.class);

            // Submit job request
            JobManager jobManager = new JobManager(jobRequest, db);
            jobManager.startJob();

        } catch (IOException e) {
            return new ErrorInfo("Request body is malformed", HttpStatus.BAD_REQUEST).toResponse();
        }

        return Response.status(200)
                .entity("{ \"result\": \"success\" }")
                .type(MediaType.APPLICATION_JSON).build();
    }

}
