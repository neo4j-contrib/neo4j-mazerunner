package extension;


import config.ConfigurationLoader;
import messaging.BatchWriterService;
import org.neo4j.graphdb.GraphDatabaseService;
import translation.Writer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
                .entity("{ result: 'success' }")
                .type(MediaType.APPLICATION_JSON).build();
    }

    /**
     * Submit a PageRank job to Spark for processing.
     * @param relationship The name of the relationship type to extract from Neo4j.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/analysis/{type}/{relationship}")
    public Response pageRank(@PathParam("type") String type, @PathParam("relationship") String relationship, @Context GraphDatabaseService db) {

        // Update relationship configuration
        if(relationship != null && !relationship.isEmpty())
            ConfigurationLoader.getInstance().setMazerunnerRelationshipType(relationship);

        // Export graph to HDFS and send message to Spark when complete
        try {
            Writer.startAgentJob(db, type);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return Response.status(200)
                .entity("{ result: 'success' }")
                .type(MediaType.APPLICATION_JSON).build();
    }

}
