package Extension;


import Messaging.BatchWriterService;
import Translation.Writer;
import com.google.gson.Gson;
import org.neo4j.graphdb.GraphDatabaseService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
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

        Gson gson = new Gson();

        return Response.status(200)
                .entity(gson.toJson("{ result: 'success' }"))
                .type(MediaType.APPLICATION_JSON).build();
    }


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/pagerank")
    public Response pageRank(@Context GraphDatabaseService db) {

        // Export graph to HDFS and send message to Spark when complete
        try {
            Writer.exportSubgraphToHDFS(db);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        Gson gson = new Gson();

        return Response.status(200)
                .entity(gson.toJson("{ result: 'success' }"))
                .type(MediaType.APPLICATION_JSON).build();
    }

}
