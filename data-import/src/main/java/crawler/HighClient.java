package crawler;

import com.google.gson.JsonObject;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class HighClient {

    private static final Logger LOG = Logger.getLogger(HighClient.class);

    private static HighClient instance = null;
    private RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost(Constants.ES_HOST, Constants.ES_PORT, Constants.SCHEME)));

    private HighClient() {
    }

    public static HighClient getInstance() {
        if (instance == null) {
            instance = new HighClient();
        }
        return instance;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public void close() throws IOException {
        client.close();
    }

    public void createIndex(String index) {

        try {
            LOG.info("Creating new Elasticsearch index: " + index);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
            client.indices().create(createIndexRequest);
        } catch (IOException e) {
            LOG.error("Exception while creating index '" + index + "': " + e.getMessage());
        }

    }

    public void deleteIndex(String index) {

        try {
            LOG.info("Deleting existing " + index + " index..");
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
            DeleteIndexResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest);
            LOG.info("Delete successful? " + deleteIndexResponse.isAcknowledged());
        } catch (IOException e) {
            LOG.info("Exception while deleting ES '" + index + "': " + e.getMessage());
        }

    }

    public void postJsonToES(String index, String type, int id, JsonObject esObj) {

        try {
            IndexRequest request = new IndexRequest(
                    index,
                    type,
                    Integer.toString(id));
            request.source(esObj.toString(), XContentType.JSON);
            client.index(request);
        } catch (IOException e) {
            LOG.error("Exception while posting object to ES index '" + index + "': " + e.getMessage());
        }

    }

}
