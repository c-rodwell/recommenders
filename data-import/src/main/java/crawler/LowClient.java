package crawler;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class LowClient {

    private static final Logger LOG = Logger.getLogger(LowClient.class);

    private static LowClient instance = null;
    private RestClient client = RestClient.builder(
            new HttpHost(Constants.ES_HOST, Constants.ES_PORT, Constants.SCHEME)).build();

    private LowClient() { }

    public static LowClient getInstance() {
        if (instance == null) {
            instance = new LowClient();
        }
        return instance;
    }

    public void close() throws IOException {
        client.close();
    }

    public boolean isIndexExists(String index) {

        boolean isIndexExists = false;
        try {
            Response response = client.performRequest("HEAD", "/" + index);
            int statusCode = response.getStatusLine().getStatusCode();
            isIndexExists = statusCode != HttpStatus.SC_NOT_FOUND;
        } catch (IOException e) {
            LOG.info("Failed to check if ES index='" + index + "' exists : " + e.getMessage());
        }
        return isIndexExists;

    }

    public void refreshIndex(String index) {

        try {
            Response response = client.performRequest("POST", "/" + index + "/_refresh");
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                LOG.info("Refreshed index='" + index + "'");
            } else {
                LOG.info("Failed to refresh index='" + index + "'");
            }
        } catch (IOException e) {
            LOG.info("Failed to refresh ES index '" + index + "' : " + e.getMessage());
        }

    }

    public void putMapping(Map<String, Object> mapping, String index, String type) {

        try {
            Gson gson = new Gson();
            Map<String, String> params = Collections.emptyMap();
            StringEntity entity = new StringEntity(gson.toJson(mapping, Map.class), ContentType.APPLICATION_JSON);
            Response response = client.performRequest("PUT", "/" + index + "/_mapping/" + type, params, entity);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                LOG.info("Mapping is created for index='" + index + "'");
            } else {
                LOG.info("Failed to create mapping index='" + index + "'");
            }
        } catch (IOException e) {
            LOG.info("Failed to create mapping | index='" + index + "', type='" + type + "' : " + e.getMessage());
        }

    }

}