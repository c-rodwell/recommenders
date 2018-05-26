package crawler;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import de.umass.lastfm.Track;
import de.umass.lastfm.User;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CollectData {

    private static final Logger LOG = Logger.getLogger(CollectData.class);

    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;
    private static final String SCHEME = "http";
    private static final String ES_INDEX = "users";
    private static final String ES_TYPE = "user";

    private static final String APIKey = "685a323d182636518e80a296f620c8a2";

    public static void main(String[] args) {

        // Connect to Elasticsearch
        LOG.info("Connecting to Elasticsearch...");

        // High level client
        RestHighLevelClient hClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ES_HOST, ES_PORT, SCHEME)));

        // Low level client
        RestClient lClient = RestClient.builder(new HttpHost(ES_HOST, ES_PORT, SCHEME)).build();

        // Check if index exists
        boolean isIndexExists = false;
        try {
            Response response = lClient.performRequest("HEAD", "/" + ES_INDEX);
            int statusCode = response.getStatusLine().getStatusCode();
            isIndexExists = (statusCode == HttpStatus.SC_NOT_FOUND) ? false : true;
        } catch (IOException e) {
            LOG.info("Exception while checking if Elasticsearch index exists: " + e.getMessage());
        }

        // Delete existing index
        if (isIndexExists) {
            try {
                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(ES_INDEX);
                DeleteIndexResponse deleteIndexResponse = hClient.indices().delete(deleteIndexRequest);
                LOG.info("Delete successful? " + deleteIndexResponse.isAcknowledged());
            } catch (IOException e) {
                LOG.info("Exception while deleting Elasticsearch index: " + e.getMessage());
            }
        }

        // Create Elasticsearch index
        try {
            LOG.info("Creating Elasticsearch index...");
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(ES_INDEX);
            hClient.indices().create(createIndexRequest);
        } catch (IOException e) {
            LOG.error("Exception while creating index: " + e.getMessage());
        }

        // Put mapping
        putMapping(lClient);

        // Read the usernames
        try {
            LOG.info("Loading usernames and tracks' data for each user...");
            FileReader fileReader = new FileReader(System.getProperty("user.dir") + "/data-import/src/main/resources/users-small.json");
            JsonReader jsonReader = new JsonReader(fileReader);

            JsonParser parser = new JsonParser();
            JsonArray data = parser.parse(jsonReader).getAsJsonArray();

            int docId = 0;
            for (int i = 0; i < data.size(); i++) {
                JsonObject obj = data.get(i).getAsJsonObject();
                String username = obj.get("username").getAsString();
                // Get top tracks for user
                Collection<Track> topTracks = User.getTopTracks(username, APIKey);
                for (Track t : topTracks){
                    JsonObject esObj = new JsonObject();
                    esObj.addProperty("username", username);
                    esObj.addProperty("track_name", t.getName());
                    esObj.addProperty("track_playcount", t.getPlaycount());
                    esObj.addProperty("track_mid", t.getMbid());
                    sendObjToESIndex(hClient, esObj, docId);
                    docId++;
                }
            }

        } catch (FileNotFoundException e) {
            LOG.error("Exception while loading usernames: " + e.getMessage());
        }

        LOG.info("Done crawling for data.");

        try {
            hClient.close();
            lClient.close();
            LOG.info("Elasticsearch client is closed.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.exit(0);

    }

    private static void sendObjToESIndex(RestHighLevelClient client, JsonObject esObj, int id) {
        try {
            IndexRequest request = new IndexRequest(
                    ES_INDEX,
                    ES_TYPE,
                    Integer.toString(id));
            request.source(esObj.toString(), XContentType.JSON);
            client.index(request);
        } catch (IOException e) {
            LOG.error("Exception while posting object to Elasticsearch index " + ES_INDEX + " : " + e.getMessage());
        }
    }

    private static void putMapping(RestClient lClient) {

        // Create Elasticsearch mapping
        Map<String, Object> usernameMap = new HashMap<>();
        usernameMap.put("type", "text");
        usernameMap.put("fielddata", true);

        Map<String, Object> tracknameMap = new HashMap<>();
        tracknameMap.put("type", "text");
        tracknameMap.put("fielddata", true);

        Map<String, Object> trackmidMap = new HashMap<>();
        trackmidMap.put("type", "text");
        trackmidMap.put("fielddata", true);

        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put("username", usernameMap);
        propertiesMap.put("track_name", tracknameMap);
        propertiesMap.put("track_mid", trackmidMap);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", propertiesMap);

        try {
            Gson gson = new Gson();
            Map<String, String> params = Collections.emptyMap();
            StringEntity entity = new StringEntity(gson.toJson(mapping, Map.class).toString(), ContentType.APPLICATION_JSON);
            Response response = lClient.performRequest("PUT", "/" + ES_INDEX + "/_mapping/" + ES_TYPE, params, entity);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                LOG.info("Mapping is created.");
            } else {
                LOG.info("Failed to create mapping.");
            }
        } catch (IOException e) {
            LOG.info("Exception while putting mapping: " + e.getMessage());
        }

    }

}
