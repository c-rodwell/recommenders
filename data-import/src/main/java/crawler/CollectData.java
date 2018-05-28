package crawler;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import de.umass.lastfm.Track;
import de.umass.lastfm.User;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestFactory;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
//import sun.net.www.http.HttpClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CollectData {

    private static final Logger LOG = Logger.getLogger(CollectData.class);

    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;
    private static final String SCHEME = "http";
    private static final String ES_INDEX = "users";
    private static final String ES_TYPE = "user";

    private static final String ES_username = "username";
    private static final String ES_trackname = "track_name";
    private static final String ES_trackplaycount = "track_playcount";
    private static final String ES_trackid = "track_mid";

    private static final String VECTOR_INDEX = "trackvectors";
    private static final String ES_listenvector = "listening_vector";

    private static final String USERS_FILE = "users-small.json";
    private static final String APIKey = "685a323d182636518e80a296f620c8a2";
    private static final int USERS_SIZE = 5000;

    public static void main(String[] args) {

        // Connect to Elasticsearch
        LOG.info("Connecting to Elasticsearch...");

        // High level client
        RestHighLevelClient hClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ES_HOST, ES_PORT, SCHEME)));

        // Low level client
        RestClient lClient = RestClient.builder(new HttpHost(ES_HOST, ES_PORT, SCHEME)).build();

        createNewIndex(lClient, hClient, ES_INDEX);
        createNewIndex(lClient, hClient, VECTOR_INDEX);

        // Put mapping
        putMapping(lClient);

        //map the unique users to ints, for making user listening vectors. will this get too big?
        HashMap<String, Integer> usersToInts = new HashMap<>();
        HashSet<String> trackIds = new HashSet<>();

        // Read the usernames
        try {
            LOG.info("Loading usernames and tracks' data for each user...");
            InputStream is = CollectData.class.getClassLoader().getResourceAsStream(USERS_FILE);
            JsonReader jsonReader = new JsonReader(new InputStreamReader(is));

            JsonParser parser = new JsonParser();
            JsonArray data = parser.parse(jsonReader).getAsJsonArray();

            int docId = 0;
            for (int i = 0; i < 10; i++) { //should be i<data.size, shrink for testing later parts
                JsonObject obj = data.get(i).getAsJsonObject();
                String username = obj.get("username").getAsString();
                usersToInts.put(username, i);
                // Get top tracks for user
                Collection<Track> topTracks = User.getTopTracks(username, APIKey);
                for (Track t : topTracks){
                    JsonObject esObj = new JsonObject();
                    esObj.addProperty(ES_username, username);
                    esObj.addProperty(ES_trackname, t.getName());
                    esObj.addProperty(ES_trackplaycount, t.getPlaycount());
                    esObj.addProperty(ES_trackid, t.getMbid());
                    sendToESIndex(hClient, ES_INDEX, esObj, docId);
                    docId++;
                    trackIds.add(t.getMbid());
                }
            }

        } catch (Exception e) {
            LOG.error("Exception while loading usernames: " + e.getMessage());
        }

        //make vectors for tracks


        for (String mid : trackIds){
            if (mid == ""){ continue;}
            try {
                int[] listenArray = new int[usersToInts.size()];

    //                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    //                sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
    //                sourceBuilder.from(0);
    //                sourceBuilder.size(5);
    //                sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));


                //query for the track id
                SearchRequest userTracksRequest = new SearchRequest(ES_INDEX);
                SearchSourceBuilder tracksRequestBuilder = new SearchSourceBuilder();
                tracksRequestBuilder.query(QueryBuilders.termQuery("_source."+ES_trackid, mid));
                tracksRequestBuilder.from(0);
                tracksRequestBuilder.size(100);
                tracksRequestBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
                userTracksRequest.source(tracksRequestBuilder);
                SearchResponse namesResponse = hClient.search(userTracksRequest);
                SearchHits hits = namesResponse.getHits();
                SearchHit[] hitsArr = hits.getHits();

                URL url = new URL("http://localhost:9200/users/_search?q="+ES_trackid+":"+mid);
                //HttpClient client = HttpClient.New(url);
                //HttpRequest req =
                //HttpClient.New("http://localhost:9200/users/_search?q="+ES_trackid+":"+mid);

                //URL url = new URL("http://example.com");
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setRequestProperty("Content-Type", "application/json");
                InputStream responsestream = connection.getInputStream();


                //fill the array
    //                for (SearchHit hit : hitsArr){
    //                    String username = hit.field(ES_username).getValue();
    //                    String listencount = hit.field(ES_trackplaycount).getValue();
    //                    listenArray[usersToInts.get(username)] = Integer.getInteger(listencount);
    //                }
                //put the array in elasticSearch
                JsonObject esObj = new JsonObject();
                esObj.addProperty(ES_trackid, mid);
                esObj.addProperty(ES_listenvector, listenArray[0]); //I want to put all of listenarray but it doesn't allow vector
                sendToESIndex(hClient, VECTOR_INDEX, esObj, 0);
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
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

    public static void createNewIndex(RestClient lClient, RestHighLevelClient hClient, String IndexName) {
        // Check if index exists
        boolean isIndexExists = false;
        try {
            Response response = lClient.performRequest("HEAD", "/" + IndexName);
            int statusCode = response.getStatusLine().getStatusCode();
            isIndexExists = (statusCode == HttpStatus.SC_NOT_FOUND) ? false : true;
        } catch (IOException e) {
            LOG.info("Exception while checking if Elasticsearch index exists: " + e.getMessage());
        }

        // Delete existing index
        if (isIndexExists) {
            try {
                LOG.info("Deleting existing " + IndexName + " index..");
                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(IndexName);
                DeleteIndexResponse deleteIndexResponse = hClient.indices().delete(deleteIndexRequest);
                LOG.info("Delete successful? " + deleteIndexResponse.isAcknowledged());
            } catch (IOException e) {
                LOG.info("Exception while deleting Elasticsearch index: " + e.getMessage());
            }
        }

        // Create Elasticsearch index
        try {
            LOG.info("Creating new Elasticsearch index: " + IndexName);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(IndexName);
            hClient.indices().create(createIndexRequest);
        } catch (IOException e) {
            LOG.error("Exception while creating index: " + e.getMessage());
        }
    }

    private static void sendToESIndex(RestHighLevelClient client, String index, JsonObject esObj, int id) {
        try {
            IndexRequest request = new IndexRequest(
                    index,
                    ES_TYPE,
                    Integer.toString(id));
            request.source(esObj.toString(), XContentType.JSON);
            client.index(request);
        } catch (IOException e) {
            LOG.error("Exception while posting object to Elasticsearch index " + ES_INDEX + " : " + e.getMessage());
        }
    }

//    private static void sendObjToESIndex(RestHighLevelClient client, JsonObject esObj, int id) {
//        try {
//            IndexRequest request = new IndexRequest(
//                    ES_INDEX,
//                    ES_TYPE,
//                    Integer.toString(id));
//            request.source(esObj.toString(), XContentType.JSON);
//            client.index(request);
//        } catch (IOException e) {
//            LOG.error("Exception while posting object to Elasticsearch index " + ES_INDEX + " : " + e.getMessage());
//        }
//    }

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
