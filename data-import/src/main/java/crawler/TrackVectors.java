package crawler;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Creates the track vectors and the normalized track vectors and inserts the vectors to Elasticsearch
 *
 */
public class TrackVectors {

    private static final Logger LOG = Logger.getLogger(TrackVectors.class);

    /**
     * Creates the track feature vectors and bulk inserts them to ES
     */
    public static void createTrackSimVectors() {

        LOG.info("Creating track similarity vectors...");

        Terms uniqueTracksTerms = UsersHelper.getUniqueTracks();

        HashMap<String, Integer> usersToInts = new HashMap<>();

        // Get all the usernames associated with each track
        int userCount = 0;
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String trackMid = b.getKeyAsString();
            try {
                SearchHits hits = getPlayCountsOfTracks(trackMid);
                for (SearchHit hit : hits) {
                    String username = (String) hit.getSourceAsMap().get("username");
                    if (!usersToInts.containsKey(username)) {
                        usersToInts.put(username, userCount);
                        userCount++;
                    }
                }
            } catch (NullPointerException e) {
                LOG.error("Null result with track mid='" + trackMid + "'");
            }
        }

        LOG.info("Begin bulk insert of track similarity vectors to ES...");
        // prepare bulk request to ES
        BulkRequest bulkRequest = new BulkRequest();
        BulkRequest normBulkReq = new BulkRequest();
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String trackMid = b.getKeyAsString();
            try {
                SearchHits hits = getPlayCountsOfTracks(trackMid);
                JsonArray vector = new JsonArray();

                int[] playCountArr = new int[usersToInts.size()];
                for (SearchHit hit : hits) {
                    int playCount = (int) hit.getSourceAsMap().get("track_playcount");
                    String username = (String) hit.getSourceAsMap().get("username");
                    if (usersToInts.containsKey(username)) {
                        playCountArr[usersToInts.get(username)] = playCount;
                    }
                }

                for (int playCount : playCountArr) {
                    vector.add(playCount);
                }

                JsonObject esObj = new JsonObject();
                esObj.addProperty("track_mid", trackMid);
                esObj.add("vector", vector);

                bulkRequest.add(new IndexRequest(Constants.TRACK_VECTORS_INDEX, Constants.TRACK_VECTORS_TYPE)
                        .source(esObj.toString(), XContentType.JSON));

                int arr[] = biasEliminationBySD(playCountArr);

                JsonArray normVector = new JsonArray();
                for (int playCount : arr) {
                    normVector.add(playCount);
                }

                JsonObject normEsObj = new JsonObject();
                normEsObj.addProperty("track_mid", trackMid);
                normEsObj.add("vector", normVector);
                normBulkReq.add(new IndexRequest(Constants.NORMALIZED_VECTOR2_INDEX, Constants.NORMALIZED_VECTOR_2_TYPE)
                        .source(normEsObj.toString(), XContentType.JSON));
            } catch (NullPointerException e) {
                LOG.error("Failed to add track/normalized track vector for track mid='" + trackMid + "'");
            }

        }

        try {
            HighClient.getInstance().getClient().bulk(bulkRequest);
            HighClient.getInstance().getClient().bulk(normBulkReq);
        } catch (IOException e) {
            LOG.error("Failed to bulk insert track similarity vectors : " + e.getMessage());
        }

    }

    /**
     * Get the play counts of the tracks
     */
    private static SearchHits getPlayCountsOfTracks(String trackMid) {

        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder).size(Constants.ES_MAX));
        SearchResponse response;
        try {
            response = HighClient.getInstance().getClient().search(request);
            return response.getHits();
        } catch (IOException e) {
            LOG.error("Failed to get play count of track mbid='" + trackMid + "'");
        }

        return null;

    }

    /**
     * Eliminates the popularity bias by normalize the track vector using
     * calculations based on sample standard deviation
     *
     * @param arr int array[]
     * @return int array[]
     */
    private static int[] biasEliminationBySD(int[] arr) {
        int sum = 0;
        int counter = 0;
        double sumOfSquares = 0;
        boolean flag = false;

        for (int i = 0; i < arr.length; i++) {
            if (arr[i] != 0) {
                flag = true;
                sum += arr[i];
                counter++;  // number of non-zero entries in the vector
            }
        }
        if (flag) {
            double avg = sum / counter;

            for (int i = 0; i < arr.length; i++) {
                if (arr[i] != 0) {
                    sumOfSquares += Math.round(Math.pow((arr[i] - avg), 2));
                }
            }
            int sd = 0;
            try {
                if (counter == 1) {
                    counter++;  // to avoid unexpected division by zero
                }
                sd = (int) (Math.ceil(Math.sqrt((sumOfSquares / (counter - 1)))));
            } catch (ArithmeticException e) {
                System.out.println("Division by zero " + e);
            }

            for (int i = 0; i < arr.length; i++) {
                if ((arr[i] != 0) && (arr[i] - avg > sd)) {
                    arr[i] = (int)(arr[i] -  Math.round(1.5*sd));
                }
            }
        }

        return arr;
    }

    /**
     * Elasticsearch mapping
     */
    public static Map<String, Object> getMapping() {

        Map<String, Object> trackmidMap = new HashMap<>();
        trackmidMap.put("type", "keyword");

        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put("track_mid", trackmidMap);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", propertiesMap);

        return mapping;

    }

}