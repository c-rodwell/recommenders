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
import java.util.ArrayList;
import java.util.HashMap;

public class TrackVectors {

    private static final Logger LOG = Logger.getLogger(TrackVectors.class);

    public static void createTrackSimVectors() throws IOException {

        LOG.info("Creating track similarity vectors...");

        Terms uniqueTracksTerms = UsersHelper.getUniqueTracks();

        HashMap<String, Integer> usersToInts = new HashMap<>();
        Terms uniqueUsersTerms = UsersHelper.getUniqueUsers();

        int userCount = 0;
        for (Terms.Bucket b : uniqueUsersTerms.getBuckets()) {
            String username = b.getKeyAsString();
            if (!usersToInts.containsKey(username)) {
                usersToInts.put(username, userCount);
                userCount++;
            }
        }

        // prepare bulk request to ES
        BulkRequest bulkRequest = new BulkRequest();

        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String trackMid = b.getKeyAsString();

            SearchHits hits = getPlayCountsOfTracks(trackMid);
            JsonArray vector = new JsonArray();

            int[] playCountArr = new int[usersToInts.size()];
            for (SearchHit hit : hits) {
                int playCount = (int) hit.getSourceAsMap().get("track_playcount");
                String username = ((String) hit.getSourceAsMap().get("username")).toLowerCase();
                if (usersToInts.containsKey(username)) {
                    playCountArr[usersToInts.get(username)] = playCount;
                }
            }

            biasEliminationBySD(playCountArr);

            for (int playCount : playCountArr) {
                vector.add(playCount);
            }

            JsonObject esObj = new JsonObject();
            esObj.addProperty("track_mid", trackMid);
            esObj.add("vector", vector);

            bulkRequest.add(new IndexRequest(Constants.TRACK_VECTORS_INDEX, Constants.TRACK_VECTORS_TYPE)
                    .source(esObj.toString(), XContentType.JSON));

        }
        
        try {
            HighClient.getInstance().getClient().bulk(bulkRequest);
        } catch (IOException e) {
            LOG.error("Failed to bulk insert track similarity vectors : " + e.getMessage());
        }

    }

    private static SearchHits getPlayCountsOfTracks(String trackMid) throws IOException {

        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder).size(Constants.ES_MAX));
        SearchResponse response = HighClient.getInstance().getClient().search(request);
        return response.getHits();

    }

    public static ArrayList<Integer> getTrackVector(String trackMid) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.TRACK_VECTORS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response = HighClient.getInstance().getClient().search(request);
        SearchHit[] hits = response.getHits().getHits();
        //there should be one result, but check if there are none or multiple
        if (hits.length == 0){
            return null;
        } else  if (hits.length == 1) {
            SearchHit hit = response.getHits().getHits()[0];
            return (ArrayList<Integer>) hit.getSourceAsMap().get("vector");
        } else{
            throw new IOException("invalid state: more than one track vector for same trackId");
        }
    }


    /**
     * Eliminates the popularity bias by normalize the track vector
     * using calculations based on sample standard deviation
     * @param arr int array[]
     * @return int array[]
     */
    private static int[] biasEliminationBySD(int [] arr ) {
        int sum = 0;
        int counter = 0;
        double sumOfSquares = 0;
        boolean flag = false;

        for (int i = 0; i < arr.length; i++) {
            if (arr[i] != 0) {
                flag = true;
                sum += arr[i];
                counter++;                     //number of non-zero entries in the vector
            }
        }
        if (flag) {
            double avg = sum / counter;
            // int avg = (int) (Math.round(average));


            for (int i = 0; i < arr.length; i++) {
                if (arr[i] != 0) {

                    sumOfSquares +=  Math.round(Math.pow((arr[i] - avg), 2));
                }
            }
            int sd = 0;
            try {
                if(counter == 1)
                    counter++;                                                        // to avoid unexpected division by zero
                sd = (int) (Math.ceil(Math.sqrt((sumOfSquares / (counter - 1)))));
            } catch (ArithmeticException e) {
                System.out.println("Division by zero " + e);
            }
            for (int i = 0; i < arr.length; i++) {
                if ((arr[i] != 0) && (arr[i] > 2 * sd)) {
                    arr[i] = arr[i] - 2 * sd;
                }
            }
        }

        return arr;
    }
}
