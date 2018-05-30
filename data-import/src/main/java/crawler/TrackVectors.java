package crawler;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class TrackVectors {

    private static final Logger LOG = Logger.getLogger(TrackVectors.class);

    public static void createTrackVectors() throws IOException {

        //get unique tracks from ES
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(0);
        TermsAggregationBuilder aggregationBuilder =
                AggregationBuilders.terms("unique_tracks").field("track_mid").size(Constants.num_tracks);
        builder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(builder);
        SearchResponse response = HighClient.getInstance().getClient().search(request);

        Aggregations aggr = response.getAggregations();
        Terms resp_terms = aggr.get("unique_tracks");

        //make hash of userids to ints for building the vector
        HashMap<String, Integer> usersToInts = new HashMap<>();
        SearchSourceBuilder user_builder = new SearchSourceBuilder();
        user_builder.size(0);
        TermsAggregationBuilder userAggregationBuilder =
                AggregationBuilders.terms("unique_users").field("username").size(Constants.num_users);
        user_builder.aggregation(userAggregationBuilder);
        SearchRequest usersRequest = new SearchRequest(Constants.USERS_INDEX);
        usersRequest.source(user_builder);
        SearchResponse usersResponse = HighClient.getInstance().getClient().search(usersRequest);
        Aggregations users_aggr = usersResponse.getAggregations();
        Terms usersRespRerms = users_aggr.get("unique_users");
        int userCount = 0;
        for (Terms.Bucket b: usersRespRerms.getBuckets()) {
            String username = b.getKeyAsString();
            usersToInts.put(username.toLowerCase(), userCount);
            userCount++;
        }

        int docId = 0;
        for (Terms.Bucket b: resp_terms.getBuckets()){
            String trackMid = b.getKeyAsString();
            System.out.println(trackMid);

            QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
            SearchRequest request2 = new SearchRequest(Constants.USERS_INDEX);
            request2.source(new SearchSourceBuilder().query(queryBuilder));
            SearchResponse response2 = HighClient.getInstance().getClient().search(request2);
            SearchHits hits = response2.getHits();
            JsonArray vector = new JsonArray();

            int[] playCountArr = new int[usersToInts.size()];
            for (SearchHit hit : hits) {
                int playCount = (int) hit.getSourceAsMap().get("track_playcount");
                String username = ((String) hit.getSourceAsMap().get("username")).toLowerCase();
                int index = 0;
                try{
                    index = usersToInts.get(username);
                    playCountArr[index] = playCount;
                    //playCountArr[usersToInts.get(username)] = playCount;
                } catch (NullPointerException e){
                    System.out.println("exception for name: "+username+" -> index: "+index);
                    System.out.println(e.getMessage());
                    throw e;
                }
            }
            for (int playCount : playCountArr) {
                System.out.println(playCount);
                vector.add(playCount);
            }
            JsonObject esObj = new JsonObject();
            esObj.addProperty("track_mid", trackMid);
            esObj.add("vector", vector);
            HighClient.getInstance().postJsonToES(Constants.TRACK_VECTORS_INDEX, Constants.TRACK_VECTORS_TYPE, docId, esObj);
            docId++;
        }


    }

}
