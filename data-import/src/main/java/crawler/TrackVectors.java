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

    public static void createTrackVectors(HashMap<String, Integer> usersToInts) throws IOException {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(0);
        TermsAggregationBuilder aggregationBuilder =
                AggregationBuilders.terms("unique_tracks").field("track_mid").size(10);
        builder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(builder);
        SearchResponse response = HighClient.getInstance().getClient().search(request);

        Aggregations aggr = response.getAggregations();
        Terms resp_terms = aggr.get("unique_tracks");

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
            //use userToInts passed as argument, created in loadUserTopTracks
            //alternatives - pass it through elasticsearch, or create it here
            int[] playCountArr = new int[usersToInts.size()];
            for (SearchHit hit : hits) {
                int playCount = (int) hit.getSourceAsMap().get("track_playcount");
                String username = (String) hit.getSourceAsMap().get("username");
                playCountArr[usersToInts.get(username)] = playCount;
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
