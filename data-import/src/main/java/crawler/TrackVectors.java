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
import java.util.HashMap;

public class TrackVectors {

    private static final Logger LOG = Logger.getLogger(TrackVectors.class);

    public static void createTrackSimVectors() throws IOException {

        LOG.info("Creating track similarity vectors...");

        // TODO: Need partitions for larger datasets
        // get unique tracks from ES
        Terms uniqueTracksTerms = getUniqueTracks();


        // TODO: Need partitions for larger datasets
        // make hash of userids to ints for building the vector
        HashMap<String, Integer> usersToInts = new HashMap<>();
        Terms uniqueUsersTerms = getUniqueUsers();

        int userCount = 0;
        for (Terms.Bucket b : uniqueUsersTerms.getBuckets()) {
            String username = b.getKeyAsString();
            usersToInts.put(username.toLowerCase(), userCount);
            userCount++;
        }

        int docId = 0;
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String trackMid = b.getKeyAsString();

            SearchHits hits = getPlaycountsOfTracks(trackMid);
            JsonArray vector = new JsonArray();

            int[] playCountArr = new int[usersToInts.size()];
            for (SearchHit hit : hits) {
                int playCount = (int) hit.getSourceAsMap().get("track_playcount");
                String username = ((String) hit.getSourceAsMap().get("username")).toLowerCase();
                playCountArr[usersToInts.get(username)] = playCount;
            }

            for (int playCount : playCountArr) {
                vector.add(playCount);
            }

            JsonObject esObj = new JsonObject();
            esObj.addProperty("track_mid", trackMid);
            esObj.add("vector", vector);
            HighClient.getInstance().postJsonToES(Constants.TRACK_VECTORS_INDEX, Constants.TRACK_VECTORS_TYPE, docId, esObj);
            docId++;
        }

    }

    private static Terms getUniqueTracks() throws IOException {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(0);

        TermsAggregationBuilder aggregationBuilder =
                AggregationBuilders.terms("unique_tracks").field("track_mid").size(Constants.num_tracks);
        builder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(builder);
        SearchResponse response = HighClient.getInstance().getClient().search(request);

        Aggregations aggr = response.getAggregations();
        return aggr.get("unique_tracks");

    }

    private static Terms getUniqueUsers() throws IOException {

        SearchSourceBuilder user_builder = new SearchSourceBuilder();
        user_builder.size(0);

        TermsAggregationBuilder userAggregationBuilder =
                AggregationBuilders.terms("unique_users").field("username").size(Constants.num_users);
        user_builder.aggregation(userAggregationBuilder);

        SearchRequest usersRequest = new SearchRequest(Constants.USERS_INDEX);
        usersRequest.source(user_builder);

        SearchResponse usersResponse = HighClient.getInstance().getClient().search(usersRequest);

        Aggregations users_aggr = usersResponse.getAggregations();
        return users_aggr.get("unique_users");

    }

    private static SearchHits getPlaycountsOfTracks(String trackMid) throws IOException {

        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response = HighClient.getInstance().getClient().search(request);
        return response.getHits();

    }

}
