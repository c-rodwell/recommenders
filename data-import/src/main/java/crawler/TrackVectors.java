package crawler;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TrackVectors {

    private static final Logger LOG = Logger.getLogger(TrackVectors.class);

    public static void createTrackVectors() throws IOException {

        // First, get max user_index. This tells us the count of the users and the size of the vector
        int userCount = (int) getCountOfUsers();
        LOG.info("Count of users = " + userCount);

        // Then, get total hits in users index
        // This tells us the number of search queries we need
        // ES returns a max of 10000 per search query
        int totalHits = getTotalHits();
        LOG.info("Total hits = " + totalHits);

        // Compute number of partition
        int numberOfPartitions = (int) Math.ceil((double) totalHits / 10000.0);

        // Now, create the track vectors
        LOG.info("Now creating track vectors...");
        int docId = 0; // doc id in ES
        for (int i = 0; i < numberOfPartitions; i++) { // do as many search queries as needed
            Terms uniqueTracksTerms = getUniqueTracks(i, numberOfPartitions);

            for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
                String trackMid = b.getKeyAsString();
                SearchHits hits = getUsersWhoPlayedTrack(trackMid);

                // initialize vector for this track with zeroes
                ArrayList<Integer> vector = new ArrayList<>(Collections.nCopies(userCount, 0));
                for (SearchHit hit : hits) {
                    int playCount = (int) hit.getSourceAsMap().get("track_playcount");
                    int userIndex = (int) hit.getSourceAsMap().get("user_index"); // user index in the track vector
                    vector.add(userIndex, playCount);
                }

                JsonObject esObj = new JsonObject();
                esObj.addProperty("track_mid", trackMid);
                esObj.add("vector", new Gson().toJsonTree(vector, new TypeToken<List<Integer>>() {}.getType()));
                HighClient.getInstance().postJsonToES(Constants.TRACK_VECTORS_INDEX, Constants.TRACK_VECTORS_TYPE, docId, esObj);
                docId++;
            }
        }
        LOG.info("Done.");
    }

    private static double getCountOfUsers() throws IOException {

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        MaxAggregationBuilder mab = AggregationBuilders.max("max_user_index").field("user_index");
        ssb.aggregation(mab);

        SearchRequest userCountReq = new SearchRequest(Constants.USERS_INDEX);
        userCountReq.source(ssb);

        SearchResponse userCountResp = HighClient.getInstance().getClient().search(userCountReq);
        Aggregations userCountAgg = userCountResp.getAggregations();
        Max m = userCountAgg.get("max_user_index");

        return m.getValue();

    }

    private static Terms getUniqueTracks(int partition, int totalPartitions) throws IOException {

        SearchSourceBuilder ssb = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggBuilder = AggregationBuilders.terms("unique_tracks").field("track_mid").size(10000);
        termsAggBuilder.includeExclude(new IncludeExclude(partition, totalPartitions));
        ssb.aggregation(termsAggBuilder);

        SearchRequest uniqueTracksReq = new SearchRequest(Constants.USERS_INDEX);
        uniqueTracksReq.source(ssb);
        SearchResponse uniqueTracksResp = HighClient.getInstance().getClient().search(uniqueTracksReq);

        Aggregations uniqueTracksAggr = uniqueTracksResp.getAggregations();

        return uniqueTracksAggr.get("unique_tracks");

    }

    private static int getTotalHits() throws IOException {

        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequest trackUsersReq = new SearchRequest(Constants.USERS_INDEX);
        trackUsersReq.source(new SearchSourceBuilder().query(qb));
        SearchResponse trackUsersResp = HighClient.getInstance().getClient().search(trackUsersReq);

        return (int) trackUsersResp.getHits().totalHits;

    }

    private static SearchHits getUsersWhoPlayedTrack(String trackMid) throws IOException {

        QueryBuilder qb = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest trackUsersReq = new SearchRequest(Constants.USERS_INDEX);
        trackUsersReq.source(new SearchSourceBuilder().query(qb));
        SearchResponse trackUsersResp = HighClient.getInstance().getClient().search(trackUsersReq);

        return trackUsersResp.getHits();

    }

}
