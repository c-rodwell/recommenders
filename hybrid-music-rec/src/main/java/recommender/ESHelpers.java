package recommender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;


public class ESHelpers {

    private static final Logger LOG = Logger.getLogger(ESHelpers.class);

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost(Constants.ES_HOST, Constants.ES_PORT, Constants.SCHEME)));

    /**
     * Checks whether username is in users
     */
    public static boolean isInUsers(String username) {

        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("username", username);
        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        try {
            SearchResponse response = client.search(request);
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length > 0) {
                return true;
            }
        } catch (IOException e) {
            LOG.error("Failed to check if user='" + username + "' is in index='" + Constants.USERS_INDEX + "'");
        }

        return false;

    }

    public static ArrayList<Integer> getVector(String index, String trackMid) {

        ArrayList<Integer> vector = new ArrayList<>();
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(index);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        try {
            SearchResponse response = client.search(request);
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length > 0) {
                SearchHit hit = response.getHits().getHits()[0];
                vector = (ArrayList<Integer>) hit.getSourceAsMap().get("vector");
            }
        } catch (IOException e) {
            LOG.error("Failed to get vector of track mid='" + trackMid + "' from index='" + index + "'");
        }

        return vector;

    }

    public static HashMap<String, String> getHistoryForUser(String username, int historyNum) {

        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("username", username);
        SearchRequest request = new SearchRequest(Constants.HISTORY_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        try {
            SearchResponse response = client.search(request);
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length > 0) {
                SearchHit hit = response.getHits().getHits()[0];
                HashMap historyObj = (HashMap) hit.getSourceAsMap().get("histories");
                return  (HashMap) historyObj.get(Integer.toString(historyNum));
            }
        } catch (IOException e) {
            LOG.error("Failed to get source map of username='" + username + "' from index='" + Constants.HISTORY_INDEX + "'");
        }

        return null;
    }

    public static int getUserHistorySize(String username) {

        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("username", username);
        SearchRequest request = new SearchRequest(Constants.HISTORY_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        try {
            SearchResponse response = client.search(request);
            SearchHit[] hits = response.getHits().getHits();
            HashMap historyObj = (HashMap) hits[0].getSourceAsMap().get("histories");
            int count = 0;
            for (int i = 1; i <= historyObj.size(); i++) {
                String index = Integer.toString(i);
                String obj = historyObj.get(index).toString();
                if (!obj.equals("{}")) {
                    count++;
                }
            }

            return count;
        } catch (IOException e) {
            LOG.error("Failed to get history size of username='" + username + "' from index='" + Constants.HISTORY_INDEX + "'");
        }

        return 0;
    }



    public static void close() throws IOException {
        client.close();
    }

    public static Terms getTracksWhichHaveVectors() {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(0);

        TermsAggregationBuilder aggregationBuilder =
                AggregationBuilders.terms("unique_tracks").field("track_mid").size(Constants.ES_MAX);
        builder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(Constants.TRACK_VECTORS_INDEX);
        request.source(builder);

        SearchResponse response;
        try {
            response = client.search(request);
            Aggregations aggr = response.getAggregations();
            return aggr.get("unique_tracks");
        } catch (IOException e) {
            LOG.error("Failed to fetch unique tracks from track vectors index='" + Constants.TRACK_VECTORS_INDEX + "' : " + e.getMessage());
        }

        return null;

    }

}