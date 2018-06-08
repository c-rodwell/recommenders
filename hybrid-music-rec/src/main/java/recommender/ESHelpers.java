package recommender;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class ESHelpers {

    private static final Logger LOG = Logger.getLogger(ESHelpers.class);

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost(Constants.ES_HOST, Constants.ES_PORT, Constants.SCHEME)));

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
            return hits.length;
        } catch (IOException e) {
            LOG.error("Failed to get history size of username='" + username + "' from index='" + Constants.HISTORY_INDEX + "'");
        }

        return 0;
    }



    public static void close() throws IOException {
        client.close();
    }

    /*
    //get a user history by username and history number
    //this gets all the user histories and then just picks one - is that inefficient?
    public static HashMap<String, String> getHistoryForUser(String username, int historyNum) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("username", username);
        SearchRequest request = new SearchRequest(Constants.HISTORY_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response = client.search(request);
        SearchHit hit = response.getHits().getHits()[0]; //should be just one hit for the username
        HashMap historyObj = (HashMap) hit.getSourceAsMap().get("histories");
        //HashMap history1Obj = (HashMap) historyObj.get("1");
        return  (HashMap) historyObj.get(Integer.toString(historyNum));
    }
    */

    /*
    public static ArrayList<Integer> getTrackVector(String trackMid) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.TRACK_VECTORS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response = client.search(request);
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
    */

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
