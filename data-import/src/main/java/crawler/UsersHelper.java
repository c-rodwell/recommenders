package crawler;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 *
 * Elasticsearch helper methods for the users index
 *
 */
public class UsersHelper {

    private static final Logger LOG = Logger.getLogger(UsersHelper.class);

    public static Terms getUniqueTracks() {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(0);

        TermsAggregationBuilder aggregationBuilder =
                AggregationBuilders.terms("unique_tracks").field("track_mid").size(Constants.ES_MAX);
        builder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(builder);

        SearchResponse response;
        try {
            response = HighClient.getInstance().getClient().search(request);
            Aggregations aggr = response.getAggregations();
            return aggr.get("unique_tracks");
        } catch (IOException e) {
            LOG.error("Failed to fetch unique tracks from ES index='" + Constants.USERS_INDEX + "' : " + e.getMessage());
        }

        return null;

    }

    public static Terms getUniqueUsers() {

        SearchSourceBuilder user_builder = new SearchSourceBuilder();
        user_builder.size(0);

        TermsAggregationBuilder userAggregationBuilder =
                AggregationBuilders.terms("unique_users").field("username").size(Constants.ES_MAX);
        user_builder.aggregation(userAggregationBuilder);

        SearchRequest usersRequest = new SearchRequest(Constants.USERS_INDEX);
        usersRequest.source(user_builder);

        SearchResponse response;
        try {
            response = HighClient.getInstance().getClient().search(usersRequest);
            Aggregations users_aggr = response.getAggregations();
            return users_aggr.get("unique_users");
        } catch (IOException e) {
            LOG.error("Failed to fetch unique users from ES index='" + Constants.USERS_INDEX + "' : " + e.getMessage());
        }

        return null;

    }

    public static SearchHit getHit(String trackMid) {

        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response;
        try {
            response = HighClient.getInstance().getClient().search(request);
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length > 0) {
                return response.getHits().getHits()[0];
            }
        } catch (IOException e) {
            LOG.error("Failed to fetch track_mid='" + trackMid + "' from ES index='" + Constants.USERS_INDEX + "'");
        }

        return null;

    }

}
