package crawler;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 *
 * Elasticsearch helper methods for trackvectors index
 *
 */
public class TrackVectorsHelper {

    public static final Logger LOG = Logger.getLogger(TrackVectorsHelper.class);

    /**
     * Checks whether trackMid is in trackvectors
     */
    public static boolean isInTrackVectors(String trackMid) {

        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.TRACK_VECTORS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        try {
            SearchResponse response = HighClient.getInstance().getClient().search(request);
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length > 0) {
                return true;
            }
        } catch (IOException e) {
            LOG.error("Failed to check if track mid='" + trackMid + "' is in index='" + Constants.TRACK_VECTORS_INDEX + "'");
        }

        return false;

    }

    public static SearchHit getHit(String index, String trackMid) {

        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(index);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response;
        try {
            response = HighClient.getInstance().getClient().search(request);
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length > 0) {
                return response.getHits().getHits()[0];
            }
        } catch (IOException e) {
            LOG.error("Failed to fetch track_mid='" + trackMid + "' from ES index='" + index + "'");
        }

        return null;

    }

}
