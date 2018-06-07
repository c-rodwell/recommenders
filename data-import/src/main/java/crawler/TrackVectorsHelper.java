package crawler;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class TrackVectorsHelper {

    public static final Logger LOG = Logger.getLogger(TrackVectorsHelper.class);

    /**
     * Checks whether trackMid is in trackvectors
     * @param trackMid
     * @return
     */
    public static boolean isInTrackVectors(String trackMid) {

        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.TRACK_VECTORS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        try {
            SearchResponse response = HighClient.getInstance().getClient().search(request);
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length > 1) {
                return true;
            }
        } catch (IOException e) {
            LOG.error("Failed to get track vector for track mid='" + trackMid + "'");
        }

        return false;

    }

}
