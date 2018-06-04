package recommender;

import crawler.Constants;
import crawler.HighClient;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class ESHelpers {

    private static final Logger LOG = Logger.getLogger(ESHelpers.class);

    public static ArrayList<Integer> getTagVector(String trackMid) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.TAG_SIM_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response = HighClient.getInstance().getClient().search(request);
        SearchHit[] hits = response.getHits().getHits();
        // there should be one result, but check if there are none or multiple
        if (hits.length == 0){
            return null;
        } else  if (hits.length == 1) {
            SearchHit hit = response.getHits().getHits()[0];
            return (ArrayList<Integer>) hit.getSourceAsMap().get("vector");
        } else{
            throw new IOException("invalid state: more than one track vector for same trackId");
        }

    }
}
