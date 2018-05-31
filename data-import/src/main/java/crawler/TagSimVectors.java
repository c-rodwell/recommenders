package crawler;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import de.umass.lastfm.Tag;
import de.umass.lastfm.Track;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class TagSimVectors {

    private static final Logger LOG = Logger.getLogger(TagSimVectors.class);

    public static void createTagSimVectors() throws IOException {

        LOG.info("Creating tag similarity vectors...");

        // TODO: Need partitions for larger datasets
        // get unique tracks from ES
        Terms uniqueTracksTerms = getUniqueTracks();

        HashMap<String, Integer> tagsMap = collectTags(uniqueTracksTerms);

        ArrayList<Integer> vector = new ArrayList<>(Collections.nCopies(tagsMap.size(), 0));

        // TODO: Terrible triple nested loop... Also get rid of this second pass by collecting tags in CollectData
        int docId = 0;
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            TopHits topHits = b.getAggregations().get("top");
            for (SearchHit hit : topHits.getHits().getHits()) {
                String artist = hit.getSourceAsMap().get("track_artist").toString();
                String trackName = hit.getSourceAsMap().get("track_name").toString();
                String trackMid = hit.getSourceAsMap().get("track_mid").toString();
                Collection<Tag> topTags = Track.getTopTags(artist, trackName, Constants.APIKey);
                int index = 0;
                for (Tag t : topTags) {
                    vector.add(tagsMap.get(t.getName()), t.getCount());
                }
                JsonObject esObj = new JsonObject();
                esObj.addProperty("track_mid", trackMid);
                esObj.add("vector", new Gson().toJsonTree(vector, new TypeToken<List<Integer>>() {
                }.getType()));
                HighClient.getInstance().postJsonToES(Constants.TAG_SIM_INDEX, Constants.TAG_SIM_TYPE, docId, esObj);
                docId++;
            }
        }

    }

    private static HashMap<String, Integer> collectTags(Terms uniqueTracksTerms) {

        HashMap<String, Integer> tagsMap = new HashMap<String, Integer>();

        // TODO: This triple nested for loop is terrible... can get rid of one by collecting tags CollectData
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            TopHits topHits = b.getAggregations().get("top");
            for (SearchHit hit : topHits.getHits().getHits()) {
                String artist = hit.getSourceAsMap().get("track_artist").toString();
                String trackName = hit.getSourceAsMap().get("track_name").toString();
                Collection<Tag> topTags = Track.getTopTags(artist, trackName, Constants.APIKey);
                int index = 0;
                for (Tag t : topTags) {
                    if (!tagsMap.containsKey(t.getName())) {
                        tagsMap.put(t.getName(), index);
                        index++;
                    }
                }
                LOG.info(hit.getSourceAsString());
            }
        }

        return tagsMap;

    }

    private static Terms getUniqueTracks() throws IOException {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(0);

        List<String> fieldDataFields = new ArrayList<String>();
        fieldDataFields.add("track_artist");
        fieldDataFields.add("track_name");

        TermsAggregationBuilder aggregationBuilder =
                AggregationBuilders.terms("unique_tracks").field("track_mid")
                        .subAggregation(AggregationBuilders.topHits("top").size(Constants.num_tracks));
        builder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(builder);
        SearchResponse response = HighClient.getInstance().getClient().search(request);

        Aggregations aggr = response.getAggregations();
        return aggr.get("unique_tracks");

    }

}
