package crawler;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import de.umass.lastfm.Tag;
import de.umass.lastfm.Track;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class TagSimVectors {

    private static final Logger LOG = Logger.getLogger(TagSimVectors.class);

    public static void createTagSimVectors() throws IOException {

        LOG.info("Creating tag similarity vectors...");

        Terms uniqueTracksTerms = getUniqueTracks();
        HashMap<String, Integer> tagsMap = collectTags(uniqueTracksTerms);

        ArrayList<String> tagnames = new ArrayList<>(Collections.nCopies(tagsMap.size(), "blank"));
        for (Map.Entry<String, Integer> entry : tagsMap.entrySet()) {
            tagnames.set(entry.getValue(), entry.getKey());
        }

        BulkRequest bulkRequest = new BulkRequest();
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String trackMid = b.getKeyAsString();
            String artist = getHit(trackMid).getSourceAsMap().get("track_artist").toString();
            String trackName = getHit(trackMid).getSourceAsMap().get("track_name").toString();
            Collection<Tag> topTags = Track.getTopTags(artist, trackName, Constants.LASTFM_APIKey);

            ArrayList<Integer> vector = new ArrayList<>(Collections.nCopies(tagsMap.size(), 0));
            for (Tag t : topTags) {
                if (tagsMap.containsKey(t.getName())) {
                    vector.set(tagsMap.get(t.getName()), t.getCount());
                }
            }

            JsonObject esObj = new JsonObject();
            esObj.addProperty("track_mid", trackMid);
            esObj.addProperty("track_artist", artist);
            esObj.addProperty("track_name", trackName);
            esObj.add("vector",
                    new Gson().toJsonTree(vector, new TypeToken<List<Integer>>() {
                    }.getType()));
            esObj.add("tagnames",
                    new Gson().toJsonTree(tagnames, new TypeToken<List<Integer>>() {
                    }.getType()));
            bulkRequest.add(new IndexRequest(Constants.TAG_SIM_INDEX, Constants.TAG_SIM_TYPE)
                    .source(esObj.toString(), XContentType.JSON));
        }

        if (bulkRequest.estimatedSizeInBytes() > 0) {
            try {
                HighClient.getInstance().getClient().bulk(bulkRequest);
            } catch (IOException e) {
                LOG.error("Exception on ES bulk insert : " + e.getMessage());
            }
        }

    }

    private static HashMap<String, Integer> collectTags(Terms uniqueTracksTerms) throws IOException {

        HashMap<String, Integer> tagsMap = new HashMap<String, Integer>();
// top 20 tags, excluding "seen live" tag us subjective result 
        tagsMap.put("rock", 0);
        tagsMap.put("electronic", 1);
        tagsMap.put("alternative", 2);
        tagsMap.put("indie", 3);
        tagsMap.put("pop", 4);
        tagsMap.put("female vocalists", 5);
        tagsMap.put("metal", 6);
        tagsMap.put("alternative rock", 7);
        tagsMap.put("classic rock", 8);
        tagsMap.put("jazz", 9);
        tagsMap.put("experimental", 10);
        tagsMap.put("classic rock", 11);
        tagsMap.put("ambient", 12);
        tagsMap.put("folk", 13);
        tagsMap.put("punk", 14);
        tagsMap.put("indie rock", 15);
        tagsMap.put("Hip-Hop", 16);
        tagsMap.put("hard rock", 17);
        tagsMap.put("instrumental", 18);
        tagsMap.put("singer-songwriter", 19);

        int index = 0;
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String trackMid = b.getKeyAsString();
            String artist = getHit(trackMid).getSourceAsMap().get("track_artist").toString();
            String trackName = getHit(trackMid).getSourceAsMap().get("track_name").toString();
            Collection<Tag> topTags = Track.getTopTags(artist, trackName, Constants.LASTFM_APIKey);
            for (Tag t : topTags) {
                if (!tagsMap.containsKey(t.getName())) {
                    tagsMap.put(t.getName(), index);
                    index++;
                }
            }
        }

        return tagsMap;

    }

    private static Terms getUniqueTracks() throws IOException {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(0);

        TermsAggregationBuilder aggregationBuilder
                = AggregationBuilders.terms("unique_tracks").field("track_mid").size(Constants.num_tracks);
        builder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(builder);
        SearchResponse response = HighClient.getInstance().getClient().search(request);

        Aggregations aggr = response.getAggregations();
        return aggr.get("unique_tracks");

    }

    public static SearchHit getHit(String trackMid) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("track_mid", trackMid);
        SearchRequest request = new SearchRequest(Constants.USERS_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response = HighClient.getInstance().getClient().search(request);
        SearchHit[] hits = response.getHits().getHits();
        //there should be one result, but check if there are none or multiple
        if (hits.length == 0) {
            return null;
        } else if (hits.length == 1) {
            SearchHit hit = response.getHits().getHits()[0];
            return hit;
        } else {
            throw new IOException("invalid state: more than one track vector for same trackId");
        }
    }

}
