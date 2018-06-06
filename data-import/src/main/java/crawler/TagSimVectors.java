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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

/**
 *
 * Creates the tag similarity vectors and inserts the vectors to Elasticsearch
 *
 */
public class TagSimVectors {

    private static final Logger LOG = Logger.getLogger(TagSimVectors.class);

    public static void createTagSimVectors() {

        LOG.info("Creating tag similarity vectors...");

        Terms uniqueTracksTerms = UsersHelper.getUniqueTracks();
        if (uniqueTracksTerms == null) {
            LOG.info("Unable to create tag similarity vectors. Failed to fetch unique tracks.");
            return;
        }

        HashMap<String, Integer> tagsMap = collectTags(uniqueTracksTerms);

        // store the tag names here
        ArrayList<String> tagnames = new ArrayList<>(Collections.nCopies(tagsMap.size(), "-"));
        for(Map.Entry<String, Integer> entry : tagsMap.entrySet()) {
            tagnames.set(entry.getValue(), entry.getKey());
        }

        // prepare bulk request to ES
        BulkRequest bulkRequest = new BulkRequest();
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String trackMid = b.getKeyAsString();
            try {
                String artist = UsersHelper.getHit(trackMid).getSourceAsMap().get("track_artist").toString();
                String trackName = UsersHelper.getHit(trackMid).getSourceAsMap().get("track_name").toString();
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

                // add to bulk request
                bulkRequest.add(new IndexRequest(Constants.TAG_SIM_INDEX, Constants.TAG_SIM_TYPE)
                        .source(esObj.toString(), XContentType.JSON));

            } catch(NullPointerException e) {
                LOG.error("Unable to fetch tag similarity vector for track mid='" + trackMid + "'");
            }
        }

        try {
            HighClient.getInstance().getClient().bulk(bulkRequest);
        } catch (IOException e) {
            LOG.error("Failed to bulk insert tag similarity vectors : " + e.getMessage());
        }

    }

    private static HashMap<String, Integer> collectTags(Terms uniqueTracksTerms) {

        HashMap<String, Integer> tagsMap = new HashMap<>();

        int index = 0;
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String trackMid = b.getKeyAsString();
            try {
                String artist = UsersHelper.getHit(trackMid).getSourceAsMap().get("track_artist").toString();
                String trackName = UsersHelper.getHit(trackMid).getSourceAsMap().get("track_name").toString();
                Collection<Tag> topTags = Track.getTopTags(artist, trackName, Constants.LASTFM_APIKey);
                for (Tag t : topTags ) {
                    if (!tagsMap.containsKey(t.getName())) {
                        tagsMap.put(t.getName(), index);
                        index++;
                    }
                }
            } catch (NullPointerException e) {
                LOG.error("Unable to fetch track name and artist for track mid='" + trackMid + "'");
            }
        }

        return tagsMap;

    }

}
