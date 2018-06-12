package crawler;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import de.umass.lastfm.Tag;
import de.umass.lastfm.Track;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

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

        // top 20 vectors from last.fm
        HashMap<String, Integer> tagsMap = new HashMap<>();
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
        tagsMap.put("ambient", 11);
        tagsMap.put("folk", 12);
        tagsMap.put("punk", 13);
        tagsMap.put("indie rock", 14);
        tagsMap.put("Hip-Hop", 15);
        tagsMap.put("hard rock", 16);
        tagsMap.put("instrumental", 17);
        tagsMap.put("singer-songwriter", 18);
        tagsMap.put("black metal", 19);

        // store the tag names here
        ArrayList<String> tagnames = new ArrayList<>(Collections.nCopies(tagsMap.size(), "-"));
        for(Map.Entry<String, Integer> entry : tagsMap.entrySet()) {
            tagnames.set(entry.getValue(), entry.getKey());
        }

        LOG.info("Begin bulk insert of tag similarity vectors to ES...");
        LOG.info("This takes the longest... please wait");
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            // prepare bulk request to ES
            BulkRequest bulkRequest = new BulkRequest();
            String trackMid = b.getKeyAsString();
            try {
                String artist = UsersHelper.getHit(trackMid).getSourceAsMap().get("track_artist").toString();
                String trackName = UsersHelper.getHit(trackMid).getSourceAsMap().get("track_name").toString();
                Collection<Tag> topTags = Track.getTopTags(artist, trackName, Constants.LASTFM_APIKey);

                ArrayList<Integer> vector = new ArrayList<>(Collections.nCopies(tagsMap.size(), 0));
                Iterator<Tag> it = topTags.iterator();
                int i = 0;
                while (it.hasNext() && i < 5) {
                    Tag t = it.next();
                    if (tagsMap.containsKey(t.getName())) {
                        vector.set(tagsMap.get(t.getName()), t.getCount());
                    }
                }

                if (!isAllZeroes(vector)) {

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
                } else {
                    // the tag vectors for some tracks will inevitably be all zeroes if the none of their tags match with the top 20
                    // need to skip those tracks here, and also delete them from the the trackvectors and normalized-vector-2 indices
                    String esId = TrackVectorsHelper.getHit(Constants.TRACK_VECTORS_INDEX, trackMid).getId();
                    String normEsId = TrackVectorsHelper.getHit(Constants.NORMALIZED_VECTOR2_INDEX, trackMid).getId();
                    DeleteRequest delRequest = new DeleteRequest(Constants.TRACK_VECTORS_INDEX, Constants.TRACK_VECTORS_TYPE, esId);
                    DeleteRequest normDelRequest = new DeleteRequest(Constants.NORMALIZED_VECTOR2_INDEX, Constants.NORMALIZED_VECTOR_2_TYPE, normEsId);
                    try {
                        HighClient.getInstance().getClient().delete(delRequest);
                        HighClient.getInstance().getClient().delete(normDelRequest);
                    } catch (IOException e) {
                        LOG.error("Failed to delete track with all-zero tag vectors.");
                    }
                }

            } catch(NullPointerException e) {
                LOG.error("Unable to fetch tag similarity vector for track mid='" + trackMid + "'");
            }
            if (bulkRequest.estimatedSizeInBytes() > 0) {
                try {
                    HighClient.getInstance().getClient().bulk(bulkRequest);
                } catch (IOException e) {
                    LOG.error("Failed to bulk insert tag similarity vectors : " + e.getMessage());
                }
            }
        }

    }

    /**
     * Checks if vector is all zeroes
     */
    private static boolean isAllZeroes(ArrayList<Integer>  vector) {

        for (Integer v : vector) {
            if (v > 0) {
                return false;
            }
        }
        return true;

    }

}