package crawler;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.socrata.api.HttpLowLevel;
import com.socrata.api.Soda2Consumer;
import com.socrata.builders.SoqlQueryBuilder;
import com.socrata.exceptions.LongRunningQueryException;
import com.socrata.exceptions.SodaError;
import com.socrata.model.soql.SoqlQuery;
import com.sun.jersey.api.client.ClientResponse;
import de.umass.lastfm.Track;
import de.umass.lastfm.User;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Bulk processor for inserting large dataset of users to Elasticsearch
 *
 */
public class Crawler {

    private static final Logger LOG = Logger.getLogger(Crawler.class);

    private static JsonArray data = new JsonArray();

    public static void crawlForUsers() {

        int offset = 0;
        int partitions = Constants.NUM_OF_USERS / Constants.SODA_MAX;
        int remainder = Constants.NUM_OF_USERS % Constants.SODA_MAX;

        for (int i = 0; i < partitions; i++) {
            offset = i * Constants.SODA_MAX;
            crawl(offset, Constants.SODA_MAX);
            bulkInsert();
        }

        if (remainder > 0) {
            crawl(offset, remainder);
            bulkInsert();
        }

    }

    private static void crawl(int offset, int limit) {

        Soda2Consumer consumer = Soda2Consumer.newConsumer("https://opendata.socrata.com/?$$app_token=" + Constants.SOCRATA_APIKey);
        SoqlQuery usersQuery = new SoqlQueryBuilder()
                .addSelectPhrase("username")
                .addSelectPhrase("play_count")
                .setWhereClause("play_count>=50000")
                .setLimit(limit)
                .setOffset(offset)
                .build();

        try {
            ClientResponse response = consumer.query(Constants.SOCRATA_Resource, HttpLowLevel.JSON_TYPE, usersQuery);
            String payload = response.getEntity(String.class);
            JsonParser parser = new JsonParser();
            data = parser.parse(payload).getAsJsonArray();
            LOG.info("Fetched data size=" + data.size());
        } catch (LongRunningQueryException e) {
            LOG.error("Exception while executing Soda query, program will continue : " + e.getMessage());
        } catch (SodaError sodaError) {
            LOG.error("Soda API threw an exception, program will continue : " + sodaError.getMessage());
        }

    }

    private static void bulkInsert() {

        LOG.info("Begin bulk insert of users dataset to ES...");

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < data.size(); i++) {
            JsonObject obj = data.get(i).getAsJsonObject();
            String username = obj.get("username").getAsString();
            try {
                Collection<Track> topTracks = User.getTopTracks(username, Constants.LASTFM_APIKey);
                for (Track t : topTracks) {
                    if (t.getMbid().length() > 0) {
                        JsonObject esObj = new JsonObject();
                        esObj.addProperty("username", username);
                        esObj.addProperty("track_name", t.getName());
                        esObj.addProperty("track_playcount", t.getPlaycount());
                        esObj.addProperty("track_mid", t.getMbid());
                        esObj.addProperty("track_artist", t.getArtist());
                        bulkRequest.add(new IndexRequest(Constants.USERS_INDEX, Constants.USERS_TYPE)
                                .source(esObj.toString(), XContentType.JSON));
                    }
                }
            } catch(Exception e) {
                LOG.error("last.fm API threw an exception, program will continue : " + e.getMessage());
            }
        }

        try {
            HighClient.getInstance().getClient().bulk(bulkRequest);
        } catch (IOException e) {
            LOG.error("Failed to bulk insert user dataset : " + e.getMessage());
        }

    }

    public static Map<String, Object> getMapping() {

        Map<String, Object> usernameMap = new HashMap<>();
        usernameMap.put("type", "text");
        usernameMap.put("fielddata", true);

        Map<String, Object> tracknameMap = new HashMap<>();
        tracknameMap.put("type", "text");
        tracknameMap.put("fielddata", true);

        Map<String, Object> trackmidMap = new HashMap<>();
        trackmidMap.put("type", "keyword");

        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put("username", usernameMap);
        propertiesMap.put("track_name", tracknameMap);
        propertiesMap.put("track_mid", trackmidMap);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", propertiesMap);

        return mapping;

    }

}
