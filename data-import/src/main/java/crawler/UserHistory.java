package crawler;

import com.google.gson.JsonObject;
import de.umass.lastfm.PaginatedResult;
import de.umass.lastfm.Track;
import de.umass.lastfm.User;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 * Create set of user histories for each user - each history is a sequence of tracks the user listened to
 * Put the results in Elasticsearch: user1 -> (history1 , history2, ...) where each is a sequence of track_mid
 *
 */
public class UserHistory {

    private static final Logger LOG = Logger.getLogger(UserHistory.class);


    public static void makeUserHistories() {

        LOG.info("Begin bulk insert of user histories to ES...");
        // prepare bulk request to ES
        BulkRequest bulkRequest = new BulkRequest();

        Terms uniqueUsersTerms = UsersHelper.getUniqueUsers();
        for (Terms.Bucket b : uniqueUsersTerms.getBuckets()) {
            JsonObject userHistoriesObj = new JsonObject();

            String username = b.getKeyAsString();
            PaginatedResult<Track> recentTracksPage = User.getRecentTracks(username,1, Constants.RECENT_TRACKS_SIZE, Constants.LASTFM_APIKey);
            Iterator<Track> trackIter = recentTracksPage.iterator();
            for (int i=1; i<=Constants.HISTORIES_PER_USER; i++) {
                JsonObject currentHistoryObj = new JsonObject();
                for (int j=1; j<=Constants.HISTORY_SIZE; j++) {
                    if (!trackIter.hasNext()){
                        break;
                    }
                    Track t = trackIter.next();

                    String mbid = t.getMbid();
                    // only count non-blank mbid which exist in trackVectors. if all are blank, we should eventually hit the "trackIter is out" condition
                    if ((mbid.equals("")) || (!TrackVectorsHelper.isInTrackVectors(mbid))) {
                        j--; // repeat this index so we get the same total number
                    }
                    else {
                        currentHistoryObj.addProperty(Integer.toString(j), mbid);
                    }
                }

                userHistoriesObj.add(Integer.toString(i), currentHistoryObj);
            }

            JsonObject esObj = new JsonObject();
            esObj.addProperty("username", username);
            esObj.add("histories", userHistoriesObj);

            // add to bulk request
            bulkRequest.add(new IndexRequest(Constants.HISTORY_INDEX, Constants.HISTORY_TYPE)
                    .source(esObj.toString(), XContentType.JSON));
        }

        try {
            HighClient.getInstance().getClient().bulk(bulkRequest);
        } catch (IOException e) {
            LOG.error("Failed to bulk insert tag similarity vectors : " + e.getMessage());
        }

    }

}