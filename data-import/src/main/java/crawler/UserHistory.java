package crawler;

import com.google.gson.JsonObject;
import de.umass.lastfm.PaginatedResult;
import de.umass.lastfm.Track;
import de.umass.lastfm.User;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

public class UserHistory {

    public static int recentTracksSize = 1000;
    public static int historiesPerUser = 10;
    public static int historySize = 10;


    //create set of user histories for each user - each history is a sequence of tracks the user listened to
    //put the results in Elasticsearch: user1 -> (history1 , history2, ...) where each is a sequence of track_mid
    public static void makeUserHistories() throws IOException {

        //create an index for user histories, but for now I'll do it as a hashmap
        HashMap<String, ArrayList<ArrayList<String>>> allUsersHistories = new HashMap<String, ArrayList<ArrayList<String>>>();

        //I made TrackVectors.UniqueUsers() public so I can use it here
        //should it go in a utility methods class instead? or save the info in an accessible place?
        Terms uniqueUsersTerms = TrackVectors.getUniqueUsers();
        int docId=0;
        for (Terms.Bucket b : uniqueUsersTerms.getBuckets()) {
            ArrayList<ArrayList<String>> userHistories = new ArrayList<ArrayList<String>>(); //should there be a class for this?
            String username = b.getKeyAsString();
            PaginatedResult<Track> recentTracksPage = User.getRecentTracks(username,1, recentTracksSize, Constants.APIKey);
            Iterator<Track> trackIter = recentTracksPage.iterator();;
            for (int i=0; i<historiesPerUser; i++){
                ArrayList<String> currentHistory = new ArrayList<String>();
                for (int j=0; j<historySize; j++) {
                    Track t = trackIter.next();
                    currentHistory.add(t.getMbid());
                }
                userHistories.add(currentHistory);
            }
            //save history for that user in ES (hashmap for now) - what structure should it go in?
            allUsersHistories.put(username,userHistories);
            JsonObject esObj = new JsonObject();
            esObj.addProperty("username", username);
            JsonObject historiesObj = new JsonObject();
            historiesObj.addProperty("test", 0);  //replace 0 with some representation of the histories
            esObj.add("histories", historiesObj);
            HighClient.getInstance().postJsonToES(Constants.HISTORY_INDEX, Constants.HISTORY_TYPE, docId, esObj);
            docId++;
        }
        return;
    }
}
