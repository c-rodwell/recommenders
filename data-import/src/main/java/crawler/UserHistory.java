package crawler;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import de.umass.lastfm.PaginatedResult;
import de.umass.lastfm.Track;
import de.umass.lastfm.User;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

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
        //I made TrackVectors.UniqueUsers() public so I can use it here
        //should it go in a utility methods class instead? or save the info in an accessible place?
        Terms uniqueUsersTerms = TrackVectors.getUniqueUsers();
        int docId=0;
        for (Terms.Bucket b : uniqueUsersTerms.getBuckets()) {

            //what form to put a vector in ES? try a json with explicit integers as keys, maybe that will be easier to retreive
            JsonObject userHistoriesObj = new JsonObject();

            String username = b.getKeyAsString();
            PaginatedResult<Track> recentTracksPage = User.getRecentTracks(username,1, recentTracksSize, Constants.APIKey);
            Iterator<Track> trackIter = recentTracksPage.iterator();;
            for (int i=1; i<=historiesPerUser; i++){
                //ArrayList<String> currentHistory = new ArrayList<String>();
                JsonObject currentHistoryObj = new JsonObject();
                for (int j=1; j<=historySize; j++) {
                    if (!trackIter.hasNext()){//out of tracks - how to handle it when user history is smaller than others?
                        System.out.println("user \""+username+"\" ran out of track history at history ="+i+", track = "+j);
                        break;
                    }
                    Track t = trackIter.next();

                    //currentHistory.add(t.getMbid());
                    String mbid = t.getMbid();
                    //only count non-blank mbid. if all are blank, we should eventually hit the "trackIter is out" condition
                    if (mbid.equals("")){
                        j--; //repeat this index
                    }
                    else {
                        currentHistoryObj.addProperty(Integer.toString(j), mbid); //use int -> string as key, will that be easy to retrieve?
                    }
                }
                //userHistories.add(currentHistory);
                userHistoriesObj.add(Integer.toString(i), currentHistoryObj);
            }
            //save history for that user in ES (hashmap for now) - what structure should it go in?
            //allUsersHistories.put(username,userHistories);
            JsonObject esObj = new JsonObject();
            esObj.addProperty("username", username);
            esObj.add("histories", userHistoriesObj);
            HighClient.getInstance().postJsonToES(Constants.HISTORY_INDEX, Constants.HISTORY_TYPE, docId, esObj);
            docId++;
        }
    }

    public static SearchHit getHistoriesForUser(String username) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("username", username);
        SearchRequest request = new SearchRequest(Constants.HISTORY_INDEX);
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response = HighClient.getInstance().getClient().search(request);
        SearchHit result = response.getHits().getHits()[0]; //should be just one hit for the username
        return result; //could get the separate histories here too.
    }
}
