package crawler;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import de.umass.lastfm.Tag;
import de.umass.lastfm.Track;
import de.umass.lastfm.User;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CollectData {

    private static final Logger LOG = Logger.getLogger(CollectData.class);

    public static void loadUserTopTracks() {

        LOG.info("Loading usernames and tracks' data for each user...");
        try {

            // Read the usernames
            InputStream is = CollectData.class.getClassLoader().getResourceAsStream(Constants.USERS_FILE);
            JsonReader jsonReader = new JsonReader(new InputStreamReader(is));

            JsonParser parser = new JsonParser();
            JsonArray data = parser.parse(jsonReader).getAsJsonArray();

            int docId = 0;
            for (int i = 0; (i < data.size()) && (i < Constants.num_users); i++) { //allow running on small set of users
                JsonObject obj = data.get(i).getAsJsonObject();
                String username = obj.get("username").getAsString();
                // Get top tracks for user
                Collection<Track> topTracks = User.getTopTracks(username, Constants.APIKey);
                for (Track t : topTracks) {
                    String mbid = t.getMbid();
                    if ((mbid != null)&&(mbid.length()>0)) {
                        JsonObject esObj = new JsonObject();
                        esObj.addProperty("username", username);
                        esObj.addProperty("track_name", t.getName());
                        esObj.addProperty("track_playcount", t.getPlaycount());
                        esObj.addProperty("track_mid", t.getMbid());
                        esObj.addProperty("track_artist", t.getArtist());
                        HighClient.getInstance().postJsonToES(Constants.USERS_INDEX, Constants.USERS_TYPE, docId, esObj);
                        docId++;
                    }
                }
            }

        } catch (Exception e) {
            LOG.error("Exception while loading usernames and tracks' data: " + e.getMessage());
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
