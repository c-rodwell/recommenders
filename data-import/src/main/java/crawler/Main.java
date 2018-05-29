package crawler;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class);

    public HashMap<String, Integer> usersToInts;

    public static void main(String[] args) throws IOException {

        // populateUsersIndex();
        populateTrackVectorsIndex(populateUsersIndex());

        HighClient.getInstance().close();
        LowClient.getInstance().close();

    }

    private static void populateTrackVectorsIndex(HashMap<String, Integer> usersToInts) throws IOException {

        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.TRACK_VECTORS_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.TRACK_VECTORS_INDEX);
        }

        // Create new index
        HighClient.getInstance().createIndex(Constants.TRACK_VECTORS_INDEX);

        TrackVectors.createTrackVectors(usersToInts);

    }

    private static HashMap<String, Integer> populateUsersIndex() {

        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.USERS_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.USERS_INDEX);
        }

        // Create new index
        HighClient.getInstance().createIndex(Constants.USERS_INDEX);

        // Put mapping
        LowClient.getInstance().putMapping(CollectData.getMapping(), Constants.USERS_INDEX, Constants.USERS_TYPE);

        return CollectData.loadUserTopTracks();

    }

}
