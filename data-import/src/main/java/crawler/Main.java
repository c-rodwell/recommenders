package crawler;

import de.umass.lastfm.Caller;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.logging.Level;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String[] args) throws IOException {

        LOG.info("Begin data import program.");
        final long startTime = System.currentTimeMillis();

        // This disables the last-fm loggings
        Caller.getInstance().getLogger().setLevel(Level.OFF);

        populateUsersIndex();
        LowClient.getInstance().refreshIndex(Constants.USERS_INDEX);

        populateTrackVectorsIndex();
        LowClient.getInstance().refreshIndex(Constants.TRACK_VECTORS_INDEX);

        populateTagSimIndex();
        LowClient.getInstance().refreshIndex(Constants.TAG_SIM_INDEX);

        populateUserHistoryIndex();
        LowClient.getInstance().refreshIndex(Constants.HISTORY_INDEX);

        LOG.info("Closing ES clients...");

        HighClient.getInstance().close();
        LowClient.getInstance().close();

        LOG.info("Data import done.");

        final long endTime = System.currentTimeMillis();
        LOG.info("Total data-import program execution time: " + ((endTime - startTime)/1000) + " seconds." );

        System.exit(0);

    }

    private static void populateTagSimIndex() throws IOException {

        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.TAG_SIM_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.TAG_SIM_INDEX);
        }

        // Create new index
        HighClient.getInstance().createIndex(Constants.TAG_SIM_INDEX);

        // create tag similarity vectors and post to Elasticsearch
        TagSimVectors.createTagSimVectors();

    }

    private static void populateTrackVectorsIndex() throws IOException {

        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.TRACK_VECTORS_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.TRACK_VECTORS_INDEX);
        }

        // Create new index
        HighClient.getInstance().createIndex(Constants.TRACK_VECTORS_INDEX);

        TrackVectors.createTrackSimVectors();

    }

    private static void populateUsersIndex() {

        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.USERS_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.USERS_INDEX);
        }

        // Create new index
        HighClient.getInstance().createIndex(Constants.USERS_INDEX);

        // Put mapping
        LowClient.getInstance().putMapping(Crawler.getMapping(), Constants.USERS_INDEX, Constants.USERS_TYPE);

        Crawler.crawlForUsers();

    }

    private static void populateUserHistoryIndex() throws IOException {

        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.HISTORY_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.HISTORY_INDEX);
        }

        // Create new index
        HighClient.getInstance().createIndex(Constants.HISTORY_INDEX);

        //does this need a mapping like users index?
        UserHistory.makeUserHistories();

    }

}
