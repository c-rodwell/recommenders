package crawler;

import de.umass.lastfm.Caller;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.logging.Level;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String[] args) throws IOException {

        // Check for program arguments
        if (args.length > 1) {
            LOG.info("Invalid args length.");
            LOG.info("usage: crawler <number_of_users>");
            LOG.info("number_of_users: MAX = 100000, MIN = 10, DEFAULT = 10");
            System.exit(-1);
        }

        if (args.length == 1) {
            try {
                Constants.NUM_OF_USERS = Integer.parseInt(args[0]);
                if (Constants.NUM_OF_USERS > 500000 || Constants.NUM_OF_USERS < 100) {
                    LOG.info("Invalid arg.");
                    LOG.info("usage : crawler <number_of_users>");
                    LOG.info("number_of_users (optional) : MAX = 100000, MIN = 10, DEFAULT = 10");
                    System.exit(-1);
                }
            } catch(Exception e) {
                LOG.info("Invalid arg.");
                LOG.info("usage : crawler <number_of_users>");
                LOG.info("number_of_users (optional) : MAX = 100000, MIN = 10, DEFAULT = 10");
                System.exit(-1);
            }
        }

        // Begin collecting data and inserting them to Elasticsearch
        LOG.info("Begin data import program.");
        final long startTime = System.currentTimeMillis();

        // This disables the last-fm loggings
        Caller.getInstance().getLogger().setLevel(Level.OFF);

        populateUsersIndex();
        LowClient.getInstance().refreshIndex(Constants.USERS_INDEX);

        createNormalizedVectorIndex();
        LowClient.getInstance().refreshIndex(Constants.NORMALIZED_VECTOR2_INDEX);

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
        LOG.info("Total data import program execution time: " + ((endTime - startTime) / 1000) + " seconds.");

        System.exit(0);

    }

    private static void populateTagSimIndex() {
        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.TAG_SIM_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.TAG_SIM_INDEX);
        }
        // Create new index
        HighClient.getInstance().createIndex(Constants.TAG_SIM_INDEX);

        TagSimVectors.createTagSimVectors();
    }

    private static void populateTrackVectorsIndex() {
        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.TRACK_VECTORS_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.TRACK_VECTORS_INDEX);
        }
        // Create new index
        HighClient.getInstance().createIndex(Constants.TRACK_VECTORS_INDEX);
        // Put mapping
        LowClient.getInstance().putMapping(TrackVectors.getMapping(), Constants.TRACK_VECTORS_INDEX, Constants.TRACK_VECTORS_TYPE);

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
        // begin crawling here
        Crawler.crawlForUsers();
    }

    private static void createNormalizedVectorIndex() {
        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.NORMALIZED_VECTOR2_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.NORMALIZED_VECTOR2_INDEX);
        }
        // Create new index
        HighClient.getInstance().createIndex(Constants.NORMALIZED_VECTOR2_INDEX);
    }

    private static void populateUserHistoryIndex() {
        // Delete existing index
        boolean isIndexExists = LowClient.getInstance().isIndexExists(Constants.HISTORY_INDEX);
        if (isIndexExists) {
            HighClient.getInstance().deleteIndex(Constants.HISTORY_INDEX);
        }
        // Create new index
        HighClient.getInstance().createIndex(Constants.HISTORY_INDEX);

        UserHistory.makeUserHistories();
    }

}
