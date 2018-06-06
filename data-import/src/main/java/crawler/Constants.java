package crawler;

public class Constants {

    public static final String SCHEME = "http";
    public static final String ES_HOST = "localhost";
    public static final int ES_PORT = 9200;

    public static final String USERS_INDEX = "users";
    public static final String USERS_TYPE = "usertracks";

    public static final String TRACK_VECTORS_INDEX = "trackvectors";
    public static final String TRACK_VECTORS_TYPE = "trackvecs";

    public static final String TAG_SIM_INDEX = "tag-sim";
    public static final String TAG_SIM_TYPE = "tagvecs";

    public static final String HISTORY_INDEX = "userhistory";
    public static final String HISTORY_TYPE = "history";


    public static String NORMALIZED_VECTOR2_INDEX = "normalized-vector-2";
    public static String NORMALIZED_VECTOR_2_TYPE = "norm-vector-2";

    public static final String SOCRATA_APIKey = "55KiTqMCKtty3TJqWO5VBq5aG";
    public static final String LASTFM_APIKey = "685a323d182636518e80a296f620c8a2";


    public static final int NUM_OF_USERS = 123000;
    public static final int SODA_MAX = 50000;
    public static final int ES_MAX = 10000;




    // constants for testing
    public static final String USERS_FILE = "users-small.json";
    public static final int num_users = 10;

    public static final int num_tracks = 100;

}
