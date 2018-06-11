package recommender;

public class Constants {

    public static final String SCHEME = "http";
    public static final String ES_HOST = "localhost";
    public static final int ES_PORT = 9200;
    public static final int ES_MAX = 10000;

    public static final String USERS_INDEX = "users";
    public static final String TRACK_VECTORS_INDEX = "trackvectors";
    public static final String TAG_SIM_INDEX = "tag-sim";
    public static final String HISTORY_INDEX = "userhistory4x25";
    public static final String NORMALIZED_VECTOR2_INDEX = "normalized-vector-2";

    // recommender params
    public static final double TRACK_SIM_WEIGHT = 0.6;

}
