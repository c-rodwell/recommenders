package recommender;

import crawler.TrackVectors;
import crawler.UserHistory;
import org.apache.log4j.Logger;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class Recommender {

    private static final Logger LOG = Logger.getLogger(Recommender.class);

    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;
    private static final String SCHEME = "http";
    private static final String ES_INDEX = "users";
    private static final String ES_TYPE = "user";

    public static void main(String[] args){
        //for now this is just testing
        int[] v1 = {0,0,1,2,3};
        int[] v2 = {3,4,0,0,0};
        int[] v3 = {1,1,1,3,2};

        System.out.println("|v1|^2 = "+dotProduct(v1, v1));
        System.out.println("|v2|^2 = "+dotProduct(v2, v2));
        System.out.println("|v3|^2 = "+dotProduct(v3, v3));
        //simple similarity - commutative
        double sim12 = trackSimilarity(v1, v2);
        double sim13 = trackSimilarity(v1, v3);
        double sim23 = trackSimilarity(v2, v3);
        System.out.println("similarity of v1 and v2 is: "+sim12);
        System.out.println("similarity of v1 and v3 is: "+sim13);
        System.out.println("similarity of v2 and v3 is: "+sim23);

        //adjusted similarities relative to v3:
        double asim31 = adjustedTrackSimilarity(v3, v1);
        double asim32 = adjustedTrackSimilarity(v3, v2);
        System.out.println("adjusted similarity of v1 relative to v3 is: "+asim31);
        System.out.println("adjusted similarity of v2 relative to v3 is: "+asim32);

        //test getting user history
        SearchHit userHistoryHit;
        SearchHit trackVectorHit;
        try {
            userHistoryHit = UserHistory.getHistoriesForUser("CaiusD");
            trackVectorHit = TrackVectors.getTrackVector("acc74a10-ff4c-4bfb-b9aa-08d10e92c9f5");
            DocumentField field = userHistoryHit.field("1");
            String s = field.toString();
        } catch (IOException e){
            System.out.println("error getting data from elasticsearch: "+e.getMessage());
        }


    }

    //based on "Evaluating Hybrid Music Recommender Systems" by Hornung et al
    //weighted sum of different cosine distance similarity functions on user's listening history
    public String[] recommendTracks(String user, String[] history, int numToRecommend){
        String[] tracks = new String[numToRecommend];
        for (String trackID: history){

        }

        //for each track in dataset:
        //for each track in user's history:
        //compute similarity, add it to relevance score for the track
        //maybe compute similarities first, put in ES)
        //return tracks with most relevance
        return tracks;
    }

    public double relevanceOfTrack(String user, String trackId, String[] history){
        double total = 0.0;
        return total;
    }

    //similarity of two tracks = cosine distance of the user listening vectors
    //subtract from each vector : "the average number of times the user j listened to a track"
    //should we subtract here? or subtract when creating the vectors?
    //adjust by track popularity - defined by listening total for all users for the track- this is sum of the vector
    public static double trackSimilarity(int[] track1, int[] track2){
        return cosineDistance(track1, track2);
    }

    public static double adjustedTrackSimilarity(int[] track1, int[] track2){
        return cosineDistance(track1, track2)/sum(track2);
    }

    public static double cosineDistance(int[]v1, int[]v2){
        return dotProduct(v1,v2)/((Math.sqrt(dotProduct(v1,v1)))*(Math.sqrt(dotProduct(v2,v2))));
    }

    public static int sum(int[] v){
        int s = 0;
        for(int i=0; i<v.length; i++){
            s +=v[i];
        }
        return s;
    }

    public static double dotProduct(int[]v1, int[]v2){
        if (v1.length != v2.length){
            throw new IllegalArgumentException("vector sizes don't match");
        }
        double sum=0.0;
        for (int i=0; i< v1.length; i++){
            sum += v1[i]*v2[i];
        }
        return sum;
    }
}
