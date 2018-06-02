package recommender;

import crawler.TrackVectors;
import crawler.UserHistory;
import org.apache.log4j.Logger;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

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
//        Integer[] v1 = {0,0,1,2,3};
//        Integer[] v2 = {3,4,0,0,0};
//        Integer[] v3 = {1,1,1,3,2};
//
//        System.out.println("|v1|^2 = "+dotProduct(v1, v1));
//        System.out.println("|v2|^2 = "+dotProduct(v2, v2));
//        System.out.println("|v3|^2 = "+dotProduct(v3, v3));
//        //simple similarity - commutative
//        double sim12 = trackSimilarity(v1, v2);
//        double sim13 = trackSimilarity(v1, v3);
//        double sim23 = trackSimilarity(v2, v3);
//        System.out.println("similarity of v1 and v2 is: "+sim12);
//        System.out.println("similarity of v1 and v3 is: "+sim13);
//        System.out.println("similarity of v2 and v3 is: "+sim23);
//
//        //adjusted similarities relative to v3:
//        double asim31 = adjustedTrackSimilarity(v3, v1);
//        double asim32 = adjustedTrackSimilarity(v3, v2);
//        System.out.println("adjusted similarity of v1 relative to v3 is: "+asim31);
//        System.out.println("adjusted similarity of v2 relative to v3 is: "+asim32);

        //test getting user history
        HashMap<String, String> userHistory;
        ArrayList<Integer> currentTrackVector;
        ArrayList<Integer> trackVectorFromHistory;
        String username = "CaiusD";
        String currentTrackId = "90a0c88f-f157-4594-afb2-d8a6213b61fd";
        try {
            userHistory = UserHistory.getHistoryForUser("username", 3);
            currentTrackVector = TrackVectors.getTrackVector(currentTrackId);
            double similarity = 0.0;
            for (int i=1; i<=currentTrackVector.size(); i++){
                String historyTrackName = userHistory.get(Integer.toString(i));
                trackVectorFromHistory = TrackVectors.getTrackVector(historyTrackName);
                similarity += trackSimilarity((Integer[]) currentTrackVector.toArray(), (Integer []) trackVectorFromHistory.toArray());
            }
            System.out.println("score for track "+currentTrackId+ " is "+similarity);
        } catch (IOException e){
            System.out.println("error getting data from elasticsearch: "+e.getMessage());
        }


        System.exit(0);
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
    public static double trackSimilarity(Integer[] track1, Integer[] track2){
        return cosineDistance(track1, track2);
    }

    public static double adjustedTrackSimilarity(Integer[] track1, Integer[] track2){
        return cosineDistance(track1, track2)/sum(track2);
    }

    public static double cosineDistance(Integer[]v1, Integer[]v2){
        return dotProduct(v1,v2)/((Math.sqrt(dotProduct(v1,v1)))*(Math.sqrt(dotProduct(v2,v2))));
    }

    public static int sum(Integer[] v){
        int s = 0;
        for(int i=0; i<v.length; i++){
            s +=v[i];
        }
        return s;
    }

    public static double dotProduct(Integer[]v1, Integer[]v2){
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
