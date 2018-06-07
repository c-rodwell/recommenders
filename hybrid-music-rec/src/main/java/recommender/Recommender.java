package recommender;

import org.apache.log4j.Logger;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

public class Recommender {

    private static final Logger LOG = Logger.getLogger(Recommender.class);

    public static void main(String[] args) throws IOException {

        LOG.info("Start recommender");

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

        String username = "rockstr";
        String trackid = "0f9201b6-6989-476e-b11e-fe3c0f3d4dc1"; //track listened to by h0bbel


        HashMap<String, String> history = ESHelpers.getHistoryForUser(username, 1);
        HashMap<String, Double> noAdjustScores = recommendTracksForUser(history, 10, false, false);
        HashMap<String, Double> AdjustBeforeScores = recommendTracksForUser(history, 10, true, false);
        HashMap<String, Double> AdjustAfterScores = recommendTracksForUser(history, 10, false, true);
        HashMap<String, Double> AdjustBeforeAndAfterScores = recommendTracksForUser(history, 10, true, true);

        System.out.println("for user = "+username+" , trackId = "+trackid+" :");
        System.out.println("no adjust:                  score = "+noAdjustScores.get(trackid));
        System.out.println("adjust before:              score = "+AdjustBeforeScores.get(trackid));
        System.out.println("adjust after:               score = "+AdjustAfterScores.get(trackid));
        System.out.println("adjust before and after:    score = "+AdjustBeforeAndAfterScores.get(trackid));
        ESHelpers.close();
        System.exit(0);
    }

    public static double weightedAvg(double trackSim, double tagSim) {

        double trackSimWeight = Constants.TRACK_SIM_WEIGHT;
        double tagSimWeight = 1.0 - trackSimWeight;

        double weightedAvg = (trackSimWeight * trackSim) + (tagSimWeight * tagSim);
        //System.out.println(weightedAvg);

        return weightedAvg;
    }

    //based on "Evaluating Hybrid Music Recommender Systems" by Hornung et al
    //weighted sum of different cosine distance similarity functions on user's listening history
    public static HashMap<String, Double> recommendTracksForUser(HashMap<String, String> userHistory, int numToRecommend, boolean adjust_before, boolean adjust_after){
        //String[] tracks = new String[numToRecommend];
        String trackIndexToUse;
        if (adjust_before){
            trackIndexToUse = Constants.NORMALIZED_VECTOR2_INDEX;
        } else{
            trackIndexToUse = Constants.TRACK_VECTORS_INDEX;
        }

        HashMap<String, Double> trackScores = new HashMap();

        int queueSize = 10;
        TrackScoreComparator tsc = new TrackScoreComparator();
        PriorityQueue<TrackScore> queue = new PriorityQueue<TrackScore>(queueSize, tsc);

        //get history
        //HashMap<String, String> userHistory = ESHelpers.getHistoryForUser(username, historyNum);
        ArrayList<Integer> trackVectorFromHistory;
        ArrayList<Integer> tagVectorFromHistory;
        Integer[] historyTrackArr = new Integer[Constants.num_users];
        Integer[] historyTagArr = new Integer[Constants.num_users];

        //loop for each track we have a vector for
        Terms uniqueTracksTerms = ESHelpers.getTracksWhichHaveVectors();
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String currentTrackId = b.getKeyAsString();
            ArrayList<Integer> currentTrackVector;
            ArrayList<Integer> currentTagVector;
            Integer[] currentTrackArr = new Integer[Constants.num_users];
            Integer[] currentTagArr = new Integer[Constants.num_users];

            currentTrackVector = ESHelpers.getVector(trackIndexToUse, currentTrackId);
            currentTrackArr = currentTrackVector.toArray(currentTrackArr);

            currentTagVector = ESHelpers.getVector(Constants.TAG_SIM_INDEX, currentTrackId);
            currentTagArr = currentTagVector.toArray(currentTagArr);

            double similarity = 0.0;
            double tagSimilarity = 0.0;

            //loop over tracks in history
            for (int i=1; i<=userHistory.size(); i++) {
                String historyTrackName = userHistory.get(Integer.toString(i));

                trackVectorFromHistory = ESHelpers.getVector(trackIndexToUse ,historyTrackName);
                historyTrackArr = trackVectorFromHistory.toArray(historyTrackArr);

                tagVectorFromHistory = ESHelpers.getVector(Constants.TAG_SIM_INDEX ,historyTrackName);
                historyTagArr = tagVectorFromHistory.toArray(historyTagArr);

                //similarity += trackSimilarity((Integer[]) currentTrackVector.toArray(), (Integer []) trackVectorFromHistory.toArray());
                if (adjust_after) {
                    similarity += adjustedTrackSimilarity(currentTrackArr, historyTrackArr);
                } else{
                    similarity += trackSimilarity(currentTrackArr, historyTrackArr);
                }
                tagSimilarity += tagSimilarity(currentTagArr, historyTagArr);
            }
            //System.out.println("score for track "+currentTrackId+ " is "+similarity);
            //System.out.println("TAG score for track "+currentTrackId+ " is "+tagSimilarity);
            trackScores.put(currentTrackId, weightedAvg(similarity, tagSimilarity));
            TrackScore ts = new TrackScore(currentTrackId, weightedAvg(similarity, tagSimilarity));

            insertToSortedQueue(queue, queueSize, ts);
        }

        // Testing, print put sorted, bounded queue
        for (int i = 0; i < queueSize; i++) {
            System.out.println(queue.remove().toString());
        }

        try {
            ESHelpers.close();
        } catch (IOException e){
            System.out.println(e.getMessage());

    }

        //for each track in dataset:
        //for each track in user's history:
        //compute similarity, add it to relevance score for the track
        //maybe compute similarities first, put in ES)
        //return tracks with most relevance
        return trackScores;
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

    public static double tagSimilarity(Integer[] track1, Integer[] track2){
        return cosineDistance(track1, track2);
    }

    //adjust for popularity by dividing by track2's sum - then make sure track is from history
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

    public static void insertToSortedQueue(PriorityQueue<TrackScore> queue, int size, TrackScore ts) {

        if (queue.size() < size) {
            queue.add(ts);
        } else {
            double scoreToAdd = ts.getScore();
            double lowestScore = queue.peek().getScore(); // peek the head of the queue
            if (scoreToAdd > lowestScore) {
                queue.remove(); // remove head, which is the lowest score
                queue.add(ts); // add new score
            }
        }

    }

}
