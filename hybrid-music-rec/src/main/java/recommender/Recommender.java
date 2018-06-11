package recommender;

import org.apache.log4j.Logger;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.PriorityQueue;

import static java.util.Arrays.asList;

public class Recommender {

    private static final Logger LOG = Logger.getLogger(Recommender.class);


    public static void main(String[] args) throws IOException {

        LOG.info("Start recommender");


        //test cosines and similarity
        //Integer[] v1= {0,0,1,2,3}; //new ArrayList(asList(0,0,1,2,3));
        ArrayList<Integer> v1 = new ArrayList(asList(0,0,1,2,3));
        //Integer[] v2 = {3,4,0,0,0};
        ArrayList<Integer> v2 =new ArrayList(asList(3,4,0,0,0));
        //Integer[] v3 = {1,1,1,3,2};
        ArrayList<Integer> v3 =new ArrayList(asList(1,1,1,3,2));

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

        System.out.println("cosine similiarites:");
        System.out.println("sim:");
        System.out.println("cosine similiarites:");

        // are we passing one user for every run?
//        String username = "rockstr";
//
//        ArrayList<String> hiddenTracks = new ArrayList<>();
//        int userHistorySize = ESHelpers.getUserHistorySize(username);
//        if (userHistorySize == 0) {
//            System.out.println("User " + username + " has no listening history.");
//        } else {
//            System.out.println("User " + username + " has " + userHistorySize + " listening history.");
//            int numOfHistToEval = 3;
//            for (int i = 1; i <= userHistorySize && i <= numOfHistToEval; i++) {
//                HashMap<String, String> history = ESHelpers.getHistoryForUser(username, i);
//                String lastTrackKey = Integer.toString(history.size()); // get last track - this is the hidden track
//                hiddenTracks.add(history.get(lastTrackKey));
//                System.out.println("Hide " + history.get(lastTrackKey) + " from listening history #" + i);
//
//                // commented code below goes here?
//                //HashMap<String, Double> noAdjustScores = recommendTracksForUser(history, 10, false, false);
//                //System.out.println(noAdjustScores.values().toString());
//
//                // what to do next?
//
//
//            }
//        }

        //test getting user history
//        String trackid = "0f9201b6-6989-476e-b11e-fe3c0f3d4dc1"; //track listened to by h0bbel
//
//        HashMap<String, String> history = ESHelpers.getHistoryForUser(username, 1);
//        HashMap<String, Double> noAdjustScores = recommendTracksForUser(history, 10, false, false);
//        HashMap<String, Double> AdjustBeforeScores = recommendTracksForUser(history, 10, true, false);
//        HashMap<String, Double> AdjustAfterScores = recommendTracksForUser(history, 10, false, true);
//        HashMap<String, Double> AdjustBeforeAndAfterScores = recommendTracksForUser(history, 10, true, true);
//
//        System.out.println("for user = "+username+" , trackId = "+trackid+" :");
//        System.out.println("no adjust:                  score = "+noAdjustScores.get(trackid));
//        System.out.println("adjust before:              score = "+AdjustBeforeScores.get(trackid));
//        System.out.println("adjust after:               score = "+AdjustAfterScores.get(trackid));
//        System.out.println("adjust before and after:    score = "+AdjustBeforeAndAfterScores.get(trackid));
        String username = "rockstr";
        HashMap<Integer, String> hiddenTracks = new HashMap<>();
        int numOfHistToEval = 1;

        int userHistorySize = ESHelpers.getUserHistorySize(username);
        System.out.println(userHistorySize);
        if (userHistorySize == 0) {
            System.out.println("User " + username + " has no listening history.");
        } else {
            System.out.println("User " + username + " has " + userHistorySize + " listening history.");

            for (int i = 1; i <= userHistorySize && i <= numOfHistToEval; i++) {

                HashMap<String, String> history = ESHelpers.getHistoryForUser(username, i);

                String lastTrackKey = Integer.toString(history.size()); // get last track - this is the hidden track
                hiddenTracks.put(i, history.get(lastTrackKey));

                System.out.println("Hide " + history.get(lastTrackKey) + " from listening history #" + i);
                history.remove(lastTrackKey); // remove history from history list

                PriorityQueue<TrackScore> noAdjustScores = recommendTracksForUser(history, 10, false, false);
                System.out.println("---------------------------------------");
                PriorityQueue<TrackScore> AdjustBeforeScores = recommendTracksForUser( history, 10, true, false);
                System.out.println("---------------------------------------");
                PriorityQueue<TrackScore> AdjustAfterScores = recommendTracksForUser( history, 10, false, true);
                System.out.println("---------------------------------------");
                PriorityQueue<TrackScore> AdjustBeforeAndAfterScores = recommendTracksForUser( history, 10, true, true);

            }
        }

        ESHelpers.close();
        System.exit(0);
    }

    private static double weightedAvg(double trackSim, double tagSim) {

        double trackSimWeight = Constants.TRACK_SIM_WEIGHT;
        double tagSimWeight = 1.0 - trackSimWeight;

        double weightedAvg = (trackSimWeight * trackSim) + (tagSimWeight * tagSim);

        return weightedAvg;
    }

    public static PriorityQueue<TrackScore> recommendTracksForUser(HashMap<String, String> userHistory, int numToRecommend,
                                                                   boolean adjust_before, boolean adjust_after) {

        String trackIndexToUse;

        if (adjust_before) {
            trackIndexToUse = Constants.NORMALIZED_VECTOR2_INDEX;
        } else {
            trackIndexToUse = Constants.TRACK_VECTORS_INDEX;
        }

        int queueSize = numToRecommend;
        TrackScoreComparator tsc = new TrackScoreComparator();
        PriorityQueue<TrackScore> queue = new PriorityQueue<>(queueSize, tsc);

        // loop for each track we have a vector for
        Terms uniqueTracksTerms = ESHelpers.getTracksWhichHaveVectors();
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String currentTrackId = b.getKeyAsString();

            ArrayList<Integer> currentTrackVector = ESHelpers.getVector(trackIndexToUse, currentTrackId);
            ArrayList<Integer> currentTagVector = ESHelpers.getVector(Constants.TAG_SIM_INDEX, currentTrackId);

            double similarity = 0.0;
            double tagSimilarity = 0.0;

            // loop over tracks in history
            for (int i = 1; i <= userHistory.size(); i++) {
                String historyTrackName = userHistory.get(Integer.toString(i));

                ArrayList<Integer> trackVectorFromHistory = ESHelpers.getVector(trackIndexToUse, historyTrackName);
                ArrayList<Integer> tagVectorFromHistory = ESHelpers.getVector(Constants.TAG_SIM_INDEX, historyTrackName);

                double sim = 0.0;
                if (adjust_after) {
                    similarity += adjustedTrackSimilarity(currentTrackVector, trackVectorFromHistory);
                } else {
                    sim = trackSimilarity(currentTrackVector, trackVectorFromHistory);
                    similarity += sim;
                }
                tagSimilarity += tagSimilarity(currentTagVector, tagVectorFromHistory);

            }

            TrackScore ts = new TrackScore(currentTrackId, weightedAvg(similarity, tagSimilarity));
            insertToSortedQueue(queue, queueSize, ts);
        }


        // Testing, print out sorted, bounded queue
        /*
        for (int i = 0; i < queueSize; i++) {
            System.out.println(queue.remove().toString());
        }
        */


        return queue;
    }


    // TODO: subtract from each vector: "the average number of times the user j listened to a track"
    // similarity of two tracks = cosine distance of the user listening vectors
    // adjust by track popularity - defined by listening total for all users for the track- this is sum of the vector
    public static double trackSimilarity(ArrayList<Integer> track1, ArrayList<Integer> track2) {
        return cosineDistance(track1, track2);
    }

    public static double tagSimilarity(ArrayList<Integer> track1, ArrayList<Integer> track2) {
        return cosineDistance(track1, track2);
    }

    private static double adjustedTrackSimilarity(ArrayList<Integer> track1, ArrayList<Integer> track2) {
        return cosineDistance(track1, track2) * (220.0 /(double) sum(track1));
    }

    private static double cosineDistance(ArrayList<Integer> v1, ArrayList<Integer> v2) {
        double denominator = Math.sqrt(dotProduct(v1, v1)) * Math.sqrt(dotProduct(v2, v2));
        if (denominator == 0.0) {
            throw new IllegalArgumentException("There is a vector that is all zeroes!");
        }
        return dotProduct(v1, v2) / denominator;
    }

    public static int sum(ArrayList<Integer> v) {
        int s = 0;
        for (int i = 0; i < v.size(); i++) {
            s += v.get(i);
        }
        return s;
    }

    private static double dotProduct(ArrayList<Integer> v1, ArrayList<Integer> v2) {
        if (v1.size() != v2.size()) {
            throw new IllegalArgumentException("Vector sizes don't match!");
        }
        double sum = 0.0;
        for (int i = 0; i < v1.size(); i++) {
            sum += v1.get(i) * v2.get(i);
        }
        return sum;
    }

    private static void insertToSortedQueue(PriorityQueue<TrackScore> queue, int size, TrackScore ts) {

        if (queue.size() < size) {
            queue.add(ts);
        } else {
            double scoreToAdd = ts.getScore();
            double lowestScore = 0; // peek the head of the queue
            if (queue.peek() != null) {
                lowestScore = queue.peek().getScore();
            }
            if (scoreToAdd > lowestScore) {
                queue.remove(); // remove head, which is the lowest score
                queue.add(ts); // add new score
            }
        }

    }

}
