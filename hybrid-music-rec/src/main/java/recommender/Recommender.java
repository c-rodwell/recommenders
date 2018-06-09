package recommender;

import org.apache.log4j.Logger;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.PriorityQueue;

public class Recommender {

    private static final Logger LOG = Logger.getLogger(Recommender.class);

    public static void main(String[] args) throws IOException {

        LOG.info("Start recommender");

        String username = "rockstr";
        HashMap<Integer, String> hiddenTracks = new HashMap<>();
        int numOfHistToEval = 1;

        int userHistorySize = ESHelpers.getUserHistorySize(username);
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
//                PriorityQueue<TrackScore> AdjustBeforeScores = recommendTracksForUser(history, 10, true, false);
//                System.out.println("---------------------------------------");
//                PriorityQueue<TrackScore> AdjustAfterScores = recommendTracksForUser(history, 10, false, true);
//                System.out.println("---------------------------------------");
//                PriorityQueue<TrackScore> AdjustBeforeAndAfterScores = recommendTracksForUser(history, 10, true, true);

            }
        }
        ESHelpers.close();
        System.exit(0);
    }

    public static double weightedAvg(double trackSim, double tagSim) {

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
        PriorityQueue<TrackScore> queue = new PriorityQueue<TrackScore>(queueSize, tsc);

        ArrayList<Integer> trackVectorFromHistory;
        ArrayList<Integer> tagVectorFromHistory;

        // loop for each track we have a vector for
        Terms uniqueTracksTerms = ESHelpers.getTracksWhichHaveVectors();
        for (Terms.Bucket b : uniqueTracksTerms.getBuckets()) {
            String currentTrackId = b.getKeyAsString();
            // System.out.println("Current track id: " + currentTrackId);

            ArrayList<Integer> currentTrackVector = ESHelpers.getVector(trackIndexToUse, currentTrackId);
            ArrayList<Integer> currentTagVector = ESHelpers.getVector(Constants.TAG_SIM_INDEX, currentTrackId);

            double similarity = 0.0;
            double tagSimilarity = 0.0;

            // loop over tracks in history
            for (int i = 1; i <= userHistory.size(); i++) {
                String historyTrackName = userHistory.get(Integer.toString(i));
                trackVectorFromHistory = ESHelpers.getVector(trackIndexToUse, historyTrackName);
                tagVectorFromHistory = ESHelpers.getVector(Constants.TAG_SIM_INDEX, historyTrackName);

                if (adjust_after) {
                    similarity += adjustedTrackSimilarity(currentTrackVector, trackVectorFromHistory);
                } else {
                    similarity += trackSimilarity(currentTrackVector, trackVectorFromHistory);
                }
                tagSimilarity += tagSimilarity(currentTagVector, tagVectorFromHistory);
            }

            TrackScore ts = new TrackScore(currentTrackId, weightedAvg(similarity, tagSimilarity));
            insertToSortedQueue(queue, queueSize, ts);
        }


        // Testing, print out sorted, bounded queue
        for (int i = 0; i < queueSize; i++) {
            System.out.println(queue.remove().toString());
        }


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
        return cosineDistance(track1, track2) / sum(track2);
    }

    private static double cosineDistance(ArrayList<Integer> v1, ArrayList<Integer> v2) {
        double denominator = Math.sqrt(dotProduct(v1, v1)) * Math.sqrt(dotProduct(v2, v2));
        if (denominator == 0.0) {
            throw new IllegalArgumentException("There is a vector that is all zeroes!");
        }
        return dotProduct(v1, v2) / denominator;
    }

    private static int sum(ArrayList<Integer> v) {
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
