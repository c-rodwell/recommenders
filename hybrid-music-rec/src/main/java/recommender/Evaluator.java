package recommender;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

public class Evaluator {

    public static void main(String[] args) {

        double popScore = 0.0;
        double accuracy = evaluateAccuracy(100, popScore);

        System.out.println("accuracy is " + accuracy);

        System.exit(0);

    }

    public static double evaluateAccuracy(int queueSize, double popScore) {

        double count = 0.0;
        String username = "h0bbel"; // TODO: need to get a list of users we have data for
        int historysize = ESHelpers.getUserHistorySize(username);
        int numOfHistToEval = 1;
        for (int i = 1; i <= historysize && i <= numOfHistToEval; i++) {
            HashMap<String, String> history = ESHelpers.getHistoryForUser(username, i);
            Integer rank = resultRank(history, queueSize, false, false, popScore);
            if (rank != null) {
                count += 1.0;
            }
        }
        double accuracy = count / (double) historysize;
        return accuracy;
    }

    // hide one track in history and try to guess it
    // return the position in the ranking, or null if it was not in ranking
    public static Integer resultRank(HashMap<String, String> userHistory, int queueSize, boolean adjust_before, boolean adjust_after, double popScore) {
        String hiddentrack = userHistory.remove(Integer.toString(userHistory.size())); // take out the last one from the history
        PriorityQueue<TrackScore> recommendations = Recommender.recommendTracksForUser(userHistory, queueSize, adjust_before, adjust_after);

        Integer position = queueSize; // count backward since poll gives lowest score first
        while (!recommendations.isEmpty()){
            TrackScore t = recommendations.poll();

            popScore += getPopularityScore(t.getTrackMid());
            System.out.println(popScore);

            if (t.getTrackMid() == hiddentrack){
                System.out.println("Found hidden track: " + hiddentrack);
                return position;
            }
            position--;

        }

        System.out.println("popularity score is " + popScore / queueSize);

        return null;

    }

    public static double getPopularityScore(String trackMid) {

        ArrayList<Integer> vector = ESHelpers.getVector(Constants.TRACK_VECTORS_INDEX, trackMid);
        return (double) Recommender.sum(vector);

    }

}
