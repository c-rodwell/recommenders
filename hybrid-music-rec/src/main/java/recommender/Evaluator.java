package recommender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

public class Evaluator {

    private static double popScore = 0.0;

    public static void main(String[] args) throws IOException {

        evaluate(100);

        ESHelpers.close();

        System.exit(0);

    }

    private static double evaluate(int queueSize) {

        double count = 0.0;

        String username = "killeroid"; // TODO: need to get a list of users we have data for

        int historysize = ESHelpers.getUserHistorySize(username);
        for (int i = 1; i <= historysize; i++) {
            System.out.println("**********************************************");
            System.out.println("Track recommendations for history #" + i);
            HashMap<String, String> history = ESHelpers.getHistoryForUser(username, i);
            if (history != null) {
                Integer rank = resultRank(history, queueSize, false, false);
                if (rank != -1) {
                    count += 1.0;
                }
            }
        }

        double accuracy = count / (double) historysize;

//        System.out.println("**********************************************");
//        System.out.println("Accuracy = " + accuracy);
        return accuracy;

    }

    // hide one track in history and try to guess it
    // return the position in the ranking, or null if it was not in ranking
    private static int resultRank(HashMap<String, String> userHistory, int queueSize, boolean adjust_before,
                                     boolean adjust_after) {

        String hiddentrack = userHistory.remove(Integer.toString(userHistory.size())); // take out the last one from the history
        PriorityQueue<TrackScore> recommendations =
                Recommender.recommendTracksForUser(hiddentrack, userHistory, queueSize, adjust_before, adjust_after);

        int position = queueSize; // count backward since poll gives lowest score first
        int retval = -1;
        while (!recommendations.isEmpty()){
            TrackScore t = recommendations.poll();

            popScore += getPopularityScore(t.getTrackMid());
            System.out.println(t.toString());

            if (t.getTrackMid().equals(hiddentrack)){
                retval = position;
            }
            position--;
        }

        System.out.println("Popularity = " + popScore / queueSize);

        return retval;

    }

    private static double getPopularityScore(String trackMid) {

        ArrayList<Integer> vector = ESHelpers.getVector(Constants.TRACK_VECTORS_INDEX, trackMid);
        return (double) Recommender.sum(vector);

    }

}
