package recommender;

import de.umass.lastfm.Track;

import java.util.Iterator;
import java.util.PriorityQueue;

import java.util.ArrayList;
import java.util.HashMap;

public class Evaluator {

    public static void main(String[] args) {



//        HashMap<String, String> history = ESHelpers.getHistoryForUser(username, 1);
//        Integer rank = resultRank(history, 100, false, false);
//        if (rank == null){
//            System.out.println("failed to predict the hidden track");
//        } else {
//            System.out.println("rank is "+rank);
//        }

        double accuracy = evaluateAccuracy( 100);
        System.out.println("accuracy is "+accuracy);

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
//                HashMap<String, Double> noAdjustScores = recommendTracksForUser(history, 10, false, false);
//                System.out.println(noAdjustScores.values().toString());
//
//                // what to do next?

    }

    public static double evaluateAccuracy(int queueSize){
        double count = 0.0;
        String username = "h0bbel"; //need to get a list of users we have data for
        int historysize = ESHelpers.getUserHistorySize(username);
        for (int i = 1; i<=historysize; i++) {
            HashMap<String, String> history = ESHelpers.getHistoryForUser(username, 1);
            Integer rank = resultRank(history, queueSize, false, false);
            if (rank != null){
                count += 1.0;
            }
        }
        double accuracy = count / (double) historysize;
        return count;
    }

    //hide one track in history and try to guess it
    //return the position in the ranking, or null if it was not in ranking
    public static Integer resultRank(HashMap<String, String> userHistory, int queueSize, boolean adjust_before, boolean adjust_after){
        //take out the last one from the history
        //String hiddentrack = userHistory.remove(Integer.toString(userHistory.size()));
        String hiddentrack = userHistory.get(Integer.toString(userHistory.size()));
        PriorityQueue<TrackScore> recommendations = Recommender.recommendTracksForUser(userHistory, queueSize, adjust_before, adjust_after);
        Integer position = queueSize; //count backward since poll gives lowest score first
        while (! recommendations.isEmpty()){
            TrackScore t = recommendations.poll();
            if (t.getTrackMid() == hiddentrack){
                return position;
            }
            position --;
        }
        return null;
    }

}
