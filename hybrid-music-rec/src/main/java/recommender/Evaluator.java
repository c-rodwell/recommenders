package recommender;

import de.umass.lastfm.Track;

import java.util.Iterator;
import java.util.PriorityQueue;

import java.util.ArrayList;
import java.util.HashMap;

public class Evaluator {

    public static void main(String[] args) {


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

    //hide one track in history and try to guess it
    //return the position in the ranking, or null if it was not in ranking
    public Integer resultRank(HashMap<String, String> userHistory, int queueSize, boolean adjust_before, boolean adjust_after){
        //take out the last one from the history
        String hiddentrack = userHistory.remove(Integer.toString(userHistory.size()));
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
