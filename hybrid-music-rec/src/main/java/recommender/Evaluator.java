package recommender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

public class Evaluator {


    public static void main(String[] args) throws IOException {

        ArrayList<HashMap<String, String>> historySet = getHistories();

        double[] noAdjust = evaluate(historySet,100, false, false);
        System.out.println("no adjust: accuracy = "+noAdjust[0]+" , avg rank = "+noAdjust[2]+" , avg popularity = "+noAdjust[1]);

        double[] adjustBefore = evaluate(historySet,100, true, false);
        System.out.println("adjust before: accuracy = "+adjustBefore[0]+" , avg rank = "+adjustBefore[2]+" , avg popularity = "+adjustBefore[1]);

        double[] adjustAfter = evaluate(historySet,100, false, true);
        System.out.println("adjust after: accuracy = "+adjustAfter[0]+" , avg rank = "+adjustAfter[2]+" , avg popularity = "+adjustAfter[1]);

        double[] adjustBoth = evaluate(historySet,100, true, true);
        System.out.println("adjust before and after: accuracy = "+adjustBoth[0]+" , avg rank = "+adjustBoth[2]+" , avg popularity = "+adjustBoth[1]);

        ESHelpers.close();

        System.exit(0);

    }

    public static ArrayList<HashMap<String, String>> getHistories(){
        String[] usernames = {
                "amifamousnow",
                "gsnedders",
                "beefigursk",
                "paularms",
                "rockstr",
                "blue_shirt_2",
                "jmullan",
                "rob",
                "halr9000",
                "jablko"};
        int historiesPerUser = 1;
        ArrayList<HashMap<String, String>> histories = new ArrayList();
        for (String username: usernames){
            for (int i=1; i<=historiesPerUser; i++){
                HashMap<String, String> history = ESHelpers.getHistoryForUser(username, i);
                histories.add(history);
            }
        }
        return histories;
    }

    public static String historyString(HashMap<String, String> history){
        String s = "";
        for(String mbid: history.values()){
            s = s + mbid + " , ";
        }
        return s;
    }

    private static double[] evaluate(ArrayList<HashMap<String, String>> historiesList, int queueSize, boolean adjust_before, boolean adjust_after) {

        double count = 0.0;
        double popCount = 0.0;
        double totalRank = 0.0;

        int historysize = historiesList.size();//ESHelpers.getUserHistorySize(username);
        for (HashMap<String, String> history: historiesList) {
            //System.out.println("**********************************************");
            System.out.println("Track recommendations for history #" + historyString(history));
            if (history != null) {
                double[] rankAndPop = resultRank(history, queueSize, adjust_before, adjust_after);
                if (rankAndPop[0] != -1.0) {
                    count += 1.0;
                    totalRank += rankAndPop[0];
                }
                popCount += rankAndPop[1];
            }
        }

        double accuracy = count / (double) historysize;
        double averagePop = popCount / (double) historysize;
        double averageRank = totalRank / (double) historysize;

//        System.out.println("**********************************************");
//        System.out.println("Accuracy = " + accuracy);
//        System.out.println("average Popularity = " + averagePop);
        double[] output =  {accuracy, averagePop, averageRank};
        return output;

    }

    // hide one track in history and try to guess it
    // return the position in the ranking, or null if it was not in ranking
    private static double[] resultRank(HashMap<String, String> userHistory, int queueSize, boolean adjust_before,
                                     boolean adjust_after) {

        HashMap<String, String > historyCopy = (HashMap) userHistory.clone();
        double popScore = 0.0;
        String hiddentrack = historyCopy.remove(Integer.toString(historyCopy.size())); // take out the last one from the history
        PriorityQueue<TrackScore> recommendations =
                Recommender.recommendTracksForUser(historyCopy, queueSize, adjust_before, adjust_after);

        double position = (double) queueSize; // count backward since poll gives lowest score first
        double retval = -1.0;
        while (!recommendations.isEmpty()){
            TrackScore t = recommendations.poll();

            popScore += getPopularityScore(t.getTrackMid());
            //System.out.println(t.toString());

            if (t.getTrackMid().equals(hiddentrack)){
                retval = position;
            }
            position -= 1.0;
        }

        popScore = popScore / (double) queueSize;
        //System.out.println("Popularity = " + popScore / queueSize);

        double[] arr = {retval, popScore};
        return arr;
    }

    private static double getPopularityScore(String trackMid) {

        ArrayList<Integer> vector = ESHelpers.getVector(Constants.TRACK_VECTORS_INDEX, trackMid);
        return (double) Recommender.sum(vector);

    }

}
