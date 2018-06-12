package recommender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

public class Evaluator {

    public static void main(String[] args) throws IOException {

        // Check for program arguments
        if (args.length > 1) {
            System.out.println("Error: invalid args length.");
            printUsageMsg();
            System.exit(-1);
        }

        int numToRecommend = 100;
        if (args.length == 1) {
            try {
                numToRecommend = Integer.parseInt(args[1]);
                if (numToRecommend > Constants.NUM_TO_REC_MAX || numToRecommend < Constants.NUM_TO_REC_MIN) {
                    System.out.println("Error: invalid <number_of_recommendations>.");
                    printUsageMsg();
                    System.exit(-1);
                }
            } catch (Exception e) {
                System.out.println("Error: invalid <number_of_recommendations>.");
                printUsageMsg();
                System.exit(-1);
            }
        }


        ArrayList<HashMap<String, String>> historySet = getHistories();

        double[] noAdjust = evaluate(historySet, numToRecommend, false, false);
        System.out.println("no adjust: accuracy = " + noAdjust[0] + " , avg rank = " + noAdjust[2] + " , avg popularity = " + noAdjust[1]);

        double[] adjustBefore = evaluate(historySet, numToRecommend, true, false);
        System.out.println("adjust before: accuracy = " + adjustBefore[0] + " , avg rank = " + adjustBefore[2] + " , avg popularity = " + adjustBefore[1]);

        double[] adjustAfter = evaluate(historySet, numToRecommend, false, true);
        System.out.println("adjust after: accuracy = " + adjustAfter[0] + " , avg rank = " + adjustAfter[2] + " , avg popularity = " + adjustAfter[1]);

        double[] adjustBoth = evaluate(historySet, numToRecommend, true, true);
        System.out.println("adjust before and after: accuracy = " + adjustBoth[0] + " , avg rank = " + adjustBoth[2] + " , avg popularity = " + adjustBoth[1]);

        ESHelpers.close();

        System.exit(0);

    }

    public static ArrayList<HashMap<String, String>> getHistories() {
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
        for (String username : usernames) {
            for (int i = 1; i <= historiesPerUser; i++) {
                HashMap<String, String> history = ESHelpers.getHistoryForUser(username, i);
                histories.add(history);
            }
        }
        return histories;
    }

    private static double[] evaluate(ArrayList<HashMap<String, String>> historiesList, int queueSize, boolean adjust_before, boolean adjust_after) {

        double count = 0.0;
        double popCount = 0.0;
        double totalRank = 0.0;

        int historysize = historiesList.size();
        for (HashMap<String, String> history : historiesList) {
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
        double[] output = {accuracy, averagePop, averageRank};
        return output;

    }

    private static double[] resultRank(HashMap<String, String> userHistory, int queueSize, boolean adjust_before,
                                       boolean adjust_after) {

        HashMap<String, String> historyCopy = (HashMap) userHistory.clone();
        double popScore = 0.0;
        String hiddentrack = historyCopy.remove(Integer.toString(historyCopy.size())); // take out the last one from the history
        PriorityQueue<TrackScore> recommendations =
                Recommender.recommendTracksForUser(historyCopy, queueSize, adjust_before, adjust_after);

        double position = (double) queueSize; // count backward since poll gives lowest score first
        double retval = -1.0;
        int i = 1;
        while (!recommendations.isEmpty()) {
            TrackScore t = recommendations.poll();

            popScore += getPopularityScore(t.getTrackMid());
            System.out.println(i + ") " + t.toString());

            if (t.getTrackMid().equals(hiddentrack)) {
                retval = position;
            }
            position -= 1.0;
            i++;
        }
        System.out.println("**************************************************************************");
        System.out.println("**************************************************************************");

        popScore = popScore / (double) queueSize;

        double[] arr = {retval, popScore};
        return arr;
    }

    private static double getPopularityScore(String trackMid) {

        ArrayList<Integer> vector = ESHelpers.getVector(Constants.TRACK_VECTORS_INDEX, trackMid);
        return (double) Recommender.sum(vector);

    }

    private static void printUsageMsg() {
        System.out.println("usage : recommender <number_of_recommendations>");
        System.out.println("<number_of_recommendations> (optional) : MAX = " + Constants.NUM_TO_REC_MAX + ", MIN = " + Constants.NUM_TO_REC_MIN + ", DEFAULT = 100");
    }

}
