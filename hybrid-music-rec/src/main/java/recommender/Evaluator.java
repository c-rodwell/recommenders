package recommender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

public class Evaluator {

	private static final Logger LOG = Logger.getLogger(Evaluator.class);
	
    public static void main(String[] args) throws IOException {
    	
        // Check for program arguments
        if (args.length > 2 || args.length == 0) {
            LOG.info("Invalid args length.");
            LOG.info("usage: recommender <username> <number_of_recommendations>");
            LOG.info("<number_of_recommendations> (optional) : MAX = 100, MIN = 10, DEFAULT = 100");
            System.exit(-1);
        }

        int numToRecommend = 100;
        if (args.length >= 1 && args.length <= 2) {
        	if (!ESHelpers.isInUsers(args[0])) {
        		LOG.info("User does not exist.");
        		System.exit(0);
        	}
        	if (args.length == 2) {
	        	try {
	        		numToRecommend = Integer.parseInt(args[1]);
	                if (numToRecommend > 100 || numToRecommend < 10) {
	                    LOG.info("Invalid arg.");
	                    LOG.info("usage: recommender <username> <number_of_recommendations>");
	                    LOG.info("number_of_recommendations (optional) : MAX = 100, MIN = 10, DEFAULT = 100");
	                    System.exit(-1);
	                }
	            } catch(Exception e) {
	                LOG.info("Invalid arg.");
	                LOG.info("usage: recommender <username> <number_of_recommendations>");
	                LOG.info("number_of_recommendations (optional) : MAX = 100, MIN = 10, DEFAULT = 100");
	                System.exit(-1);
	            }
        	}
        }

        String username = args[0];
        System.out.println("User: " + username);
        System.out.println("Number of tracks to recommend: " + numToRecommend);

        double[] noAdjust = evaluate(username, numToRecommend, false, false);
        double[] adjustBefore = evaluate(username, numToRecommend, true, false);
        double[] adjustAfter = evaluate(username, numToRecommend, false, true);
        double[] adjustBoth = evaluate(username, numToRecommend, true, true);
        
        System.out.println("=======================================================================================");
        System.out.println("No adjustments: Accuracy = " + noAdjust[0] + ", AVG Popularity = " + noAdjust[1]);
        System.out.println("=======================================================================================");
        System.out.println("Adjust before CF: Accuracy = " + adjustBefore[0] + ", AVG popularity = " + adjustBefore[1]);
        System.out.println("=======================================================================================");
        System.out.println("Adjust after CF: Accuracy = " + adjustAfter[0] + ", AVG popularity = " + adjustAfter[1]);
        System.out.println("=======================================================================================");
        System.out.println("Adjust before and after: Accuracy = " + adjustBoth[0] + ", AVG popularity = " + adjustBoth[1]);
        System.out.println("=======================================================================================");

        ESHelpers.close();

        System.out.println("Done.");
        System.exit(0);

    }


    private static double[] evaluate(String username, int queueSize, boolean adjust_before, boolean adjust_after) {

        double count = 0.0;
        double popCount = 0.0;

        int historysize = ESHelpers.getUserHistorySize(username);
        System.out.println("---------------------------------------------------------------------------------------");
        System.out.println("History size: " + historysize);
        for (int i = 1; i <= historysize; i++) {
        	System.out.println("***************************************************************************************");
            System.out.println("Track recommendations based on history #" + i + ":");
            System.out.println("***************************************************************************************");
            HashMap<String, String> history = ESHelpers.getHistoryForUser(username, i);
            if (history != null) {
                double[] rankAndPop = resultRank(history, queueSize, adjust_before, adjust_after);
                if (rankAndPop[0] != -1.0) {
                    count += 1.0;
                }
                popCount += rankAndPop[1];
            }
        }

        double accuracy = count / (double) historysize;
        double averagePop = popCount / (double) historysize;
        double[] output =  {accuracy, averagePop};
        
        return output;

    }

    private static double[] resultRank(HashMap<String, String> userHistory, int queueSize, boolean adjust_before,
                                     boolean adjust_after) {

        double popScore = 0.0;
        String hiddentrack = userHistory.remove(Integer.toString(userHistory.size())); // take out the last one from the history
        System.out.println("Hidden Track: " + hiddentrack);
        PriorityQueue<TrackScore> recommendations =
                Recommender.recommendTracksForUser(userHistory, queueSize, adjust_before, adjust_after);

        double position = (double) queueSize; // count backward since poll gives lowest score first
        double retval = -1.0;
        while (!recommendations.isEmpty()){
            TrackScore t = recommendations.poll();

            popScore += getPopularityScore(t.getTrackMid());
            System.out.print(t.toString());

            if (t.getTrackMid().equals(hiddentrack)){
                retval = position;
                System.out.print(" HIT");
            }
            System.out.println();
            position -= 1.0;
        }

        popScore = popScore / (double) queueSize;

        double[] arr = {retval, popScore};
        return arr;
    }

    private static double getPopularityScore(String trackMid) {

        ArrayList<Integer> vector = ESHelpers.getVector(Constants.TRACK_VECTORS_INDEX, trackMid);
        return (double) Recommender.sum(vector);

    }

}
