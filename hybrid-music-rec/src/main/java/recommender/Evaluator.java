package recommender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

public class Evaluator {
	
    public static void main(String[] args) throws IOException {
    	
        // Check for program arguments
        if (args.length > 2 || args.length == 0) {
        	System.out.println("Error: invalid args length.");
        	printUsageMsg();
            System.exit(-1);
        }

        int numToRecommend = 100;
        if (args.length > 0) {
        	if (!ESHelpers.isInUsers(args[0])) {
        		System.out.println("Error: user does not exist.");
        		System.exit(0);
        	}
        	if (args.length == 2) {
	        	try {
	        		numToRecommend = Integer.parseInt(args[1]);
	                if (numToRecommend > Constants.NUM_TO_REC_MAX || numToRecommend < Constants.NUM_TO_REC_MIN) {
	                	System.out.println("Error: invalid <number_of_recommendations>.");
	                    printUsageMsg();
	                    System.exit(-1);
	                }
	            } catch(Exception e) {
	            	System.out.println("Error: invalid <number_of_recommendations>.");
	                printUsageMsg();
	                System.exit(-1);
	            }
        	}
        }

        String username = args[0];
        System.out.println("User: " + username);
        System.out.println("Number of tracks to recommend: " + numToRecommend);
        
        int historysize = ESHelpers.getUserHistorySize(username);
        System.out.println("---------------------------------------------------------------------------------------");
        System.out.println("History size: " + historysize);

        double[] noAdjust = evaluate(username, numToRecommend, false, false, historysize);
        double[] adjustBefore = evaluate(username, numToRecommend, true, false, historysize);
        double[] adjustAfter = evaluate(username, numToRecommend, false, true, historysize);
        double[] adjustBoth = evaluate(username, numToRecommend, true, true, historysize);
        
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


    private static double[] evaluate(String username, int queueSize, boolean adjust_before, boolean adjust_after, int historysize) {
    	
    	String type = "NO ADJUST";
    	if (adjust_before && !adjust_after) {
    		type = "BEFORE";
    	} else if (adjust_after && !adjust_before) {
    		type = "AFTER";
    	} else if (adjust_before && adjust_after) {
    		type = "BEFORE AND AFTER";
    	}

        double count = 0.0;
        double popCount = 0.0;

        for (int i = 1; i <= historysize; i++) {
        	System.out.println("***************************************************************************************");
            System.out.println("Track recommendations based on history #" + i + ":");
            System.out.println("Adjustment Type: " + type);
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
    
    private static void printUsageMsg() {
    	System.out.println("usage : recommender [username] <number_of_recommendations>");
    	System.out.println("<number_of_recommendations> (optional) : MAX = "+ Constants.NUM_TO_REC_MAX + ", MIN = " + Constants.NUM_TO_REC_MIN + ", DEFAULT = 100");
    }

}
