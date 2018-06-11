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

        ArrayList<HashMap<String, String>> historySet = getHistories();
        double[] noAdjust = evaluate(historySet, username, numToRecommend, false, false, historysize);
        double[] adjustBefore = evaluate(historySet, username, numToRecommend, true, false, historysize);
        double[] adjustAfter = evaluate(historySet, username, numToRecommend, false, true, historysize);
        double[] adjustBoth = evaluate(historySet, username, numToRecommend, true, true, historysize);
        
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
                "jablko",
                "fightingfriends"};
        int historiesPerUser = 1;
        ArrayList<HashMap<String, String>> histories = new ArrayList<>();
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

    private static double[] evaluate(ArrayList<HashMap<String, String>> historiesList, String username, int queueSize, boolean adjust_before, boolean adjust_after, int historysize) {

        double count = 0.0;
        double popCount = 0.0;
        double totalRank = 0.0;
        
    	String type = "NO ADJUST";
    	if (adjust_before && !adjust_after) {
    		type = "BEFORE";
    	} else if (adjust_after && !adjust_before) {
    		type = "AFTER";
    	} else if (adjust_before && adjust_after) {
    		type = "BEFORE AND AFTER";
    	}

    	int i = 1;
        for (HashMap<String, String> history: historiesList) {
        	System.out.println("***************************************************************************************");
            System.out.println("Track recommendations based on history #" + i + ":");
            System.out.println("Adjustment Type: " + type);
            System.out.println("***************************************************************************************");
            if (history != null) {
                double[] rankAndPop = resultRank(history, queueSize, adjust_before, adjust_after);
                if (rankAndPop[0] != -1.0) {
                    count += 1.0;
                    totalRank += rankAndPop[0];
                }
                popCount += rankAndPop[1];
            }
            i++;
        }

        double accuracy = count / (double) historysize;
        double averagePop = popCount / (double) historysize;
        double averageRank = totalRank / (double) historysize;
		double[] output =  {accuracy, averagePop, averageRank};
        
        return output;

    }

    private static double[] resultRank(HashMap<String, String> userHistory, int queueSize, boolean adjust_before,
                                     boolean adjust_after) {

        HashMap<String, String > historyCopy = (HashMap) userHistory.clone();
        double popScore = 0.0;
        
		String hiddentrack = historyCopy.remove(Integer.toString(historyCopy.size())); // take out the last one from the history
        System.out.println("Hidden Track: " + hiddentrack);
        PriorityQueue<TrackScore> recommendations =
                Recommender.recommendTracksForUser(userHistory, queueSize, adjust_before, adjust_after);

        double position = (double) queueSize; // count backward since poll gives lowest score first
        double retval = -1.0;
        int i = 1;
        while (!recommendations.isEmpty()){
            TrackScore t = recommendations.poll();

            popScore += getPopularityScore(t.getTrackMid());
            System.out.print(i + ": " + t.toString());

            if (t.getTrackMid().equals(hiddentrack)){
                retval = position;
                System.out.print(" HIT");
            }
            System.out.println();
            position -= 1.0;
            i++;
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
