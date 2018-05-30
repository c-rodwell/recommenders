package recommender;

public class Recommender {
    public static void main(){
        //for now this is just testing
        int[] v1 = {0,0,3,32,5};
        int[] v2 = {14,10,0,0,0};
        int[] v3 = {1,2,3,4,5};

        //simple similarity - commutative
        double sim12 = trackSimilarity(v1, v2);
        double sim13 = trackSimilarity(v1, v3);
        double sim23 = trackSimilarity(v2, v3);
        System.out.println("similarity of v1 and v2 is: "+sim12);
        System.out.println("similarity of v1 and v3 is: "+sim13);
        System.out.println("similarity of v2 and v3 is: "+sim23);

        //adjusted similarities relative to v3:

    }

    //based on "Evaluating Hybrid Music Recommender Systems" by Hornung et al
    //weighted sum of different cosine distance similarity functions on user's listening history
    public String[] recommendTracks(String user, int numToRecommend){
        String[] tracks = new String[numToRecommend];
        //decide what tracks to recommend for the user
        //get user's listening history
        //for each track in dataset:
            //for each track in user's history:
                //compute similarity, add it to relevance score for the track
                //maybe compute similarities first, put in ES)
        //return tracks with most relevance
        return tracks;
    }


    //similarity of two tracks = cosine distance of the user listening vectors
    //subtract from each vector : "the average number of times the user j listened to a track"
    //should we subtract here? or subtract when creating the vectors?
    //adjust by track popularity - defined by listening total for all users for the track- this is sum of the vector
    public static double trackSimilarity(int[] track1, int[] track2){
        return cosinedistance(track1, track2);
    }

    public double adjustedTrackSimilarity(int[] track1, int[] track2){
        return cosinedistance(track1, track2)/sum(track2);
    }

    public static double cosinedistance(int[]v1, int[]v2){
        return dotproduct(v1,v2)/((Math.sqrt(dotproduct(v1,v1)))*(Math.sqrt(dotproduct(v2,v2))));
    }

    public int sum(int[] v){
        int s = 0;
        for(int i=0; i<v.length; i++){
            s +=v[i];
        }
        return s;
    }

    public static double dotproduct(int[]v1, int[]v2){
        if (v1.length != v2.length){
            throw new IllegalArgumentException("vector sizes don't match");
        }
        double sum=0.0;
        for (int i=0; i< v1.length; i++){
            sum += v1[i]*v2[i];
        }
        return sum;
    }

}
