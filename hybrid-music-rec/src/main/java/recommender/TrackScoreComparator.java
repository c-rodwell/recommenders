package recommender;

import java.util.Comparator;

public class TrackScoreComparator implements Comparator<TrackScore> {

    @Override
    public int compare(TrackScore o1, TrackScore o2) {
        if(o1.getScore() > o2.getScore()){
            return 1;
        } else {
            return -1;
        }
    }
}
