package recommender;

public class TrackScore {

    private String trackMid;
    private double score;

    public TrackScore(String trackMid, double score) {
        this.trackMid = trackMid;
        this.score = score;
    }

    public String getTrackMid() {
        return trackMid;
    }

    public void setTrackMid(String trackMid) {
        this.trackMid = trackMid;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "TrackScore{" +
                "trackMid='" + trackMid + '\'' +
                ", score=" + score +
                '}';
    }
}
