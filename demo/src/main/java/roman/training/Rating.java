package roman.training;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"userId","movieId","rating","timestamp"})   
public class Rating extends Tuple4<Long, Long, Double, Long> {
    public Rating() {
        super();
    }

    public Rating(Long userId, Long movieId, Double rating, Long timestamp) {
        super(userId, movieId, rating, timestamp);
    }
    
    public Long getUserId() {
        return f0;
    }

    public Long getMovieId() {
        return f1;
    }

    public Double getRating() {
        return f2;
    }

    public Long getTimestamp() {
        return f3;
    }

    public void setUserId(Long userId) {
        setField(userId, 0);
    }

    public void setMovieId(Long movieId) {
        setField(movieId, 1);
    }

    public void setRating(Double rating) {
        setField(rating, 2);
    }

    public void setTimestamp(Long timestamp) {
        setField(timestamp, 3);
    }
}
