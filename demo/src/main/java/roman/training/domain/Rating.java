package roman.training.domain;

import java.math.BigDecimal;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"userId","movieId","rating","timestamp"})   
public class Rating extends Tuple4<Long, Long, BigDecimal, Long> {
    public Rating() {
        super();
    }

    public Rating(Long userId, Long movieId, BigDecimal rating, Long timestamp) {
        super(userId, movieId, rating, timestamp);
    }
    
    public Long getUserId() {
        return f0;
    }

    public Long getMovieId() {
        return f1;
    }

    public BigDecimal getRating() {
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

    public void setRating(BigDecimal rating) {
        setField(rating, 2);
    }

    public void setTimestamp(Long timestamp) {
        setField(timestamp, 3);
    }
}
