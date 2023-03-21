package roman.training.domain;

import java.math.BigDecimal;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"userId","movieId","rating","timestamp"})   
public class Rating extends Tuple5<Long, Long, BigDecimal, Long, Long> {
    public Rating() {
        super();
        setField(0L, 4);
    }

    public Rating(Long userId, Long movieId, BigDecimal rating, Long timestamp, Long partitionId) {
        super(userId, movieId, rating, timestamp, partitionId);
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

    public Long getPartitionId() {
        return f4;
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

    public void setPartitionId(Long partitionId) {
        setField(partitionId, 4);
    }
}
