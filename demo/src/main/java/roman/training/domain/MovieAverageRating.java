package roman.training.domain;

import java.math.BigDecimal;

import org.apache.flink.api.java.tuple.Tuple3;

public class MovieAverageRating extends Tuple3<Long, String, BigDecimal> {
    public MovieAverageRating() {
        super();
    }

    public MovieAverageRating(Long movieId, String title, BigDecimal averageRating) {
        super(movieId, title, averageRating);    
    }

    public Long getMovieId() {
        return f0;
    }

    public String getTitle() {
        return f1;
    }
    
    public BigDecimal getAverageRating() {
        return f2;
    }

    public void setMovieId(Long movieId) {
        setField(movieId, 0);
    }

    public void setTitle(String title) {
        setField(title, 1);
    }

    public void setAverageRating(BigDecimal averageRating) {
        setField(averageRating, 2);
    }
    
}
