package roman.training;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"movieId","title","genres"})                
public class Movie extends Tuple3<Long, String, String> {
    private static final long serialVersionUID = 1L;

    public Movie() {
        super();
    }

    public Movie(Long movieId, String title, String genres) {
        super(movieId, title, genres);    
    }

    public Long getMovieId() {
        return f0;
    }

    public String getTitle() {
        return f1;
    }
    
    public String getGenres() {
        return f2;
    }

    public void setMovieId(Long movieId) {
        setField(movieId, 0);
    }

    public void setTitle(String title) {
        setField(title, 1);
    }

    public void setGenres(String genres) {
        setField(genres, 2);
    }
}
