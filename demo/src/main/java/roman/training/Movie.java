package roman.training;

import org.apache.flink.api.java.tuple.Tuple2;

public class Movie extends Tuple2<Long, String> {
    public Movie() {
        super();
    }

    public Movie(Long id, String title) {
        super(id, title);    
    }

    public Long getId() {
        return f0;
    }

    public String getTitle() {
        return f1;
    }    
}
