package roman.training;

import org.apache.flink.api.java.tuple.Tuple2;

public class Rating extends Tuple2<Long, Double> {
    public Rating() {
        super();
    }

    public Rating(Long id, Double rating) {
        super(id, rating);
    }
    
    public Long getId() {
        return f0;
    }

    public Double getRating() {
        return f1;
    }
}
