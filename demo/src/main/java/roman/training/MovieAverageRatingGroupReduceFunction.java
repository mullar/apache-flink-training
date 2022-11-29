package roman.training;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class MovieAverageRatingGroupReduceFunction implements GroupReduceFunction<Rating, Tuple2<Long, Double>> {
    @Override
    public void reduce(Iterable<Rating> values, Collector<Tuple2<Long, Double>> out) throws Exception {
        Double sum = 0.0;
        long length = 0;
        long movieId = 0;

        for (Rating value : values) {
            movieId = value.getId();
            sum += value.getRating();
            ++length;
        }

        out.collect(new Tuple2<Long, Double>(movieId, sum / length));				
    }
}