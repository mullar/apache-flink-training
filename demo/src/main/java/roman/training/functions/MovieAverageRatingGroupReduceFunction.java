package roman.training.functions;

import java.math.BigDecimal;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import roman.training.domain.Rating;

public class MovieAverageRatingGroupReduceFunction implements GroupReduceFunction<Rating, Tuple2<Long, BigDecimal>> {
    @Override
    public void reduce(Iterable<Rating> values, Collector<Tuple2<Long, BigDecimal>> out) throws Exception {
        BigDecimal sum = BigDecimal.ZERO;
        sum.setScale(2);

        BigDecimal length = BigDecimal.ZERO;
        long movieId = 0;

        for (Rating value : values) {
            movieId = value.getMovieId();
            sum = sum.add(value.getRating());
            length = length.add(BigDecimal.ONE);
        }

        out.collect(new Tuple2<Long, BigDecimal>(movieId, sum.divide(length)));				
    }
}