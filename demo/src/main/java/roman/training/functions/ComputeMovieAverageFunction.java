package roman.training.functions;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import roman.training.domain.Rating;

public class ComputeMovieAverageFunction implements AggregateFunction<Rating, Tuple3<Long, BigDecimal, BigDecimal>, Tuple2<Long, BigDecimal>> {
    @Override
    public Tuple3<Long, BigDecimal, BigDecimal> add(Rating value, Tuple3<Long, BigDecimal, BigDecimal> accumulator) {
        accumulator.f0 = value.getMovieId();
        accumulator.f1 = accumulator.f1.add(value.getRating());            
        accumulator.f2 = accumulator.f2.add(BigDecimal.ONE);
        return accumulator;
    }

    @Override
    public Tuple3<Long, BigDecimal, BigDecimal> merge(Tuple3<Long, BigDecimal, BigDecimal> a, Tuple3<Long, BigDecimal, BigDecimal> b) {
        a.f1 = a.f1.add(b.f1);
        a.f2 = a.f2.add(b.f2);

        return a;
    }

    @Override
    public Tuple3<Long, BigDecimal, BigDecimal> createAccumulator() {                
        return new Tuple3<Long, BigDecimal, BigDecimal>(0L, BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public Tuple2<Long, BigDecimal> getResult(Tuple3<Long, BigDecimal, BigDecimal> accumulator) {
        return new Tuple2<Long, BigDecimal>(accumulator.f0, accumulator.f1.equals(BigDecimal.ZERO) ? BigDecimal.ZERO : accumulator.f1.divide(accumulator.f2, 1, RoundingMode.HALF_UP ));
    }            
}