package roman.training;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class MovieNameMapFunction implements MapFunction<Tuple2<Tuple2<Long,Double>,Movie>,Tuple3<Long,String, Double>> {
    @Override
    public Tuple3<Long, String, Double> map(Tuple2<Tuple2<Long, Double>, Movie> value) throws Exception {				
        return new Tuple3<Long, String, Double>(value.f0.f0, value.f1.getTitle(), value.f0.f1);
    }			
}