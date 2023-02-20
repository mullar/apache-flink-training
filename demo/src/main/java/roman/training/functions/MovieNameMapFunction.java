package roman.training.functions;

import java.math.BigDecimal;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import roman.training.domain.Movie;

public class MovieNameMapFunction extends RichMapFunction<Tuple2<Tuple2<Long,BigDecimal>,Movie>,Tuple3<Long,String, BigDecimal>> {
    private ListAccumulator<String> outAccumulator = new ListAccumulator<String>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator("printOutput", outAccumulator);
    }

    @Override
    public Tuple3<Long, String, BigDecimal> map(Tuple2<Tuple2<Long, BigDecimal>, Movie> value) throws Exception {	
        Tuple3<Long, String, BigDecimal> tuple = new Tuple3<Long, String, BigDecimal>(value.f0.f0, value.f1.getTitle(), value.f0.f1);

        outAccumulator.add(tuple.toString());
        return tuple;
    }			
}