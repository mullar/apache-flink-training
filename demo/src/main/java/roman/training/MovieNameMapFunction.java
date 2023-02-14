package roman.training;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public class MovieNameMapFunction extends RichMapFunction<Tuple2<Tuple2<Long,Double>,Movie>,Tuple3<Long,String, Double>> {
    private ListAccumulator<String> outAccumulator = new ListAccumulator<String>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator("printOutput", outAccumulator);
    }

    @Override
    public Tuple3<Long, String, Double> map(Tuple2<Tuple2<Long, Double>, Movie> value) throws Exception {	
        Tuple3<Long, String, Double> tuple = new Tuple3<Long, String, Double>(value.f0.f0, value.f1.getTitle(), value.f0.f1);

        outAccumulator.add(tuple.toString());
        return tuple;
    }			
}