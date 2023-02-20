package roman.training.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import roman.training.domain.Movie;

public class RowToMovieMapFunction implements MapFunction<Row,Movie> {
    @Override
    public Movie map(Row value) throws Exception {                
        return new Movie(value.getFieldAs("movie_id"), value.getFieldAs("title"), value.getFieldAs("genres"));
    }                
}
