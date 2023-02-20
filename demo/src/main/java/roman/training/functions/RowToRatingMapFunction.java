package roman.training.functions;

import java.time.LocalDateTime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import roman.training.domain.Rating;

public class RowToRatingMapFunction implements MapFunction<Row, Rating> {
    @Override
    public Rating map(Row value) throws Exception {
        LocalDateTime createdTs = value.getFieldAs("created_ts");                    
        return new Rating(value.getFieldAs("user_id"), value.getFieldAs("movie_id"), value.getFieldAs("rating"), createdTs.toEpochSecond(ZoneOffset.UTC));
    }            
}
