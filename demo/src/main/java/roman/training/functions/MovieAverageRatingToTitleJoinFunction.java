package roman.training.functions;

import java.math.BigDecimal;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import roman.training.domain.Movie;
import roman.training.domain.MovieAverageRating;

public class MovieAverageRatingToTitleJoinFunction extends RichFlatJoinFunction<Row, Movie, MovieAverageRating> {
    private ListAccumulator<String> outAccumulator = new ListAccumulator<String>();

    @Override
    public void open(Configuration parameters) throws Exception {            
        super.open(parameters);
        getRuntimeContext().addAccumulator("printOutput", outAccumulator);
    }

    public void join(Row averageRating, Movie movie, org.apache.flink.util.Collector<MovieAverageRating> out) throws Exception {
        outAccumulator.add(averageRating.toString());
        out.collect(new MovieAverageRating(movie.getMovieId(), movie.getTitle(), (BigDecimal)averageRating.getField("f1")));
    };          
}
