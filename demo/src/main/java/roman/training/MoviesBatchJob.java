package roman.training;

import java.math.BigDecimal;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import roman.training.domain.Movie;
import roman.training.domain.Rating;
import roman.training.functions.MovieAverageRatingGroupReduceFunction;
import roman.training.functions.MovieNameMapFunction;

public class MoviesBatchJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Rating> ratings = DataSetLookup.getRatings(env);
		DataSet<Movie> movies = DataSetLookup.getMovies(env);

		DataSet<Tuple2<Long, BigDecimal>> averageRating = ratings.groupBy(1).reduceGroup(new MovieAverageRatingGroupReduceFunction()).setParallelism(1);
		averageRating = averageRating.sortPartition(1, Order.DESCENDING).setParallelism(1);
		DataSet<Tuple2<Long, BigDecimal>> top10 = averageRating.first(20);
		
		DataSet<Tuple3<Long, String, BigDecimal>> top10WithName = top10.joinWithHuge(movies).where(0).equalTo(0).map(new MovieNameMapFunction());
		top10WithName.output(new DiscardingOutputFormat<>());
		env.execute();				
	}
}