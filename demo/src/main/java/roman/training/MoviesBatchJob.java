package roman.training;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class MoviesBatchJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Rating> ratings = getRatings(env);
		DataSet<Movie> movies = getMovies(env);

		DataSet<Tuple2<Long, Double>> averageRating = ratings.groupBy(0).reduceGroup(new MovieAverageRatingGroupReduceFunction()).setParallelism(4);
		averageRating = averageRating.sortPartition(1, Order.DESCENDING).setParallelism(4);
		DataSet<Tuple2<Long, Double>> top10 = averageRating.first(20);
		
		DataSet<Tuple3<Long, String, Double>> top10WithName = top10.joinWithHuge(movies).where(0).equalTo(0).map(new MovieNameMapFunction());
		
		top10WithName.print();
	}

	private static DataSet<Rating> getRatings(ExecutionEnvironment env) {
		return env.readCsvFile("/data/movies/ratings.csv")
				.fieldDelimiter(",")
				.ignoreFirstLine()
				.includeFields(false, true, true)
				.tupleType(Rating.class).setParallelism(4);
    }

	private static DataSet<Movie> getMovies(ExecutionEnvironment env) {
		return env.readCsvFile("/data/movies/movies.csv")
				.fieldDelimiter(",")
				.ignoreFirstLine()
				.includeFields(true, true)
				.tupleType(Movie.class);
    }
}