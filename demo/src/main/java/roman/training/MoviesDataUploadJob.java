package roman.training;

import java.sql.Timestamp;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import roman.training.domain.Movie;
import roman.training.domain.Rating;
import roman.training.functions.PartitionIdRatingsEnrichementFunction;

public class MoviesDataUploadJob {
    public static final String MY_SQL_IP = "172.19.0.1";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Movie> movies = DataSetLookup.getMovies(env).setParallelism(1);

        movies.addSink(
            JdbcSink.sink(
        "insert into movies (movie_id, title, genres) values (?, ?, ?) on duplicate key update title=values(title), genres=values(genres)",        
            (statement, movie) -> {
                statement.setLong(1, movie.getMovieId());
                statement.setBytes(2, movie.getTitle().getBytes());
                statement.setString(3, movie.getGenres());
            }, 
            new JdbcExecutionOptions.Builder()
            .withBatchSize(1000)
            .withMaxRetries(0)
            .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                    .withUrl(String.format("jdbc:mariadb://%s:3306/test_db", MY_SQL_IP))
                                    .withDriverName("org.mariadb.jdbc.Driver")
                                    .withUsername("root")
                                    .withPassword("root123")
                                    .build())).setParallelism(2); 

        DataStreamSource<Rating> ratings = DataSetLookup.getRatings(env).setParallelism(1);        
        ratings
        .keyBy(value -> 1)
        .map(new PartitionIdRatingsEnrichementFunction())
        .addSink(
            JdbcSink.sink(
        "insert into ratings (user_id, partition_id, movie_id, rating, created_ts) values (?, ?, ?, ?, ?) on duplicate key update rating=values(rating), created_ts=values(created_ts)",        
            (statement, rating) -> {
            statement.setLong(1, rating.getUserId());
            statement.setLong(2, rating.getPartitionId());
            statement.setLong(3, rating.getMovieId());
            statement.setBigDecimal(4, rating.getRating());
            statement.setTimestamp(5, new Timestamp(rating.getTimestamp() * 1000));
            }, 
            new JdbcExecutionOptions.Builder()
            .withBatchSize(1000)
            .withMaxRetries(0)
            .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(String.format("jdbc:mariadb://%s:3306/test_db", MY_SQL_IP))
                                .withDriverName("org.mariadb.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("root123")
                                .build())).setParallelism(10); 
                                
        env.execute();
    }
}
