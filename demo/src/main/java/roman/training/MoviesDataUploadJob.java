package roman.training;

import java.sql.Timestamp;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MoviesDataUploadJob {
    public static final String MY_SQL_IP = "172.19.0.2";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Movie> movies = DataSetLookup.getMovies(env).setParallelism(2);

        movies.keyBy(movie -> movie.getMovieId()).addSink(
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
                                    .withUrl(String.format("jdbc:mysql://%s:3306/test_db", MY_SQL_IP))
                                    .withDriverName("com.mysql.cj.jdbc.Driver")
                                    .withUsername("root")
                                    .withPassword("root123")
                                    .build())).setParallelism(50); 

        DataStreamSource<Rating> ratings = DataSetLookup.getRatings(env).setParallelism(10);

        ratings.keyBy(rating -> 
            new KeySelector<Rating, Tuple2<Long, Long>>() {
                public Tuple2<Long, Long> getKey(Rating rating) throws Exception {
                    return new Tuple2<Long, Long>(rating.getUserId(), rating.getMovieId());
                }
            }).addSink(
            JdbcSink.sink(
        "insert into ratings (user_id, movie_id, rating, created_ts) values (?, ?, ?, ?) on duplicate key update rating=values(rating), created_ts=values(created_ts)",        
            (statement, rating) -> {
            statement.setLong(1, rating.getUserId());
            statement.setLong(2, rating.getMovieId());
            statement.setDouble(3, rating.getRating());
            statement.setTimestamp(4, new Timestamp(rating.getTimestamp() * 1000));
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
                                .build())).setParallelism(100); 
                                
        env.execute();
    }
}
