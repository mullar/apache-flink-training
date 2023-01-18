package roman.training;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MoviesDataUploadJob {
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
                                .withUrl("jdbc:mysql://172.19.0.2:3306/test_db")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("root123")
                                .build())).setParallelism(50); 
                                
        env.execute();
    }
}
