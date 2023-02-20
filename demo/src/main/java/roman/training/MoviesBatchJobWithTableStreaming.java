package roman.training;

import java.math.BigDecimal;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import roman.training.domain.Movie;
import roman.training.domain.Rating;
import roman.training.functions.ComputeMovieAverageFunction;
import roman.training.functions.MovieAverageRatingToTitleJoinFunction;
import roman.training.functions.RowToMovieMapFunction;
import roman.training.functions.RowToRatingMapFunction;

public class MoviesBatchJobWithTableStreaming {
    public static final String MY_SQL_IP = "172.19.0.2";    
    
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(MoviesBatchJobWithTableStreaming.class);

        // setup the unified API
        // in this case: declare that the table programs should be executed in batch mode        
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);        
        tableEnv.getConfig().set("table.exec.resource.default-parallelism", "10");

        TableCreator.createMoviesTable(tableEnv, MY_SQL_IP);
        TableCreator.createRatingsTable(tableEnv, MY_SQL_IP);
        TableCreator.createTopRatedMoviesTable(tableEnv, MY_SQL_IP);

        DataStream<Rating> ratingsStream = tableEnv.toDataStream(tableEnv.from("ratings")).map(new RowToRatingMapFunction());
        DataStream<Movie> movieStream = tableEnv.toDataStream(tableEnv.from("movies")).map(new RowToMovieMapFunction());

        DataStream<Tuple2<Long, BigDecimal>> averageMoviesRating = ratingsStream.keyBy(value -> value.getMovieId())
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .aggregate(new ComputeMovieAverageFunction());

        tableEnv.createTemporaryView("AverageMoviesRating", averageMoviesRating);
        
        Table top20RatingsTable = tableEnv.sqlQuery(
            "SELECT f0, f1 FROM AverageMoviesRating ORDER BY f1 DESC LIMIT 20"
        );
        
        tableEnv.toDataStream(top20RatingsTable)
        .join(movieStream).where(value -> value.getField("f0")).equalTo(value -> value.getMovieId())
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .apply(new MovieAverageRatingToTitleJoinFunction())
        .addSink(
        JdbcSink.sink(
        "insert into top_rated_movies (movie_id, title, average_rating) values (?, ?, ?) on duplicate key update average_rating=values(average_rating)",        
            (statement, movieAverageRating) -> {
            statement.setLong(1, movieAverageRating.getMovieId());
            statement.setString(2, movieAverageRating.getTitle());
            statement.setBigDecimal(3, movieAverageRating.getAverageRating());
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
                                .build())).setParallelism(1);
                                
        logger.info(streamEnv.getExecutionPlan());
        streamEnv.execute();    
    }    
}
