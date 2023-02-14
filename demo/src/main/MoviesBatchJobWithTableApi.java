package roman.training;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoviesBatchJobWithTableApi {
    public static final String MY_SQL_IP = "172.19.0.2";    

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(MoviesBatchJobWithTableApi.class);

        // setup the unified API
        // in this case: declare that the table programs should be executed in batch mode        
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        // Using table descriptors
        final TableDescriptor moviesTableDescriptor = TableDescriptor.forConnector("jdbc")
        .schema(Schema.newBuilder()
        .column("movie_id", DataTypes.BIGINT().notNull())
        .column("title", DataTypes.STRING().notNull())
        .column("genres", DataTypes.STRING().notNull())
        .primaryKey("movie_id")
        .build())
        .option(JdbcConnectorOptions.URL, String.format("jdbc:mysql://%s:3306/test_db", MY_SQL_IP))
        .option(JdbcConnectorOptions.DRIVER, "com.mysql.cj.jdbc.Driver")
        .option(JdbcConnectorOptions.TABLE_NAME, "movies")
        .option(JdbcConnectorOptions.USERNAME, "root")
        .option(JdbcConnectorOptions.PASSWORD, "root123")
        //.option(JdbcConnectorOptions.SCAN_PARTITION_COLUMN, "movie_id")
        //.option(JdbcConnectorOptions.SCAN_PARTITION_LOWER_BOUND, 1L)
        //.option(JdbcConnectorOptions.SCAN_PARTITION_UPPER_BOUND, 209171L)
        //.option(JdbcConnectorOptions.SCAN_PARTITION_NUM, 209171 / 1000)
        .build();
        tableEnv.createTemporaryTable("movies", moviesTableDescriptor);

        final TableDescriptor ratingsTableDescriptor = TableDescriptor.forConnector("jdbc")
        .schema(Schema.newBuilder()
        .column("user_id", DataTypes.BIGINT().notNull())
        .column("movie_id", DataTypes.BIGINT().notNull())
        .column("rating", DataTypes.DECIMAL(2, 1).notNull())
        .column("created_ts", DataTypes.TIMESTAMP().notNull())
        .primaryKey("user_id", "movie_id")
        .build())
        .option(JdbcConnectorOptions.URL, String.format("jdbc:mysql://%s:3306/test_db", MY_SQL_IP))
        .option(JdbcConnectorOptions.DRIVER, "com.mysql.cj.jdbc.Driver")
        .option(JdbcConnectorOptions.TABLE_NAME, "ratings")
        .option(JdbcConnectorOptions.USERNAME, "root")
        .option(JdbcConnectorOptions.PASSWORD, "root123")
        //.option(JdbcConnectorOptions.SCAN_PARTITION_COLUMN, "movie_id")
        //.option(JdbcConnectorOptions.SCAN_PARTITION_LOWER_BOUND, 1L)
        //.option(JdbcConnectorOptions.SCAN_PARTITION_UPPER_BOUND, 209171L)
        //.option(JdbcConnectorOptions.SCAN_PARTITION_NUM, 10500)
        .build();
        tableEnv.createTemporaryTable("ratings", ratingsTableDescriptor);  
        
        final TableDescriptor topRatedMoviesTableDescriptor = TableDescriptor.forConnector("jdbc")
        .schema(Schema.newBuilder()
        .column("movie_id", DataTypes.BIGINT().notNull())
        .column("title", DataTypes.STRING().notNull())
        .column("average_rating", DataTypes.DECIMAL(2, 1).notNull())
        .primaryKey("movie_id")
        .build())
        .option(JdbcConnectorOptions.URL, String.format("jdbc:mysql://%s:3306/test_db", MY_SQL_IP))
        .option(JdbcConnectorOptions.DRIVER, "com.mysql.cj.jdbc.Driver")
        .option(JdbcConnectorOptions.TABLE_NAME, "top_rated_movies")
        .option(JdbcConnectorOptions.USERNAME, "root")
        .option(JdbcConnectorOptions.PASSWORD, "root123")
        .build();
        tableEnv.createTemporaryTable("top_rated_movies", topRatedMoviesTableDescriptor);

        /**tableEnv.executeSql(
            "INSERT INTO top_rated_movies " +
            "SELECT m.movie_id, m.title, s.average_rating " +
            "FROM ( " +
                "SELECT movie_id, AVG(rating) AS average_rating " +
                "FROM ratings " +
                "GROUP BY movie_id " + 
                "ORDER BY average_rating DESC " + 
                "LIMIT 20" + 
            ") s, movies m " + 
            "WHERE s.movie_id = m.movie_id"
        );*/        

        DataStream<Rating> ratingsStream = tableEnv.toDataStream(tableEnv.from("ratings")).
            map(new MapFunction<Row, Rating>() {
                @Override
                public Rating map(Row value) throws Exception {                
                    return new Rating(value.getFieldAs("user_id"), value.getFieldAs("movie_id"), value.getFieldAs("rating"), value.getFieldAs("created_ts"));
                }            
            });

        DataStream<Movie> movieStream = tableEnv.toDataStream(tableEnv.from("movies"))
            .map(new MapFunction<Row,Movie>() {
                @Override
                public Movie map(Row value) throws Exception {                
                    return new Movie(value.getFieldAs("movie_id"), value.getFieldAs("title"), value.getFieldAs("genres"));
                }                
            });

        ratingsStream.keyBy(value -> value.getMovieId())
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .apply(new WindowFunction<Rating, Tuple2<Long, Double>, Long,TimeWindow>() {
            public void apply(Long key, TimeWindow window, java.lang.Iterable<Rating> input, org.apache.flink.util.Collector<Tuple2<Long,Double>> out) throws Exception {
                Double sum = 0.0;
                long length = 0;
                long movieId = 0;
        
                for (Rating value : input) {
                    movieId = value.getMovieId();
                    sum += value.getRating();
                    ++length;
                }
        
                out.collect(new Tuple2<Long, Double>(movieId, sum / length));	
            };
        })
        .join(movieStream).where(value -> value.f0).equalTo(value -> value.getMovieId())
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .apply(new FlatJoinFunction<Tuple2<Long, Double>, Movie, MovieAverageRating>() {
            public void join(org.apache.flink.api.java.tuple.Tuple2<Long,Double> averageRating, Movie movie, org.apache.flink.util.Collector<MovieAverageRating> out) throws Exception {
                out.collect(new MovieAverageRating(movie.getMovieId(), movie.getTitle(), averageRating.f1));
            };          
        })
        .addSink(
        JdbcSink.sink(
        "insert into top_rates_movies (movie_id, title, average_rating) values (?, ?, ?) on duplicate key update average_rating=values(average_rating)",        
            (statement, movieAverageRating) -> {
            statement.setLong(1, movieAverageRating.getMovieId());
            statement.setString(2, movieAverageRating.getTitle());
            statement.setDouble(3, movieAverageRating.getAverageRating());
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
        streamEnv.execute();    
    }    
}
