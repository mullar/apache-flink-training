package roman.training;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoviesBatchJobWithTableStreamingStateV2 {
    public static final String MY_SQL_IP = "172.19.0.2";    
    
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(MoviesBatchJobWithTableStreamingStateV2.class);

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

        DataStream<Movie> movieStream = tableEnv.toDataStream(tableEnv.from("movies"))
            .map(new MapFunction<Row,Movie>() {
                @Override
                public Movie map(Row value) throws Exception {                
                    logger.error(value.getField("movie_id").toString());
                    return new Movie(value.getFieldAs("movie_id"), value.getFieldAs("title"), value.getFieldAs("genres"));                
                }                
            });

        DataStream<Tuple3<Long, Double, Long>> ratingsStream = tableEnv.toDataStream(tableEnv.from("ratings"))
            .map(new MapFunction<Row, Tuple3<Long, Double, Long>>() {
                @Override
                public Tuple3<Long, Double, Long> map(Row value) throws Exception {                
                    return new Tuple3(value.getFieldAs("movie_id"), value.getFieldAs("rating"), 1L);
                }            
            });

        ratingsStream.keyBy(value -> value.f0)
        .reduce(new ReduceFunction<Tuple3<Long,Double,Long>>() {
            @Override
            public Tuple3<Long, Double, Long> reduce(Tuple3<Long, Double, Long> value1,
                    Tuple3<Long, Double, Long> value2) throws Exception {
                        
                Tuple3<Long,Double,Long> returnTuple = new Tuple3<Long,Double,Long>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);                
                return returnTuple;
            }
            
        })
        .map(new MapFunction<Tuple3<Long,Double,Long>,Tuple2<Long, Double>>() {
            @Override
            public Tuple2<Long, Double> map(Tuple3<Long, Double, Long> value) throws Exception {
                return new Tuple2(value.f0, value.f1 / value.f2);
            }            
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
