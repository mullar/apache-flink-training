package roman.training;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
        .option(JdbcConnectorOptions.SCAN_PARTITION_COLUMN, "movie_id")
        .option(JdbcConnectorOptions.SCAN_PARTITION_LOWER_BOUND, 1L)
        .option(JdbcConnectorOptions.SCAN_PARTITION_UPPER_BOUND, 209171L)
        .option(JdbcConnectorOptions.SCAN_PARTITION_NUM, (int)209171L/3)
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

        tableEnv.executeSql(
            "INSERT INTO top_rated_movies " +
            "SELECT m.movie_id, m.title, s.average_rating " +
            "FROM ( " +
                "SELECT movie_id, AVG(rating) OVER (PARTITION BY movie_id) AS average_rating " +
                "FROM ratings " +
                "ORDER BY average_rating DESC " + 
                "LIMIT 20" + 
            ") s, movies m " + 
            "WHERE s.movie_id = m.movie_id"
        );
    }
}
