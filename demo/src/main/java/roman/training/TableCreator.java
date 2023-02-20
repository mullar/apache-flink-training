package roman.training;

import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class TableCreator {
    private TableCreator() {};   
    
    public static void createMoviesTable(StreamTableEnvironment tableEnv, String dbServerPort) {
        final TableDescriptor moviesTableDescriptor = TableDescriptor.forConnector("jdbc")
        .schema(Schema.newBuilder()
        .column("movie_id", DataTypes.BIGINT().notNull())
        .column("title", DataTypes.STRING().notNull())
        .column("genres", DataTypes.STRING().notNull())
        .primaryKey("movie_id")
        .build())
        .option(JdbcConnectorOptions.URL, String.format("jdbc:mysql://%s:3306/test_db", dbServerPort))
        .option(JdbcConnectorOptions.DRIVER, "com.mysql.cj.jdbc.Driver")
        .option(JdbcConnectorOptions.TABLE_NAME, "movies")
        .option(JdbcConnectorOptions.USERNAME, "root")
        .option(JdbcConnectorOptions.PASSWORD, "root123")
        .build();
        tableEnv.createTemporaryTable("movies", moviesTableDescriptor);
    }    

    public static void createRatingsTable(StreamTableEnvironment tableEnv, String dbServerPort) {
        final TableDescriptor ratingsTableDescriptor = TableDescriptor.forConnector("jdbc")
        .schema(Schema.newBuilder()
        .column("id", DataTypes.BIGINT().notNull())
        .column("user_id", DataTypes.BIGINT().notNull())
        .column("movie_id", DataTypes.BIGINT().notNull())
        .column("rating", DataTypes.DECIMAL(2, 1).notNull())
        .column("created_ts", DataTypes.TIMESTAMP().notNull())
        .primaryKey("id")
        .build())
        .option(JdbcConnectorOptions.URL, String.format("jdbc:mysql://%s:3306/test_db", dbServerPort))
        .option(JdbcConnectorOptions.DRIVER, "com.mysql.cj.jdbc.Driver")
        .option(JdbcConnectorOptions.TABLE_NAME, "ratings")
        .option(JdbcConnectorOptions.USERNAME, "root")
        .option(JdbcConnectorOptions.PASSWORD, "root123")
        .option(JdbcConnectorOptions.SCAN_PARTITION_COLUMN, "id")
        .option(JdbcConnectorOptions.SCAN_PARTITION_LOWER_BOUND, 1L)
        .option(JdbcConnectorOptions.SCAN_PARTITION_UPPER_BOUND, 25575445L)
        .option(JdbcConnectorOptions.SCAN_PARTITION_NUM, (int)(25575445L / 100000))    
        .build();
        tableEnv.createTemporaryTable("ratings", ratingsTableDescriptor);  
    }

    public static final void createTopRatedMoviesTable(StreamTableEnvironment tableEnv, String dbServerPort) {
        final TableDescriptor topRatedMoviesTableDescriptor = TableDescriptor.forConnector("jdbc")
        .schema(Schema.newBuilder()
        .column("movie_id", DataTypes.BIGINT().notNull())
        .column("title", DataTypes.STRING().notNull())
        .column("average_rating", DataTypes.DECIMAL(2, 1).notNull())
        .primaryKey("movie_id")
        .build())
        .option(JdbcConnectorOptions.URL, String.format("jdbc:mysql://%s:3306/test_db", dbServerPort))
        .option(JdbcConnectorOptions.DRIVER, "com.mysql.cj.jdbc.Driver")
        .option(JdbcConnectorOptions.TABLE_NAME, "top_rated_movies")
        .option(JdbcConnectorOptions.USERNAME, "root")
        .option(JdbcConnectorOptions.PASSWORD, "root123")
        .build();
        tableEnv.createTemporaryTable("top_rated_movies", topRatedMoviesTableDescriptor);
    }        
}
