package roman.training;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        tableEnv.getConfig().set("table.exec.resource.default-parallelism", "10");

        TableCreator.createMoviesTable(tableEnv, MY_SQL_IP);
        TableCreator.createRatingsTable(tableEnv, MY_SQL_IP);
        TableCreator.createTopRatedMoviesTable(tableEnv, MY_SQL_IP);

        tableEnv.executeSql(
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
        );
    }
}
