package roman.training;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.jackson.JacksonMapperFactory;

public class DataSetLookup {
    public static DataSet<Rating> getRatings(ExecutionEnvironment env) {
		return env.readCsvFile("/data/movies/ratings.csv")
				.fieldDelimiter(",")
				.ignoreFirstLine()
				.includeFields(true, true, true, true)
				.tupleType(Rating.class);
    }

    public static DataStreamSource<Rating> getRatings(StreamExecutionEnvironment env) {
        CsvReaderFormat<Rating> csvFormat = forPojo(Rating.class);
        FileSource<Rating> source = FileSource.forRecordStreamFormat(csvFormat, new Path("/data/movies/ratings.csv")).build();
        DataStreamSource<Rating> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "RatingsFile");
        return stream;
    }

	public static DataSet<Movie> getMovies(ExecutionEnvironment env) {
		return env.readCsvFile("/data/movies/movies.csv")
				.fieldDelimiter(",")
				.ignoreFirstLine()
				.includeFields(true, true, true)
				.tupleType(Movie.class);
    }

    public static DataStreamSource<Movie> getMovies(StreamExecutionEnvironment env) {
        CsvReaderFormat<Movie> csvFormat = forPojo(Movie.class);
        FileSource<Movie> source = FileSource.forRecordStreamFormat(csvFormat, new Path("/data/movies/movies.csv")).build();
        DataStreamSource<Movie> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "MoviesFile");
        return stream;
    }

    private static <T> CsvReaderFormat<T> forPojo(Class<T> pojoType) {
        return CsvReaderFormat.forSchema(
                () -> JacksonMapperFactory.createCsvMapper(),
                mapper -> mapper.schemaFor(pojoType).withoutQuoteChar().withHeader().withColumnSeparator(','),
                TypeInformation.of(pojoType));
    }
}