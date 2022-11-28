/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package roman.training;

import javax.management.ValueExp;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Double>> ratings = getRatings(env);
		DataSet<Tuple2<Long, String>> movies = getMovies(env);

		DataSet<Tuple2<Long, Double>> averageRating = ratings.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Long,Double>,Tuple2<Long, Double>>() {
			@Override
			public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out) throws Exception {
				Double sum = 0.0;
				long length = 0;
				long movieId = 0;

				for (Tuple2<Long, Double> value : values) {
					movieId = value.f0;
					sum += value.f1;
					++length;
				}

				out.collect(new Tuple2<Long, Double>(movieId, sum / length));				
			}			
		});

		averageRating = averageRating.sortPartition(1, Order.DESCENDING).setParallelism(1);

		DataSet<Tuple2<Long, Double>> top10 = averageRating.first(20);
		
		DataSet<Tuple3<Long, String, Double>> top10WithName = top10.joinWithHuge(movies).where(0).equalTo(0).map(new MapFunction<Tuple2<Tuple2<Long,Double>,Tuple2<Long,String>>,Tuple3<Long,String, Double>>() {
			@Override
			public Tuple3<Long, String, Double> map(Tuple2<Tuple2<Long, Double>, Tuple2<Long, String>> value)
					throws Exception {				
				return new Tuple3<Long, String, Double>(value.f0.f0, value.f1.f1, value.f0.f1);
			}			
		});
		
		top10WithName.print();
	}

	private static DataSet<Tuple2<Long, Double>> getRatings(ExecutionEnvironment env) {
		return env.readCsvFile("/data/movies/ratings.csv")
				.fieldDelimiter(",")
				.ignoreFirstLine()
				.includeFields(false, true, true)
				.types(Long.class, Double.class);
    }

	private static DataSet<Tuple2<Long, String>> getMovies(ExecutionEnvironment env) {
		return env.readCsvFile("/data/movies/movies.csv")
				.fieldDelimiter(",")
				.ignoreFirstLine()
				.includeFields(true, true)
				.types(Long.class, String.class);
    }
}