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

package Jobs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Calendar;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataSet<Tuple2<Integer, Integer>> line = env.readTextFile("data/10percent.csv")
				.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
					@Override
					public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
						String[] tuples = value.split(",");

						if(tuples.length == 2) {

							Integer key = new Integer(tuples[0]);
							Integer val = new Integer(tuples[1]);

							if (key != null && val != null) {
								out.collect(new Tuple2<>(key, new Integer(1)));
							}
						}else {
							System.out.println("invalid value : " + value);
						}
					}
				});

		Calendar calendar = Calendar.getInstance();
		calendar.set(2018, 1, 1);
		long timestampInMillis = calendar.getTimeInMillis();

		System.out.println("TimeStamp!!!");
		System.out.println(timestampInMillis);

		line.map(new timeStampMapFunction())
                .writeAsCsv("data/timestamped.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


		// execute program
		env.execute("Flink Batch Java API Skeleton");

	}

	public static class timeStampMapFunction extends RichMapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Long>> {

		private ValueState<Integer> countState;

		@Override
		public void open(Configuration parameters) throws Exception {
			countState = new ValueState<Integer>() {
				int value;
				@Override
				public Integer value() throws IOException {
					return value;
				}

				@Override
				public void update(Integer value) throws IOException {
					this.value = value;
				}

				@Override
				public void clear() {
					value = 0;
				}
			};
			countState.update(0);
		}

		@Override
		public Tuple3<Integer, Integer, Long> map(Tuple2<Integer, Integer> value) throws Exception {

			int count = countState.value();
			count ++;
			countState.update(count);

			Long timestamp = 1517499757961L + Math.floorDiv(count, 10000) * 60000L;

			return new Tuple3<>(value.f0, value.f1, timestamp);
		}
	}
}