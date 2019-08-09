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

import Sketches.CountMinSketch;
import Sketches.CountMinSketchAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



		int width = 50;
		int height = 15;
		int seed = 1;

		CountMinSketchAggregator testAggregator = new CountMinSketchAggregator(height, width, seed);
		CountMinSketch<Tuple2<Integer, Integer>> testSketch = testAggregator.createAccumulator();
		testSketch.update(new Tuple2<>(1,1));
		testSketch.update(new Tuple2<>(1,2));
		testSketch.update(new Tuple2<>(1,1));

		int approximate_count = testSketch.query(new Tuple2<>(1,1));
		System.out.println("approximate count of (1,1): " + approximate_count);



		DataStream<String> line = env.readTextFile("data/10percent.csv");
		DataStream<Tuple2<Integer, Integer>> tuple = line.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out){
                String[] tuples = value.split(",");
                if(tuples.length == 2) {
                    out.collect(new Tuple2<>(Integer.getInteger(tuples[0]), Integer.getInteger(tuples[1])));
                }
            }
        });

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyed = tuple.keyBy(0);
        WindowedStream<Tuple2<Integer, Integer>, Tuple, TimeWindow> win = keyed.timeWindow(Time.minutes(1));
        SingleOutputStreamOperator<CountMinSketch> sketches = win.aggregate(new CountMinSketchAggregator<Tuple2<Integer, Integer>>(height, width, seed));

        sketches.writeAsText("output/CMsketch").setParallelism(1);

		env.execute("Flink Streaming Java API Skeleton");

	}
}
