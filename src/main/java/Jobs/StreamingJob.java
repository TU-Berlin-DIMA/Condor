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
import Sketches.CountMinSketchAggregator2;
import Sketches.CountMinSketchProcess;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        int width = 10;
        int height = 5;
        int seed = 1;

		/*CountMinSketchAggregator testAggregator = new CountMinSketchAggregator(height, width, seed);
		CountMinSketch<Tuple2<Integer, Integer>> testSketch = testAggregator.createAccumulator();
		testSketch.update(new Tuple2<>(1,1));
		testSketch.update(new Tuple2<>(1,2));
		testSketch.update(new Tuple2<>(1,1));

		int approximate_count = testSketch.query(new Tuple2<>(1,1));
		System.out.println("approximate count of (1,1): " + approximate_count);*/


        //DataStream<String> line = env.readTextFile("data/self.csv");
        DataStream<String> line = env.readTextFile("data/10percent.csv");
        DataStream<Tuple2<Integer, Integer>> tuple = line.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out){
                String[] tuples = value.split(",");

                if(tuples.length == 2) {

                    Integer key = new Integer(tuples[0]);
                    Integer val = new Integer(tuples[1]);

                    if (key != null && val != null) {
                        out.collect(new Tuple2<>(key, new Integer(1)));
                    }
                }
            }
        });


        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyed = tuple.keyBy(0);
        //keyed.writeAsText("output/data.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);



        //SingleOutputStreamOperator<Tuple2<Integer, Integer>> testOutput = keyed.

        //WindowedStream<Tuple2<Integer, Integer>, Tuple, GlobalWindow> win = keyed.countWindow(1000000);
        //AllWindowedStream<Tuple2<Integer, Integer>, TimeWindow> win = tuple.timeWindowAll(Time.seconds(2));
        //AllWindowedStream<Tuple2<Integer, Integer>, GlobalWindow> win = tuple.countWindowAll(10000000);
        WindowedStream<Tuple2<Integer, Integer>, Tuple, TimeWindow> win = keyed.timeWindow(Time.seconds(2));




        /*SingleOutputStreamOperator<Integer> testOutput = win.aggregate(new AggregateFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }
            @Override
            public Integer add(Tuple2<Integer, Integer> value, Integer accumulator) {
                accumulator += 1;
                return accumulator;
            }
            @Override
            public Integer getResult(Integer accumulator) {
                return accumulator;
            }
            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        });*/
//        SingleOutputStreamOperator<CountMinSketch> testOutput = win.aggregate(new CountMinSketchAggregator<>(height,width,seed));

        SingleOutputStreamOperator<Tuple2<Long, CountMinSketch>> testOutput = win.aggregate(new CountMinSketchAggregator(height, width, seed), new CountMinSketchProcess());
//        SingleOutputStreamOperator<Tuple2<Long, CountMinSketch>> realOutput = testOutput.keyBy(0).reduce(new ReduceFunction<Tuple2<Long, CountMinSketch>>() {
//            @Override
//            public Tuple2<Long, CountMinSketch> reduce(Tuple2<Long, CountMinSketch> value1, Tuple2<Long, CountMinSketch> value2) throws Exception {
//                try {
//                    return new Tuple2<>(value1.f0, value1.f1.merge(value2.f1));
//                } catch (Exception e) {
//                    return null;
//                }
//            }
//        });
        SingleOutputStreamOperator<Tuple2<Long, CountMinSketch>> realOutput = testOutput.timeWindowAll(Time.seconds(1)).reduce(new ReduceFunction<Tuple2<Long, CountMinSketch>>() {
            @Override
            public Tuple2<Long, CountMinSketch> reduce(Tuple2<Long, CountMinSketch> value1, Tuple2<Long, CountMinSketch> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1.merge(value2.f1));
            }
        });

        testOutput.writeAsText("output/testOutput.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        realOutput.writeAsText("output/realOutput.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        env.execute("Flink Streaming Java API Skeleton");

    }
}
