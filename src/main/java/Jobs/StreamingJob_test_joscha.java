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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.XORShiftRandom;


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
public class StreamingJob_test_joscha {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        int parallelism = 8;

        int width = 10;
        int height = 5;
        int seed = 1;


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

        DataStream<Tuple3<Integer, Integer, Integer>> streamWithKey  = tuple.map(new MapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Tuple3<Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {

                int random = new XORShiftRandom().nextInt(parallelism);

                return new Tuple3<>(value.f0, value.f1, random);
            }
        });

        KeyedStream<Tuple3<Integer, Integer, Integer>, Tuple> keyedStream = streamWithKey.keyBy(2);
        SingleOutputStreamOperator<CountMinSketch> windowedSketches = keyedStream.timeWindow(Time.seconds(1))
                .aggregate(new CountMinSketchAggregator<>(height, width, seed));

        SingleOutputStreamOperator<CountMinSketch> merged = windowedSketches.timeWindowAll(Time.seconds(1))
                .reduce(new ReduceFunction<CountMinSketch>() {
                    @Override
                    public CountMinSketch reduce(CountMinSketch value1, CountMinSketch value2) throws Exception {
                        return value1.merge(value2);
                    }
                });


        merged.writeAsText("output/testOutput.txt", FileSystem.WriteMode.OVERWRITE);


        env.execute("Flink Streaming Java API Skeleton");
    }
}

