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

import Sketches.BuildSketch;
import Sketches.CountMinSketch;
import Sketches.CountMinSketchAggregator;
import Sketches.SketchAggregator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        int width = 10;
        int height = 5;
        long seed = 1;


        int keyField = 0;

        Object[] parameters = new Object[]{width,height,seed};
        Class<CountMinSketch> cl = CountMinSketch.class;

        Time windowTime = Time.minutes(1);
        DataStream<String> line = env.readTextFile("data/timestamped.csv");
        DataStream<Tuple3< Integer, Integer, Long>> timestamped = line.flatMap(new CreateTuplesFlatMap()) // Create the tuples from the incoming Data
                .assignTimestampsAndWatermarks(new CustomTimeStampExtractor()); // extract the timestamps and add watermarks

//        CountMinSketchAggregator agg = new CountMinSketchAggregator<>(height, width, seed, keyField);
//        SingleOutputStreamOperator<CountMinSketch> finalSketch = BuildSketch.timeBased(timestamped, windowTime, agg);
//        CountMinSketch cm = new CountMinSketch(width, height, seed);
//        SketchAggregator agg = new SketchAggregator(cm, keyField);
        SingleOutputStreamOperator<CountMinSketch> finalSketch = BuildSketch.timeBased(timestamped, windowTime, cl, parameters, keyField);

        finalSketch.writeAsText("output/eventTimeSketches.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("Flink Streaming Java API Skeleton");

    }

    /**
     * FlatMap to create Tuples from the incoming data
     */
    public static class CreateTuplesFlatMap implements FlatMapFunction<String, Tuple3<Integer, Integer, Long>>{
        @Override
        public void flatMap(String value, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            String[] tuples = value.split(",");

            if(tuples.length == 3) {

                Integer key = new Integer(tuples[0]);
                Integer val = new Integer(tuples[1]);
                Long timestamp = new Long(tuples[2]);

                if (key != null && val != null) {
                    out.collect(new Tuple3<>(key, val, timestamp));
                }
            }
        }
    }

    /**
     * The Custom TimeStampExtractor which is used to assign Timestamps and Watermarks for our data
     */
    public static class CustomTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple3<Integer, Integer, Long>> {
        /**
         * Asks this implementation if it wants to emit a watermark. This method is called right after
         * the {@link #checkAndGetNextWatermark(Tuple3, long)} method.
         *
         * <p>The returned watermark will be emitted only if it is non-null and its timestamp
         * is larger than that of the previously emitted watermark (to preserve the contract of
         * ascending watermarks). If a null value is returned, or the timestamp of the returned
         * watermark is smaller than that of the last emitted one, then no new watermark will
         * be generated.
         *
         * <p>For an example how to use this method, see the documentation of
         * {@link AssignerWithPunctuatedWatermarks this class}.
         *
         * @param lastElement
         * @param extractedTimestamp
         * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
         */
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple3<Integer, Integer, Long> lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
        }

        /**
         * Assigns a timestamp to an element, in milliseconds since the Epoch.
         *
         * <p>The method is passed the previously assigned timestamp of the element.
         * That previous timestamp may have been assigned from a previous assigner,
         * by ingestion time. If the element did not carry a timestamp before, this value is
         * {@code Long.MIN_VALUE}.
         *
         * @param element                  The element that the timestamp will be assigned to.
         * @param previousElementTimestamp The previous internal timestamp of the element,
         *                                 or a negative value, if no timestamp has been assigned yet.
         * @return The new timestamp.
         */
        @Override
        public long extractTimestamp(Tuple3<Integer, Integer, Long> element, long previousElementTimestamp) {
            return element.f2;
        }
    }
}
