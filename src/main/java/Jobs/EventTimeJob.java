package Jobs;

import Sketches.CountMinSketch;
import Sketches.CountMinSketchAggregator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public class EventTimeJob {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // int parallelism = env.getParallelism();

        int width = 10;
        int height = 5;
        int seed = 1;
        int keyField = 2;

        Time windowTime = Time.minutes(1);


        DataStream<String> line = env.readTextFile("data/timestamped.csv");
        DataStream<Tuple4<Integer, Integer, Integer, Long>> timestamped = line.flatMap(new CreateTuplesFlatMap()) // Create the tuples from the incoming Data
                .map(new AddParallelismRichFlatMapFunction()) // add a variable indicating the partition of the data
                        .assignTimestampsAndWatermarks(new CustomTimeStampExtractor()); // extract the timestamps and add watermarks

        SingleOutputStreamOperator<CountMinSketch> distributedSketches = timestamped.keyBy(0) // key by the partition (should always be on field 1)
                .timeWindow(windowTime) // keyed window by Window Time
                        .aggregate(new CountMinSketchAggregator<>(height, width, seed, keyField)); // aggregate with our Sketches

        SingleOutputStreamOperator<CountMinSketch> finalSketch = distributedSketches.timeWindowAll(windowTime) // global window
                .reduce(new ReduceFunction<CountMinSketch>() { // Merge all sketches in the global window
                    @Override
                    public CountMinSketch reduce(CountMinSketch value1, CountMinSketch value2) throws Exception {
                        return value1.merge(value2);
                    }
                });


        finalSketch.writeAsText("output/eventTimeSketches.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");
    }

    /**
     *  Stateful map function to add the parallelism variable
     */
    public static class AddParallelismRichFlatMapFunction extends RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple4<Integer, Integer, Integer, Long>> {

        private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

        ValueState<Integer> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new ValueState<Integer>() {
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
            state.update(0);
        }

        @Override
        public Tuple4<Integer, Integer, Integer, Long> map(Tuple3<Integer, Integer, Long> value) throws Exception {

            int currentNode = state.value();
            int next = currentNode +1;
            next = next % this.getRuntimeContext().getNumberOfParallelSubtasks();
            state.update(next);

            return new Tuple4<>(currentNode, value.f0, value.f1, value.f2);

        }
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
    public static class CustomTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple4<Integer, Integer, Integer, Long>>{
        /**
         * Asks this implementation if it wants to emit a watermark. This method is called right after
         * the {@link #extractTimestamp(Tuple4, long)}   method.
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
        public Watermark checkAndGetNextWatermark(Tuple4<Integer, Integer, Integer, Long> lastElement, long extractedTimestamp) {
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
        public long extractTimestamp(Tuple4<Integer, Integer, Integer, Long> element, long previousElementTimestamp) {
            return element.f3;
        }
    }
}
