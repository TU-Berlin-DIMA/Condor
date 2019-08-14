package Jobs;

import Sketches.CountMinSketch;
import Sketches.CountMinSketchAggregator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.XORShiftRandom;

import javax.annotation.Nullable;
import java.io.IOException;

public class EventTimeStreamingJob {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // int parallelism = env.getParallelism();

        int width = 10;
        int height = 5;
        int seed = 1;

        Time windowTime = Time.minutes(1);


        DataStream<String> line = env.readTextFile("data/timestamped.csv");
        DataStream<Tuple4<Integer, Integer, Long, Integer>> timestamped = line.flatMap(new eventTimeRichFlatMapFunction())
                .assignTimestampsAndWatermarks(new customTimeStampExtractor());

        SingleOutputStreamOperator<CountMinSketch> distributedSketches = timestamped.keyBy(3)
                .timeWindow(windowTime)
                .aggregate(new CountMinSketchAggregator<>(height, width, seed));

        SingleOutputStreamOperator<CountMinSketch> finalSketch = distributedSketches.timeWindowAll(windowTime)
                .reduce(new ReduceFunction<CountMinSketch>() {
                    @Override
                    public CountMinSketch reduce(CountMinSketch value1, CountMinSketch value2) throws Exception {
                        return value1.merge(value2);
                    }
                });

        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class eventTimeRichFlatMapFunction extends RichFlatMapFunction<String, Tuple4<Integer, Integer, Long, Integer>> {

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
        public void flatMap(String value, Collector<Tuple4<Integer, Integer, Long, Integer>> out) throws Exception {
            String[] tuples = value.split(",");

            if(tuples.length == 3) {

                int currentNode = state.value();
                int next = currentNode % this.getRuntimeContext().getNumberOfParallelSubtasks();
                state.update(next);

                Integer key = new Integer(tuples[0]);
                Integer val = new Integer(tuples[1]);
                Long timestamp = new Long(tuples[2]);

                if (key != null && val != null) {
                    out.collect(new Tuple4<>(key, val, timestamp, currentNode));
                }
            }
        }
    }

    public static class customTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple4<Integer, Integer, Long, Integer>>{
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
        public Watermark checkAndGetNextWatermark(Tuple4<Integer, Integer, Long, Integer> lastElement, long extractedTimestamp) {
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
        public long extractTimestamp(Tuple4<Integer, Integer, Long, Integer> element, long previousElementTimestamp) {
            return element.f2;
        }
    }

}
