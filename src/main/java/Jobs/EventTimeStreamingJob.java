package Jobs;

import Sketches.CountMinSketch;
import Sketches.CountMinSketchAggregator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

public class EventTimeStreamingJob {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        int parallelism = 8;

        int width = 10;
        int height = 5;
        int seed = 1;


        DataStream<String> line = env.readTextFile("data/timestamped.csv");
        DataStream<Tuple3<Integer, Integer, Long>> timestamped = line.flatMap(new FlatMapFunction<String, Tuple3<Integer, Integer, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<Integer, Integer, Long>> out){
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
        }).assignTimestampsAndWatermarks(new customTimeStampExtractor());


        /**
         * test with count per window
         */
        SingleOutputStreamOperator<Integer> aggregate = timestamped.timeWindowAll(Time.minutes(1))
                .aggregate(new AggregateFunction<Tuple3<Integer, Integer, Long>, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Tuple3<Integer, Integer, Long> value, Integer accumulator) {
                        accumulator++;
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
                });


        aggregate.writeAsText("output/test.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);





                /*.map(new MapFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>() {
            @Override
            public Tuple3<Integer, Integer, Long> map(Tuple3<Integer, Integer, Long> value) throws Exception {
                long random = new XORShiftRandom().nextInt(parallelism);

                return new Tuple3<Integer, Integer, Long>(value.f0, value.f1, random);
            }
        });

        timestamped.keyBy(2)*/




        /*
        SingleOutputStreamOperator<CountMinSketch> windowedSketches = keyedStream.timeWindow(Time.seconds(1))
                .aggregate(new CountMinSketchAggregator<>(height, width, seed));

        SingleOutputStreamOperator<CountMinSketch> merged = windowedSketches.timeWindowAll(Time.seconds(1))
                .reduce(new ReduceFunction<CountMinSketch>() {
                    @Override
                    public CountMinSketch reduce(CountMinSketch value1, CountMinSketch value2) throws Exception {
                        return value1.merge(value2);
                    }
                });


         */


        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class customTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple3<Integer, Integer, Long>>{
        /**
         * Asks this implementation if it wants to emit a watermark. This method is called right after
         * the {@link #extractTimestamp(Tuple3, long)}  method.
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
