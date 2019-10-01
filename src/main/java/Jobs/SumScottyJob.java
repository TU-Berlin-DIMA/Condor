package Jobs;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import de.tub.dima.scotty.flinkconnector.demo.windowFunctions.SumWindowFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class SumScottyJob {
    public static void main(String[] args) throws Exception {


        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> line = env.readTextFile("data/timestamped.csv");
        DataStream<Tuple2<Integer, Integer>> timestamped = line.flatMap(new CreateTuplesFlatMap()) // Create the tuples from the incoming Data
                .assignTimestampsAndWatermarks(new CustomTimeStampExtractor()).map(a -> new Tuple2<Integer, Integer>(a.f0,a.f1)); // extract the timestamps and add watermarks

        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> windowOperator =
                new KeyedScottyWindowOperator<>(new SumWindowFunction());

// Add multiple windows to the same operator
        windowOperator.addWindow(new TumblingWindow(WindowMeasure.Time, 1000));
//        windowOperator.addWindow(new SlidingWindow(WindowMeasure.Time, 1000, 5000));
//        windowOperator.addWindow(new SessionWindow(WindowMeasure.Time,1000));

// Add count based window
//        windowOperator.addWindow(new TumblingWindow(WindowMeasure.Count, 1000));

// Add operator to Flink job
        SingleOutputStreamOperator<AggregateWindow<Tuple2<Integer, Integer>>> finalSketch = timestamped.keyBy(0)
                .process(windowOperator);


        finalSketch.writeAsText("output/scottyTest.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");
    }



    /**
     * FlatMap to create Tuples from the incoming data
     */
    static class CreateTuplesFlatMap implements FlatMapFunction<String, Tuple3<Integer, Integer, Long>> {
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
         * the {@link #extractTimestamp(Tuple3, long)}   method.
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
