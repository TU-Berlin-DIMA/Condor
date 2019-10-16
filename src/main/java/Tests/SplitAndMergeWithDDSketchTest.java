package Tests;

import Histograms.BarSplittingHistogram;
import Histograms.EquiDepthHistogram;
import Histograms.SplitAndMergeWithDDSketch;
import Synopsis.BuildSynopsis;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class SplitAndMergeWithDDSketchTest {
    public static void main(String[] args) throws Exception {
/*

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int keyField = 0;

        Integer numBuckets = 32;
        Double sketchAccuracy = 0.05;


        Object[] parameters = new Object[]{numBuckets, sketchAccuracy};
        Class<SplitAndMergeWithDDSketch> sketchClass = SplitAndMergeWithDDSketch.class;

        Time windowTime = Time.minutes(1);

        DataStream<String> line = env.readTextFile("data/timestamped.csv");
        DataStream<Tuple3<Integer, Integer, Long>> timestamped = line.flatMap(new SplitAndMergeWithDDSketchTest.CreateTuplesFlatMap()) // Create the tuples from the incoming Data
                .assignTimestampsAndWatermarks(new BashHistogramTest.CustomTimeStampExtractor()); // extract the timestamps and add watermarks


        SingleOutputStreamOperator<SplitAndMergeWithDDSketch> splitAndMergeWithDDSketchSingleOutputStreamOperator = BuildSynopsis.timeBased(timestamped, windowTime, keyField, sketchClass, parameters);

        splitAndMergeWithDDSketchSingleOutputStreamOperator.writeAsText("output/SplitAndMerge.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataStream<Double> queryResult = splitAndMergeWithDDSketchSingleOutputStreamOperator.map(new MapFunction<SplitAndMergeWithDDSketch, Double>() {
            @Override
            public Double map(SplitAndMergeWithDDSketch sketch) throws Exception {
                return sketch.rangeQuery(2d, 16d);
            }
        });

        queryResult.writeAsText("output/SplitAndMergeRangeQuery.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");*/
    }

    /**
     * FlatMap to create Tuples from the incoming data
     */
   /* public static class CreateTuplesFlatMap implements FlatMapFunction<String, Tuple3<Integer, Integer, Long>> {
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
    }*/

    /**
     * The Custom TimeStampExtractor which is used to assign Timestamps and Watermarks for our data
     */
  /*  public static class CustomTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple3<Integer, Integer, Long>> {
        *//**
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
         *//*
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple3<Integer, Integer, Long> lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
        }

        *//**
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
         *//*
        @Override
        public long extractTimestamp(Tuple3<Integer, Integer, Long> element, long previousElementTimestamp) {
            return element.f2;
        }
    }*/
}
