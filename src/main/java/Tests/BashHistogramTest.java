package Tests;

import Histograms.BarSplittingHistogram;
import Histograms.SplitAndMergeWithDDSketch;
import Histograms.EquiDepthHistogram;
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

public class BashHistogramTest {
    public static void main(String[] args) throws Exception {


        // set up the streaming execution environment

<<<<<<< HEAD
=======
        int keyField = 0;

        Integer numFinalBuckets = 27;
        Integer precision = 5;

        Object[] parameters = new Object[]{precision, numFinalBuckets};
        Class<BarSplittingHistogram> sketchClass = BarSplittingHistogram.class;

        Time windowTime = Time.minutes(1);

        DataStream<String> line = env.readTextFile("data/timestamped.csv");
        DataStream<Tuple3<Integer, Integer, Long>> timestamped = line.flatMap(new CreateTuplesFlatMap()) // Create the tuples from the incoming Data
                .assignTimestampsAndWatermarks(new CustomTimeStampExtractor()); // extract the timestamps and add watermarks


        SingleOutputStreamOperator<BarSplittingHistogram> bash = BuildSynopsis.timeBased(timestamped, windowTime, keyField, sketchClass, parameters);

        bash.writeAsText("output/BASHHistogram.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        SingleOutputStreamOperator<EquiDepthHistogram> equiDepthHistograms = bash.map(b -> b.buildEquiDepthHistogram());

        equiDepthHistograms.writeAsText("output/EquiDepth.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataStream<Double> queryResult = equiDepthHistograms.map(new MapFunction<EquiDepthHistogram, Double>() {
            @Override
            public Double map(EquiDepthHistogram hist) throws Exception {
                return hist.rangeQuery(2d, 16d);
            }
        });

        queryResult.writeAsText("output/EquiDepthtHistogramQueryOutput.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");
>>>>>>> 0b239cdf8adab3af6cc773724e77c5fb73dab7fe
    }

}
