package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.flinkScottyConnector.processor.BuildSynopsis;
import de.tub.dima.condor.core.synopsis.Histograms.EquiWidthHistogram;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.ArrayList;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

/**
 * Created by Rudi Poepsel Lemaitre on 22/10/2020.
 */
public class EquiWidthHistogramAccuracy {
    public static void main(String[] args) throws Exception {

        System.out.println("Equi-width histogram accuracy test");
//		double bL = (-73.991119 - (-73.965118)) / (double) 100;
//		System.out.println(bL);

        // set up the streaming execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.setParallelism(Integer.parseInt(args[0]));
		env.setMaxParallelism(Integer.parseInt(args[0]));

//        env.setParallelism(1);
//        env.setMaxParallelism(1);

        Class<EquiWidthHistogram> synopsisClass = EquiWidthHistogram.class;

        DataStream<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> messageStream = env
                .addSource(new NYCTaxiRideSource(-1, 200000, new ArrayList<>())).setParallelism(1);


        final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new NYCTimestampsAndWatermarks());

//        SingleOutputStreamOperator<String> result = timestamped.flatMap(new Test(-73.991119, -73.965118, 100));

		SingleOutputStreamOperator<EquiWidthHistogram> synopsesStream = BuildSynopsis.timeBased(timestamped, Time.milliseconds(10000),6, synopsisClass, new Object[]{-73.991119, -73.965118, 100});

		SingleOutputStreamOperator<Integer> result = synopsesStream.flatMap(new queryBucketCounts());

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/ew-histogram_result_"+Integer.parseInt(args[0])+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

//        result.writeAsText("EDADS/output/indexes.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Equi-width histogram accuracy test");
    }

    private static class queryBucketCounts implements FlatMapFunction<EquiWidthHistogram, Integer> {

        @Override
        public void flatMap(EquiWidthHistogram histogram, Collector<Integer> out) throws Exception {
            //estimate the frequencies of all taxiID's [2013000001, 2013013223]
            int[] counts = histogram.getFrequency();
            for (int i = 0; i < counts.length; i++) {
                out.collect(counts[i]);
            }
        }
    }


}
