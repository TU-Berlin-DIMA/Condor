package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.processor.BuildSynopsis;
import de.tub.dima.condor.core.synopsis.Histograms.EquiWidthHistogram;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
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
        // set up the streaming execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Get the parallelism
        int parallelism = Integer.parseInt(args[0]);

        // Initialize NYCTaxi DataSource
        DataStream<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> messageStream = env
                .addSource(new NYCTaxiRideSource(-1, 200000, new ArrayList<>())).setParallelism(1);

        final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new NYCTimestampsAndWatermarks());

        // We want to build the equi-width histogram based on the value of field 6 (startLon)
        SingleOutputStreamOperator<Double> inputStream = timestamped.map(new NYCExtractKeyField(6));

        // Set up other configuration parameters
        Class<EquiWidthHistogram> synopsisClass = EquiWidthHistogram.class;
        Window[] windows = {new TumblingWindow(WindowMeasure.Time, 10000)};
        Object[] synopsisParameters = new Object[]{-73.991119, -73.965118, 100};

        BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

        // Build the synopses
        SingleOutputStreamOperator<WindowedSynopsis<EquiWidthHistogram>> synopsesStream = SynopsisBuilder.build(env, config);

        //  Get the number of taxi rides with start longitude between [−73.991119, −73.965118]
		SingleOutputStreamOperator<Integer> result = synopsesStream.flatMap(new queryBucketCounts());

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/ew-histogram_result_"+parallelism+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Equi-width histogram accuracy test");
    }

    private static class queryBucketCounts implements FlatMapFunction<WindowedSynopsis<EquiWidthHistogram>, Integer> {

        @Override
        public void flatMap(WindowedSynopsis<EquiWidthHistogram> histogram, Collector<Integer> out) throws Exception {
            // Get the number of taxi rides with start longitude between [−73.991119, −73.965118]
            int[] counts = histogram.getSynopsis().getFrequency();
            for (int i = 0; i < counts.length; i++) {
                out.collect(counts[i]);
            }
        }
    }


}
