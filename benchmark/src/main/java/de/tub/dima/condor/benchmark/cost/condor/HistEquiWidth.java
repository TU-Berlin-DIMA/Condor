package de.tub.dima.condor.benchmark.cost.condor;

import de.tub.dima.condor.benchmark.sources.input.UniformDistributionSource;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Histograms.BarSplittingHistogram;
import de.tub.dima.condor.core.synopsis.Histograms.EquiDepthHistogram;
import de.tub.dima.condor.core.synopsis.Histograms.EquiWidthHistogram;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class HistEquiWidth {
    public static void run(int parallelism, int targetThroughput, int iteration) throws Exception {
        String jobName = "Synopses COST test EquiWidth Histogram | parallelism: "+parallelism + " | iteration: "+iteration;
        System.out.println(jobName);

        // Set up the streaming execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();

        // Initialize Uniform DataSource
        if(targetThroughput == -1){
            // This is a parameter indicates the throughput per core that the input stream will try to achieve.
            // However, it varies depending on the Hardware used. For our experiments we
            // didn't saw any performance improvement beyond this value.
            targetThroughput = 200000;
        }
        DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
                .addSource(new UniformDistributionSource(20000, targetThroughput));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new SyntheticTimestampsAndWatermarks());

        // We want to build the synopsis based on the value of field 0
        SingleOutputStreamOperator<Integer> inputStream = timestamped.map(new SyntheticExtractKeyField(0)).returns(Integer.class);

        // Measure and report the throughput
        inputStream.flatMap(new ParallelThroughputLogger<Integer>(1000, jobName));

        // Set up other configuration parameters
        Class<EquiWidthHistogram> synopsisClass = EquiWidthHistogram.class;
        Window[] windows = {new TumblingWindow(WindowMeasure.Time, 15000)};
        Object[] synopsisParameters = new Object[]{0d, 1001d, 100}; // lower bound, upper bound, numBuckets

        BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

        // Build the synopses
        SingleOutputStreamOperator<WindowedSynopsis<EquiWidthHistogram>> synopsesStream = SynopsisBuilder.build(config);

        synopsesStream.addSink(new SinkFunction<WindowedSynopsis<EquiWidthHistogram>>() {
            @Override
            public void invoke(WindowedSynopsis<EquiWidthHistogram> value, Context context) throws Exception {
                System.out.println(value.getSynopsis());
            }
        });

        env.execute(jobName);
    }
}
