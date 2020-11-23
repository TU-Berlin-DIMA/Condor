package de.tub.dima.condor.benchmark.scalability.processing.bucketing;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.input.UniformDistributionSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.sources.utils.SyntecticExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.SyntecticTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Histograms.EquiWidthHistogram;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class EquiWidthHistogramBucketing {
    public static void run(int parallelism, long runtime, int targetThroughput) throws Exception {
        String jobName = "Equi-width histogram - bucketing scalability test "+parallelism;
        System.out.println(jobName);

        // set up the streaming execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Initialize Uniform DataSource
        if(targetThroughput == -1){
            // This is a parameter indicates the throughput per core that the input stream will try to achieve.
            // However, it varies depending on the Hardware used. For our experiments we
            // didn't saw any performance improvement beyond this value.
            targetThroughput = 200000;
        }
        DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
                .addSource(new UniformDistributionSource(runtime, targetThroughput));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new SyntecticTimestampsAndWatermarks());

        // We want to build the synopsis based on the value of field 0
        SingleOutputStreamOperator<Integer> inputStream = timestamped.map(new SyntecticExtractKeyField(0)).returns(Integer.class);

        // Measure and report the throughput
        inputStream.flatMap(new ParallelThroughputLogger<Integer>(1000, jobName));

        // Set up other configuration parameters
        Class<EquiWidthHistogram> synopsisClass = EquiWidthHistogram.class;
        Window[] windows = {new TumblingWindow(WindowMeasure.Time, 5000)};
        Object[] synopsisParameters = new Object[]{0.0, 1001.0, 10};

        BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

        // Build the synopses
        SingleOutputStreamOperator<WindowedSynopsis<EquiWidthHistogram>> synopsesStream = SynopsisBuilder.build(env, config);

        synopsesStream.addSink(new SinkFunction() {
            @Override
            public void invoke(final Object value) throws Exception {
                //Environment.out.println(value);
            }
        });

        env.execute(jobName);
    }
}
