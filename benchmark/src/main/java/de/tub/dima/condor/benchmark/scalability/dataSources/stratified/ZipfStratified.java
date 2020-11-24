package de.tub.dima.condor.benchmark.scalability.dataSources.stratified;

import de.tub.dima.condor.benchmark.sources.input.ZipfDistributionSource;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.stratifiers.SyntheticStratifier;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsisWrapper;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class ZipfStratified {
	public static void run(int parallelism, long runtime, int targetThroughput) throws Exception {
		// We set the stratification degree to be the same as the parallelism. However, feel free to change it!
		int stratification = parallelism;

		String jobName = "Zipf dataset - global synopses scalability test "+parallelism;
		System.out.println(jobName);

		// Set up the streaming execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Initialize Uniform DataSource
		if(targetThroughput == -1){
			// This is a parameter indicates the throughput per core that the input stream will try to achieve.
			// However, it varies depending on the Hardware used. For our experiments we
			// didn't saw any performance improvement beyond this value.
			targetThroughput = 200000;
		}
		DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
				.addSource(new ZipfDistributionSource(runtime, targetThroughput));

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> inputStream = messageStream
				.assignTimestampsAndWatermarks(new SyntheticTimestampsAndWatermarks());

		// Measure and report the throughput
		inputStream.flatMap(new ParallelThroughputLogger<>(1000, jobName));

		// Set up other configuration parameters
		SyntheticStratifier stratificationKeyExtractor = new SyntheticStratifier(stratification);
		Class<CountMinSketch> synopsisClass = CountMinSketch.class;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 5000)};
		Object[] synopsisParameters = new Object[]{65536, 5, 7L};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism, stratificationKeyExtractor);

		// Build the stratified synopses
		SingleOutputStreamOperator<StratifiedSynopsisWrapper<Integer,WindowedSynopsis<CountMinSketch>>> synopsesStream = SynopsisBuilder.buildStratified(config);

		synopsesStream.addSink(new SinkFunction() {
			@Override
			public void invoke(final Object value) throws Exception {
				//Environment.out.println(value);
			}
		});

		env.execute(jobName);
	}
}
