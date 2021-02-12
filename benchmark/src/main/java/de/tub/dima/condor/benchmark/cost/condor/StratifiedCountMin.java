package de.tub.dima.condor.benchmark.cost.condor;

import de.tub.dima.condor.benchmark.sources.input.UniformDistributionSource;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.sources.utils.stratifiers.SyntheticStratifier;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsisWrapper;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.processor.BuildStratifiedSynopsisOld;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class StratifiedCountMin {
	public static void run(int parallelism, int targetThroughput) throws Exception {
		// We set the stratification degree to be the same as the parallelism. However, feel free to change it!
		int stratification = parallelism;

		String jobName = "StratifiedCountMin Synopses COST test  "+parallelism;
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
				.addSource(new UniformDistributionSource(-1, targetThroughput));

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new SyntheticTimestampsAndWatermarks());

		// Measure and report the throughput
		timestamped.flatMap(new ParallelThroughputLogger<Tuple3<Integer, Integer, Long>>(1000, jobName));

		// Set up other configuration parameters
		RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> stratificationKeyExtractor = new SyntheticStratifier(stratification);
		Class<CountMinSketch> synopsisClass = CountMinSketch.class;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 5000)};
		Object[] synopsisParameters = new Object[]{65536, 5, 7L};

		BuildConfiguration config = new BuildConfiguration(timestamped, synopsisClass, windows, synopsisParameters, parallelism, stratificationKeyExtractor);

		// Build the stratified synopses
		SingleOutputStreamOperator<StratifiedSynopsisWrapper<Integer, WindowedSynopsis<CountMinSketch>>> synopsesStream= SynopsisBuilder.buildStratified(config);

		// BuildStratifiedSynopsisOld.timeBasedADA(inputStream, Time.seconds(5), null, stratificationKeyExtractor, synopsisClass, synopsisParameters);


		synopsesStream.addSink(new SinkFunction() {
			@Override
			public void invoke(final Object value) throws Exception {
				//Environment.out.println(value);
			}
		});

		env.execute(jobName);
	}
}
