package de.tub.dima.condor.benchmark.scalability.evaluation;

import de.tub.dima.condor.benchmark.sources.input.IPaddressesSource;
import de.tub.dima.condor.benchmark.sources.queries.IPQuerySourceStratified;
import de.tub.dima.condor.benchmark.sources.utils.queries.QueryCountMin;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.sources.utils.stratifiers.IPStratifier;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsisWrapper;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.ApproximateDataAnalytics;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.StratifiedQueryResult;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
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
public class QueryLatestStratified {
	public static void run(int parallelism, int queryThroughput) throws Exception {
		// We set the stratification degree to be the same as the parallelism. However, feel free to change it!
		int stratification = parallelism;

		String jobName = "Query latest stratified - scalability test "+parallelism;
		System.out.println(jobName);

		// Set up the streaming execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Initialize IP Address DataSource
		DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
				.addSource(new IPaddressesSource(20000, 20000));

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> inputStream = messageStream
				.assignTimestampsAndWatermarks(new SyntheticTimestampsAndWatermarks());

		// Set up other configuration parameters
		IPStratifier stratificationKeyExtractor = new IPStratifier(stratification);
		Class<CountMinSketch> synopsisClass = CountMinSketch.class;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 5000)};
		Object[] synopsisParameters = new Object[]{65536, 5, 7L};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism, stratificationKeyExtractor);

		// Build the stratified synopses
		SingleOutputStreamOperator<StratifiedSynopsisWrapper<Integer, WindowedSynopsis<CountMinSketch>>> synopsesStream = SynopsisBuilder.buildStratified(env, config);

		// Initialize stratified query stream
		if (queryThroughput == -1){
			// This is a parameter indicates the throughput per core that the query stream will try to achieve.
			// However, it varies depending on the Hardware used. For our experiments we
			// didn't saw any performance improvement beyond this value.
			queryThroughput = 1000000;
		}
		DataStream<Tuple2<Integer, Integer>> queryStream = env.addSource(new IPQuerySourceStratified(Time.seconds(20), queryThroughput, Time.seconds(40), stratification));

		// Measure and report the throughput
		queryStream.flatMap(new ParallelThroughputLogger<>(1000, jobName));

		// Evaluate the synopsis stream based on the query stream
		SingleOutputStreamOperator<StratifiedQueryResult<Integer, Integer, Integer>> resultStream = ApproximateDataAnalytics.queryLatestStratified(synopsesStream, queryStream, new QueryCountMin(), Integer.class);

		resultStream.addSink(new SinkFunction() {
			@Override
			public void invoke(final Object value) throws Exception {
				//Environment.out.println(value);
			}
		});

		env.execute(jobName);
	}
}
