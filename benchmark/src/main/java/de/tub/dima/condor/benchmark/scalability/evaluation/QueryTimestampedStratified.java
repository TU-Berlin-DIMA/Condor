package de.tub.dima.condor.benchmark.scalability.evaluation;

import de.tub.dima.condor.benchmark.sources.input.IPaddressesSource;
import de.tub.dima.condor.benchmark.sources.queries.IPQuerySourceStratified;
import de.tub.dima.condor.benchmark.sources.queries.IPQuerySourceTimestampedStratified;
import de.tub.dima.condor.benchmark.sources.utils.*;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsisWrapper;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.ApproximateDataAnalytics;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.StratifiedQueryResult;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.TimestampedQuery;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class QueryTimestampedStratified {
	public static void run(int parallelism, long runtime, int queryThroughput) throws Exception {
		// We set the stratification degree to be the same as the parallelism. However, feel free to change it!
		int stratification = parallelism;

		String jobName = "Query timestamped stratified - scalability test "+parallelism;
		System.out.println(jobName);

		// Set up the streaming execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Initialize IP Address DataSource
		DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
				.addSource(new IPaddressesSource(runtime, 20000));

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new SyntecticTimestampsAndWatermarks());

		// We want to build the synopsis based on the value of field 0
		SingleOutputStreamOperator<Integer> inputStream = timestamped.map(new SyntecticExtractKeyField(0)).returns(Integer.class);

		// Set up other configuration parameters
		UniformStratifier stratificationKeyExtractor = new UniformStratifier(stratification);
		Class<CountMinSketch> synopsisClass = CountMinSketch.class;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 5000)};
		Object[] synopsisParameters = new Object[]{65536, 5, 7L};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism, stratificationKeyExtractor);

		// Build the stratified synopses
		SingleOutputStreamOperator<StratifiedSynopsisWrapper<Integer, WindowedSynopsis<CountMinSketch>>> synopsesStream = SynopsisBuilder.buildStratified(env, config);

		// Initialize timestamped and stratified query stream
		if (queryThroughput == -1){
			// This is a parameter indicates the throughput per core that the query stream will try to achieve.
			// However, it varies depending on the Hardware used. For our experiments we
			// didn't saw any performance improvement beyond this value.
			queryThroughput = 250000;
		}
		DataStream<Tuple2<Integer, TimestampedQuery<Integer>>> queryStream = env.addSource(new IPQuerySourceTimestampedStratified(Time.seconds(20), queryThroughput, Time.seconds(40), Time.seconds(20), stratification));

		// Measure and report the throughput
		queryStream.flatMap(new ParallelThroughputLogger<>(1000, jobName));

		// The evaluate operator will maintain the latest maxSynopsisCount synopses in the broadcasted state.
		// We used this value for our experiments. However feel free to change it.
		int maxSynopsisCount = 120;

		// Evaluate the synopsis stream based on the query stream
		SingleOutputStreamOperator<StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer>> resultStream = ApproximateDataAnalytics.queryTimestampedStratified(synopsesStream, queryStream, new QueryCountMin(), Integer.class, maxSynopsisCount);

		resultStream.addSink(new SinkFunction() {
			@Override
			public void invoke(final Object value) throws Exception {
				//Environment.out.println(value);
			}
		});

		env.execute(jobName);
	}
}
