package de.tub.dima.condor.benchmark.scalability.evaluation;

import de.tub.dima.condor.benchmark.sources.input.IPaddressesSource;
import de.tub.dima.condor.benchmark.sources.queries.IPQuerySource;
import de.tub.dima.condor.benchmark.sources.utils.queries.QueryCountMin;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.ApproximateDataAnalytics;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryResult;
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
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class QueryLatest {
	public static void run(int parallelism, int queryThroughput) throws Exception {
		String jobName = "Query latest - scalability test "+parallelism;
		System.out.println(jobName);

		// Set up the streaming execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Initialize IP Address DataSource - 20 seconds runtime
		DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
				.addSource(new IPaddressesSource(20000, 20000));

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new SyntheticTimestampsAndWatermarks());

		// We want to build the synopsis based on the value of field 0
		SingleOutputStreamOperator<Integer> inputStream = timestamped.map(new SyntheticExtractKeyField(0)).returns(Integer.class);

		// Set up other configuration parameters
		Class<CountMinSketch> synopsisClass = CountMinSketch.class;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 5000)};
		Object[] synopsisParameters = new Object[]{65536, 5, 7L};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

		// Build the synopses
		SingleOutputStreamOperator<WindowedSynopsis<CountMinSketch>> synopsesStream = SynopsisBuilder.build(env, config);

		// Initialize query stream - query stream runs from seconds 40 to 60 (20 seconds after the synopsis stream stopped)
		if (queryThroughput == -1){
			// This is a parameter indicates the throughput per core that the query stream will try to achieve.
			// However, it varies depending on the Hardware used. For our experiments we
			// didn't see any performance improvement beyond this value.
			queryThroughput = 1000000;
		}
		DataStream<Integer> queryStream = env.addSource(new IPQuerySource(Time.seconds(20), queryThroughput, Time.seconds(40)));

		// Measure and report the throughput
		queryStream.flatMap(new ParallelThroughputLogger<Integer>(1000, jobName));

		// Evaluate the synopsis stream based on the query stream
		SingleOutputStreamOperator<QueryResult<Integer, Integer>> resultStream = ApproximateDataAnalytics.queryLatest(synopsesStream, queryStream, new QueryCountMin());

		resultStream.addSink(new SinkFunction() {
			@Override
			public void invoke(final Object value) throws Exception {
				//Environment.out.println(value);
			}
		});

		env.execute(jobName);
	}
}
