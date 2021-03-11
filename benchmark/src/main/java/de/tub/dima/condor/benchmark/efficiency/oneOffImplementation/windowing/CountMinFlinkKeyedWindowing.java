package de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.windowing;

import de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils.AddParallelismIndex;
import de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils.CountMinAggregator;
import de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils.MergeSynopsis;
import de.tub.dima.condor.benchmark.sources.input.UniformDistributionSource;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class CountMinFlinkKeyedWindowing {
	public static void run(int parallelism, int targetThroughput, int nConcurrentWindows) throws Exception {
		long runtime = 40000;
		String jobName = "One-Off Count Min sketch implementation in Flink - windowing test | parallelism: "+parallelism + " | nConcurrentWindows: " + nConcurrentWindows;
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
				.addSource(new UniformDistributionSource(runtime, targetThroughput));

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new SyntheticTimestampsAndWatermarks());

		// We want to build the synopsis based on the value of field 0
		SingleOutputStreamOperator<Integer> inputStream = timestamped.map(new SyntheticExtractKeyField(0)).returns(Integer.class);

		// Measure and report the throughput
		inputStream.flatMap(new ParallelThroughputLogger<Integer>(1000, jobName));

		KeyedStream keyedStream = inputStream.map(new AddParallelismIndex()).setParallelism(parallelism)
				.keyBy(0);

		long windowSize = 20000;
		long windowSlide = 20000/nConcurrentWindows;

		SingleOutputStreamOperator<CountMinSketch> windows = keyedStream.timeWindow(Time.milliseconds(windowSize), Time.milliseconds(windowSlide))
				.aggregate(new CountMinAggregator(65536, 5, 7L));

		AllWindowedStream<CountMinSketch, TimeWindow> preAggregated = windows.timeWindowAll(Time.milliseconds(windowSize), Time.milliseconds(windowSlide));
		SingleOutputStreamOperator<CountMinSketch> synopsesStream = preAggregated.reduce(new MergeSynopsis());

		synopsesStream.addSink(new SinkFunction() {
			@Override
			public void invoke(final Object value) throws Exception {
				//Environment.out.println(value);
			}
		});

		env.execute(jobName);
	}
}
