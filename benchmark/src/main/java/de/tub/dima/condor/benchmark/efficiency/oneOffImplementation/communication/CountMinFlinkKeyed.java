package de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.communication;

import de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils.AddParallelismIndex;
import de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils.CountMinAggregator;
import de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils.MergeSynopsis;
import de.tub.dima.condor.benchmark.sources.input.UniformDistributionSource;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class CountMinFlinkKeyed {
	public static void run(int parallelism, int sourceParallelism, long runtime, int targetThroughput) throws Exception {
		String jobName = "One-Off Count Min sketch implementation in Flink - communication test "+sourceParallelism;
		System.out.println(jobName);

		// Set up the streaming execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().enableObjectReuse();

		// Initialize Uniform DataSource
		if(targetThroughput == -1){
			// This is a parameter indicates the throughput per core that the input stream will try to achieve.
			// However, it varies depending on the Hardware used. For our experiments we
			// didn't saw any performance improvement beyond this value.
			targetThroughput = 200000;
		}
		DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
				.addSource(new UniformDistributionSource(runtime, targetThroughput)).setParallelism(sourceParallelism);

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new SyntheticTimestampsAndWatermarks());

		// We want to build the synopsis based on the value of field 0
		SingleOutputStreamOperator<Integer> inputStream = timestamped.map(new SyntheticExtractKeyField(0)).returns(Integer.class);

		// Measure and report the throughput
		inputStream.flatMap(new ParallelThroughputLogger<Integer>(1000, jobName));

		KeyedStream keyedStream = inputStream.map(new AddParallelismIndex()).setParallelism(parallelism)
				.keyBy(0);

		SingleOutputStreamOperator<CountMinSketch> windows = keyedStream.timeWindow(Time.milliseconds(5000))
				.aggregate(new CountMinAggregator(65536, 5, 7L));

		AllWindowedStream<CountMinSketch, TimeWindow> preAggregated = windows.timeWindowAll(Time.milliseconds(5000));
		SingleOutputStreamOperator<CountMinSketch> synopsesStream = preAggregated.reduce(new MergeSynopsis());

		synopsesStream.addSink(new SinkFunction() {
			@Override
			public void invoke(final Object value) throws Exception {
				//Environment.out.println(value);
			}
		}).setParallelism(1);

		env.execute(jobName);
	}
}
