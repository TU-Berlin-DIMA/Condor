package de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.classification;

import de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils.CountMinSketchCommutative;
import de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils.CountMinSketchMergeable;
import de.tub.dima.condor.benchmark.sources.input.UnorderedSource;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.SyntheticTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.throughputUtils.ParallelThroughputLogger;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class Mergeable {
	public static void run(int parallelism, long runtime, int targetThroughput, String outputDir) throws Exception {
		String jobName = "Condor's Count Min Sketch (Mergeable) - classification test "+parallelism;
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
				.addSource(new UnorderedSource(runtime, targetThroughput));

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new SyntheticTimestampsAndWatermarks());

		// We want to build the synopsis based on the value of field 0
		SingleOutputStreamOperator<Integer> inputStream = timestamped.map(new SyntheticExtractKeyField(0)).returns(Integer.class);

		// Measure and report the throughput
		inputStream.flatMap(new ParallelThroughputLogger<Integer>(1000, jobName));

		// Set up other configuration parameters
		Class<CountMinSketchMergeable> synopsisClass = CountMinSketchMergeable.class;
		Window[] windows = {new SlidingWindow(WindowMeasure.Time, 5000, 5000)};
		Object[] synopsisParameters = new Object[]{65536, 5, 7L};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

		// Build the synopses
		SingleOutputStreamOperator<WindowedSynopsis<CountMinSketchMergeable>> synopsesStream = SynopsisBuilder.build(config);

	/*	synopsesStream.map(new MapFunction<WindowedSynopsis<CountMinSketchMergeable>, Integer>() {
			@Override
			public Integer map(WindowedSynopsis<CountMinSketchMergeable> cms) throws Exception {
				return cms.getSynopsis().getElementsProcessed();
			}
		}).writeAsText(outputDir+ "/mergeable_"+parallelism+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
*/
		synopsesStream.addSink(new SinkFunction() {
			@Override
			public void invoke(final Object value) throws Exception { }
		});

		env.execute(jobName);
	}
}
