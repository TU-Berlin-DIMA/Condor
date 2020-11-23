package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.core.synopsis.Sketches.HyperLogLogSketch;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class HLLSketchAccuracy {
	public static void run(int parallelism, String outputDir) throws Exception {
		String jobName = "HyperLogLog sketch accuracy test "+parallelism;
		System.out.println(jobName);

		// set up the streaming execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().enableObjectReuse();

		// Initialize NYCTaxi DataSource
		DataStreamSource<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(-1, 200000)).setParallelism(1);

		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new NYCTimestampsAndWatermarks());

		// We want to build the hyperloglog sketch based on the value of field 1 (taxiId)
		SingleOutputStreamOperator<Long> inputStream = timestamped.map(new NYCExtractKeyField(1)).returns(Long.class);

		// Set up other configuration parameters
		Class<HyperLogLogSketch> synopsisClass = HyperLogLogSketch.class;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 10000)};
		Object[] synopsisParameters = new Object[]{16, 7L};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

		// Build the synopses
		SingleOutputStreamOperator<WindowedSynopsis<HyperLogLogSketch>> synopsesStream = SynopsisBuilder.build(env, config);

		// Predict the number of distinct taxiID’s
		SingleOutputStreamOperator<Long> result = synopsesStream.flatMap(new countDistinct());

		result.writeAsText(outputDir+"/hll_result_"+parallelism+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute(jobName);
	}

	private static class countDistinct implements FlatMapFunction<WindowedSynopsis<HyperLogLogSketch>, Long> {

		@Override
		public void flatMap(WindowedSynopsis<HyperLogLogSketch> hllSketch, Collector<Long> out) throws Exception {
			// Predict the number of distinct taxiID’s
			out.collect(hllSketch.getSynopsis().distinctItemsEstimator());
		}
	}


}
