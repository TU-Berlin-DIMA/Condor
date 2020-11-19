package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.processor.BuildSynopsis;
import de.tub.dima.condor.core.synopsis.Sketches.HyperLogLogSketch;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Created by Rudi Poepsel Lemaitre on 22/10/2020.
 */
public class HLLSketchAccuracy {
	public static void main(String[] args) throws Exception {

		System.out.println("HyperLogLog sketch accuracy test");
		// set up the streaming execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


		// Get the parallelism
		int parallelism = Integer.parseInt(args[0]);

		// Initialize NYCTaxi DataSource
		DataStreamSource<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(-1, 200000,  new ArrayList<>())).setParallelism(1);

		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new NYCTimestampsAndWatermarks());

		// We want to build the hyperloglog sketch based on the value of field 1 (taxiId)
		SingleOutputStreamOperator<Long> inputStream = timestamped.map(new NYCExtractKeyField(1));

		// Set up other configuration parameters
		Class<HyperLogLogSketch> synopsisClass = HyperLogLogSketch.class;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 10000)};
		Object[] synopsisParameters = new Object[]{16, 7L};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

		// Build the synopses
		SingleOutputStreamOperator<WindowedSynopsis<HyperLogLogSketch>> synopsesStream = SynopsisBuilder.build(env, config);

		// Predict the number of distinct taxiID’s
		SingleOutputStreamOperator<Long> result = synopsesStream.flatMap(new countDistinct());

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/hll_result_"+parallelism+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute("HyperLogLog sketch accuracy test");
	}

	private static class countDistinct implements FlatMapFunction<WindowedSynopsis<HyperLogLogSketch>, Long> {

		@Override
		public void flatMap(WindowedSynopsis<HyperLogLogSketch> hllSketch, Collector<Long> out) throws Exception {
			// Predict the number of distinct taxiID’s
			out.collect(hllSketch.getSynopsis().distinctItemsEstimator());
		}
	}


}
