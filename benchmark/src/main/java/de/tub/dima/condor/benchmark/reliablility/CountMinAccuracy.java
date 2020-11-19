package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
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
 * Created by Rudi Poepsel Lemaitre on 22/10/2020.
 */
public class CountMinAccuracy {
	public static void run(int parallelism) throws Exception {

		System.out.println("Count-Min sketch accuracy test");

		// Set up the streaming execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Initialize NYCTaxi DataSource
		DataStreamSource<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(-1, 200000,  new ArrayList<>())).setParallelism(1);

		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new NYCTimestampsAndWatermarks());

		// We want to build the count-min sketch based on the value of field 1 (taxiId)
		SingleOutputStreamOperator<Long> inputStream = timestamped.map(new NYCExtractKeyField(1)).returns(Long.class);

		// Set up other configuration parameters
		Class<CountMinSketch> synopsisClass = CountMinSketch.class;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 10000)};
		Object[] synopsisParameters = new Object[]{633, 5, 7L};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

		// Build the synopses
		SingleOutputStreamOperator<WindowedSynopsis<CountMinSketch>> synopsesStream = SynopsisBuilder.build(env, config);

		// Query the estimated number of entries in the dataset of each taxiID between [2013000001, 2013013223]
		SingleOutputStreamOperator<Integer> result = synopsesStream.flatMap(new queryFrequency());

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/count-min_result_"+parallelism+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute("Count-Min sketch accuracy test");
	}

	private static class queryFrequency implements FlatMapFunction<WindowedSynopsis<CountMinSketch>, Integer> {

		@Override
		public void flatMap(WindowedSynopsis<CountMinSketch> cmSketch, Collector<Integer> out) throws Exception {
			// Estimate the frequencies of all taxiID's [2013000001, 2013013223]
			for (int i = 2013000001; i <= 2013013223; i++) {
				out.collect(cmSketch.getSynopsis().query(i));
			}
		}
	}


}
