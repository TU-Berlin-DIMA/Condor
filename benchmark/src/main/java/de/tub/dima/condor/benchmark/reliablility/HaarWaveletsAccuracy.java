package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.core.synopsis.Wavelets.DistributedWaveletsManager;
import de.tub.dima.condor.core.synopsis.Wavelets.WaveletSynopsis;
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
public class HaarWaveletsAccuracy {
	public static void run(int parallelism, String outputDir) throws Exception {
		String jobName = "Haar Wavelets accuracy test "+parallelism;
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

		// We want to build the Haar wavelets based on the value of field 10 (passengerCnt)
		SingleOutputStreamOperator<Short> inputStream = timestamped.map(new NYCExtractKeyField(10)).returns(Short.class);

		// Set up other configuration parameters
		Class<WaveletSynopsis> synopsisClass = WaveletSynopsis.class;
		Class<DistributedWaveletsManager> managerClass = DistributedWaveletsManager.class;
		int miniBatchSize = parallelism * 10;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 10000)};
		Object[] synopsisParameters = new Object[]{1000};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism, miniBatchSize, null, managerClass);

		// Build the synopses
		SingleOutputStreamOperator<WindowedSynopsis<DistributedWaveletsManager>> synopsesStream = SynopsisBuilder.build(config);

		//  Compute the range sums of the passengers counts for every 10,000 entries
		SingleOutputStreamOperator<Double> result = synopsesStream.flatMap(new rangeSumPassengerCount());

		result.writeAsText(outputDir+"/haar-wavelets_result_"+parallelism+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute(jobName);
	}

	private static class rangeSumPassengerCount implements FlatMapFunction<WindowedSynopsis<DistributedWaveletsManager>, Double> {

		@Override
		public void flatMap(WindowedSynopsis<DistributedWaveletsManager> waveletsManager, Collector<Double> out) throws Exception {
			// Estimate the range sums of the passengers counts
			int rangeSize = 10000;
			for (int i = 0; i < 2999998; i+=rangeSize) {
				try {
					out.collect(waveletsManager.getSynopsis().rangeSumQuery(i,i+rangeSize-1));
				} catch (IllegalArgumentException e){
					System.out.println("[ "+i+", "+(i+rangeSize-1)+" ]");
				}
			}
		}
	}


}
