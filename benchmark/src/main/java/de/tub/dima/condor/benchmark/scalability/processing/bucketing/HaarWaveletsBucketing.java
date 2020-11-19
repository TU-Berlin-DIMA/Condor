package de.tub.dima.condor.benchmark.scalability.processing.bucketing;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.input.UniformDistributionSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.benchmark.sources.utils.SyntecticExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.SyntecticTimestampsAndWatermarks;
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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Rudi Poepsel Lemaitre.
 */
public class HaarWaveletsBucketing {
	public static void run(int parallelism, long runtime) throws Exception {

		System.out.println("Haar Wavelets - bucketing scalability test "+parallelism);
		// set up the streaming execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().enableObjectReuse();

		// Initialize Uniform DataSource
		DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
				.addSource(new UniformDistributionSource(runtime, 200000));

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new SyntecticTimestampsAndWatermarks());

		// We want to build the synopsis based on the value of field 0
		SingleOutputStreamOperator<Integer> inputStream = timestamped.map(new SyntecticExtractKeyField(0)).returns(Integer.class);

		// Set up other configuration parameters
		Class<WaveletSynopsis> synopsisClass = WaveletSynopsis.class;
		Class<DistributedWaveletsManager> managerClass = DistributedWaveletsManager.class;
		int miniBatchSize = parallelism * 10;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 10000)};
		Object[] synopsisParameters = new Object[]{1000};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism, miniBatchSize, null, managerClass);

		// Build the synopses
		SingleOutputStreamOperator<WindowedSynopsis<DistributedWaveletsManager>> synopsesStream = SynopsisBuilder.build(env, config);

		//  Compute the range sums of the passengers counts for every 10,000 entries
		SingleOutputStreamOperator<Double> result = synopsesStream.flatMap(new rangeSumPassengerCount());

		result.writeAsText(outputDir+"/haar-wavelets_result_"+parallelism+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute("Haar Wavelets - bucketing scalability test "+parallelism);
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
