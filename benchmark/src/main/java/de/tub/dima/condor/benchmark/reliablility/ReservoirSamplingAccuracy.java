package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCExtractKeyField;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.core.synopsis.Sampling.ReservoirSampler;
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
public class ReservoirSamplingAccuracy {
	public static void main(String[] args) throws Exception {

		System.out.println("Reservoir sampling accuracy test");
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

		// We want to build the reservoir sample based on the value of field 10 (passengerCnt)
		SingleOutputStreamOperator<Short> inputStream = timestamped.map(new NYCExtractKeyField(10));

		// Set up other configuration parameters
		Class<ReservoirSampler> synopsisClass = ReservoirSampler.class;
		Window[] windows = {new TumblingWindow(WindowMeasure.Time, 10000)};
		Object[] synopsisParameters = new Object[]{10000};

		BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

		// Build the synopses
		SingleOutputStreamOperator<WindowedSynopsis<ReservoirSampler>> synopsesStream = SynopsisBuilder.build(env, config);

		// Compute the average passenger count
		SingleOutputStreamOperator<Double> result = synopsesStream.flatMap(new queryAvgPassengerCount());

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/res-sampler_result_"+Integer.parseInt(args[0])+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute("Reservoir sampling accuracy test");
	}

	private static class queryAvgPassengerCount implements FlatMapFunction<WindowedSynopsis<ReservoirSampler>, Double> {

		@Override
		public void flatMap(WindowedSynopsis<ReservoirSampler> resSample, Collector<Double> out) throws Exception {
			//estimate the average passenger count
			Object[] sample = resSample.getSynopsis().getSample();
			double sum = 0.0;
			for (int i = 0; i < sample.length; i++) {
				sum += (Short) sample[i];
			}
			out.collect(sum/sample.length);
		}
	}


}
