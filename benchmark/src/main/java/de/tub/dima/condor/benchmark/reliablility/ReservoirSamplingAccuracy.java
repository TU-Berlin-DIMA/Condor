package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.flinkScottyConnector.processor.BuildSynopsis;
import de.tub.dima.condor.core.synopsis.Sampling.ReservoirSampler;
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
public class ReservoirSamplingAccuracy {
	public static void main(String[] args) throws Exception {

		System.out.println("Reservoir sampling accuracy test");
		// set up the streaming execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.setParallelism(Integer.parseInt(args[0]));
		env.setMaxParallelism(Integer.parseInt(args[0]));
//
//		env.setParallelism(1);
//		env.setMaxParallelism(1);

		Class<ReservoirSampler> synopsisClass = ReservoirSampler.class;

		DataStreamSource<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(-1, 200000,  new ArrayList<>())).setParallelism(1);


		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new NYCTimestampsAndWatermarks());

		SingleOutputStreamOperator<ReservoirSampler> synopsesStream = BuildSynopsis.timeBased(timestamped, Time.milliseconds(10000),10, synopsisClass, new Object[]{10000});

		SingleOutputStreamOperator<Double> result = synopsesStream.flatMap(new queryAvgPassengerCount());
//        result.writeAsText("EDADS/output/avgPassengerCount.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/res-sampler_result_"+Integer.parseInt(args[0])+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute("Reservoir sampling accuracy test");
	}

	private static class queryAvgPassengerCount implements FlatMapFunction<ReservoirSampler, Double> {

		@Override
		public void flatMap(ReservoirSampler resSample, Collector<Double> out) throws Exception {
			//estimate the average passenger count
			Object[] sample = resSample.getSample();
			double sum = 0.0;
			for (int i = 0; i < sample.length; i++) {
				sum += (Short) sample[i];
			}
			out.collect(sum/sample.length);
		}
	}


}
