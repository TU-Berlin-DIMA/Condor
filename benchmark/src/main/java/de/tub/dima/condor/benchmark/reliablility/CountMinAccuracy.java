package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.flinkScottyConnector.processor.BuildSynopsis;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
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
 * Created by Rudi on 22/10/2020.
 */
public class CountMinAccuracy {
	public static void main(String[] args) throws Exception {

		System.out.println("Count-Min sketch accuracy test");
		// set up the streaming execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.setParallelism(Integer.parseInt(args[0]));
		env.setMaxParallelism(Integer.parseInt(args[0]));

		Class<CountMinSketch> synopsisClass = CountMinSketch.class;

		DataStreamSource<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(-1, 200000,  new ArrayList<>())).setParallelism(1);


		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new NYCTimestampsAndWatermarks());

		SingleOutputStreamOperator<CountMinSketch> synopsesStream = BuildSynopsis.timeBased(timestamped, Time.milliseconds(10000),0, synopsisClass, new Object[]{633, 5, 7L});

		SingleOutputStreamOperator<Integer> result = synopsesStream.flatMap(new queryFrequency());

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/count-min_result_"+Integer.parseInt(args[0])+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute("Count-Min sketch accuracy test");
	}

	private static class queryFrequency implements FlatMapFunction<CountMinSketch, Integer> {

		@Override
		public void flatMap(CountMinSketch cmSketch, Collector<Integer> out) throws Exception {
			//estimate the frequencies of all taxiID's [2013000001, 2013013223]
			for (int i = 2013000001; i <= 2013013223; i++) {
				out.collect(cmSketch.query(i));
			}
		}
	}


}
