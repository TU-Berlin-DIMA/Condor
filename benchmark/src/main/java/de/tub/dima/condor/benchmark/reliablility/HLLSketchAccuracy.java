package de.tub.dima.condor.benchmark.reliablility;

import de.tub.dima.condor.benchmark.sources.input.NYCTaxiRideSource;
import de.tub.dima.condor.benchmark.sources.utils.NYCTimestampsAndWatermarks;
import de.tub.dima.condor.flinkScottyConnector.processor.BuildSynopsis;
import de.tub.dima.condor.core.synopsis.Sketches.HyperLogLogSketch;
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
public class HLLSketchAccuracy {
	public static void main(String[] args) throws Exception {

		System.out.println("HyperLogLog sketch accuracy test");
		// set up the streaming execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.setParallelism(Integer.parseInt(args[0]));
		env.setMaxParallelism(Integer.parseInt(args[0]));

//		env.setParallelism(1);
//		env.setMaxParallelism(1);

		Class<HyperLogLogSketch> synopsisClass = HyperLogLogSketch.class;

		DataStreamSource<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(-1, 200000,  new ArrayList<>())).setParallelism(1);


		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new NYCTimestampsAndWatermarks());

		SingleOutputStreamOperator<HyperLogLogSketch> synopsesStream = BuildSynopsis.timeBased(timestamped, Time.milliseconds(10000),1, synopsisClass, new Object[]{16, 7L});

		SingleOutputStreamOperator<Long> result = synopsesStream.flatMap(new countDistinct());

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/hll_result_"+Integer.parseInt(args[0])+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
//        result.writeAsText("EDADS/output/cd_taxiID.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute("HyperLogLog sketch accuracy test");
	}

	private static class countDistinct implements FlatMapFunction<HyperLogLogSketch, Long> {

		@Override
		public void flatMap(HyperLogLogSketch hllSketch, Collector<Long> out) throws Exception {
			//estimate the frequencies of all taxiID's [2013000001, 2013013223]
			out.collect(hllSketch.distinctItemsEstimator());
		}
	}


}
