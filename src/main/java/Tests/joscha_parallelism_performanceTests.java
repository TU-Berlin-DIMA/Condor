package Tests;

import Sketches.FastAMS;
import Synopsis.BuildSynopsis;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.HashMap;

public class joscha_parallelism_performanceTests {
    public static void main(String[] args) throws Exception {


        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(8);

        Time windowTime = Time.minutes(1);

        DataStream<String> line = env.readTextFile("data/timestamped.csv");
        DataStream<Tuple3<Integer, Integer, Long>> timestamped = line.flatMap(new BashHistogramTest.CreateTuplesFlatMap()) // Create the tuples from the incoming Data
                .assignTimestampsAndWatermarks(new BashHistogramTest.CustomTimeStampExtractor()); // extract the timestamps and add watermarks


        SingleOutputStreamOperator aggregate = timestamped.map(new AddParallelismTuple())
                .keyBy(0)
                .timeWindow((windowTime))
                .aggregate(new customAggregateFunction<>());// key by the new parallelism


        aggregate.writeAsText("output/parallelism_test.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute("Flink Streaming Java API Skeleton");
    }

    /**
     * Stateful map functions to add the parallelism variable
     *
     * @param <T0> type of input elements
     */
    public static class AddParallelismTuple<T0> extends RichMapFunction<T0, Tuple2<Integer,T0>> {

        ValueState<Integer> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new BuildSynopsis.IntegerState();
        }

        @Override
        public Tuple2<Integer,T0> map(T0 value) throws Exception {
            Tuple2 newTuple = new Tuple2<Integer,T0>();
            int currentNode = state.value();
            int next = currentNode +1;
            next = next % this.getRuntimeContext().getNumberOfParallelSubtasks();
            state.update(next);

            newTuple.setField(currentNode,0);
            newTuple.setField(value,1);

            return newTuple;
        }
    }
}
