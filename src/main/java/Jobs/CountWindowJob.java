package Jobs;

import Sketches.CountMinSketch;
import Sketches.CountMinSketchAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CountWindowJob {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //Set up the sketch parameters
        int width = 10;
        int height = 5;
        int seed = 1;

        //Read file, convert to tuple and add the partition index
        DataStream<String> line = env.readTextFile("data/10percent.csv");
        SingleOutputStreamOperator<CountMinSketch> distributed = line.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
                String[] tuples = value.split(",");

                if (tuples.length == 2) {

                    Integer key = new Integer(tuples[0]);
                    Integer val = new Integer(tuples[1]);

                    if (key != null && val != null) {
                        out.collect(new Tuple2<>(key, new Integer(1)));
                    }
                }
            }
        }).map(new ProcessingTimeJob.AddParallelismRichMapFunction())
                .keyBy(0)
                        .timeWindow(Time.seconds(1))
                                .aggregate(new CountMinSketchAggregator(height, width, seed, 1));

        SingleOutputStreamOperator<CountMinSketch> finalSketches = distributed.timeWindowAll(Time.seconds(1))
                .reduce(new ReduceFunction<CountMinSketch>() {
                    @Override
                    public CountMinSketch reduce(CountMinSketch value1, CountMinSketch value2) throws Exception {
                        return value1.merge(value2);
                    }
                });

        finalSketches.writeAsText("output/processingTimeSketches.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");

    }

    /**
     *  Stateful map function to add the parallelism variable
     */
    public static class AddParallelismRichMapFunction extends RichMapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>> {

        private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

        ValueState<Integer> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new ValueState<Integer>() {
                int value;

                @Override
                public Integer value() throws IOException {
                    return value;
                }

                @Override
                public void update(Integer value) throws IOException {
                    this.value = value;
                }

                @Override
                public void clear() {
                    value = 0;
                }
            };
            state.update(0);
        }

        @Override
        public Tuple3<Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {

            int currentNode = state.value();
            int next = currentNode % this.getRuntimeContext().getNumberOfParallelSubtasks();
            state.update(next);

            return new Tuple3<>(currentNode, value.f0, value.f1);
        }
    }

}
