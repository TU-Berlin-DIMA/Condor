package Jobs;

import Sketches.CountMinSketch;
import Sketches.CountMinSketchAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class ProcessingTimeJob {
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
        DataStream<Tuple2<Integer, Integer>> tuple = line.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out){
                String[] tuples = value.split(",");

                if(tuples.length == 2) {

                    Integer key = new Integer(tuples[0]);
                    Integer val = new Integer(tuples[1]);

                    if (key != null && val != null) {
                        out.collect(new Tuple2<>(key, new Integer(1)));
                    }
                }
            }
        });


        


        env.execute("Flink Streaming Java API Skeleton");

    }
}
