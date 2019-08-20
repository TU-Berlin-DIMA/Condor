package Sketches;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;

public final class BuildSketch {
    public static <T, S extends Sketch> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Class<S> sketchClass,Object[] parameters, int keyField){
        SketchAggregator agg = new SketchAggregator(sketchClass, parameters, keyField);

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismTuple())
                .keyBy(0)
                .timeWindow(windowTime)
                .aggregate(agg)
                .timeWindowAll(windowTime)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public Sketch reduce(Sketch value1, Sketch value2) throws Exception {
                        return value1.merge(value2);
                    }
                }).returns(sketchClass);
        return reduce;
    }

    public static <T, S extends Sketch> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, Class<S> sketchClass, Object[] parameters, int keyField){
        SketchAggregator agg = new SketchAggregator(sketchClass, parameters, keyField);
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismTuple())
                .keyBy(0)
                .countWindow(windowSize/parallelism)
                .aggregate(agg)
                .countWindowAll(parallelism)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public Sketch reduce(Sketch value1, Sketch value2) throws Exception {
                        return value1.merge(value2);
                    }
                }).returns(sketchClass);
        return reduce;
    }

    public static class IntegerState implements ValueState<Integer>{
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
    }
    /**
     *  Stateful map functions to add the parallelism variable
     */
    public static class AddParallelismTuple<T0> extends RichMapFunction<T0, Tuple2<Integer,T0>> {

        ValueState<Integer> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new IntegerState();
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
