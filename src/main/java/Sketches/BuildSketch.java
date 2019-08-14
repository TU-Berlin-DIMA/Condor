package Sketches;

import Jobs.EventTimeStreamingJob;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class BuildSketch {
    public SingleOutputStreamOperator<CountMinSketch> timeBased(DataStream<Tuple> inputStream, Time windowTime, AggregateFunction agg){
        return inputStream
                .map(new AddParallelismRichFlatMapFunction())
                .keyBy(0)
                .timeWindow(windowTime)
                .aggregate(agg)
                .timeWindowAll(windowTime)
                .reduce(new ReduceFunction<CountMinSketch>() { // Merge all sketches in the global window
                    @Override
                    public CountMinSketch reduce(CountMinSketch value1, CountMinSketch value2) throws Exception {
                        return value1.merge(value2);
                    }
                });
    }

    /**
     *  Stateful map function to add the parallelism variable
     */
    public static class AddParallelismRichFlatMapFunction extends RichMapFunction<Tuple, Tuple> {

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
        public Tuple map(Tuple value) throws Exception {
            Tuple newTuple;

            switch (value.getArity()) {
                case 1: newTuple = new Tuple2<>(); break;
                case 2: newTuple = new Tuple3<>(); break;
                case 3: newTuple = new Tuple4<>(); break;
                case 4: newTuple = new Tuple5<>(); break;
                case 5: newTuple = new Tuple6<>(); break;
                case 6: newTuple = new Tuple7<>(); break;
                case 7: newTuple = new Tuple8<>(); break;
                case 8: newTuple = new Tuple9<>(); break;
                case 9: newTuple = new Tuple10<>(); break;
                case 10: newTuple = new Tuple11<>(); break;
                case 11: newTuple = new Tuple12<>(); break;
                case 12: newTuple = new Tuple13<>(); break;
                case 13: newTuple = new Tuple14<>(); break;
                case 14: newTuple = new Tuple15<>(); break;
                case 15: newTuple = new Tuple16<>(); break;
                case 16: newTuple = new Tuple17<>(); break;
                case 17: newTuple = new Tuple18<>(); break;
                case 18: newTuple = new Tuple19<>(); break;
                case 19: newTuple = new Tuple20<>(); break;
                case 20: newTuple = new Tuple21<>(); break;
                case 21: newTuple = new Tuple22<>(); break;
                case 22: newTuple = new Tuple23<>(); break;
                case 23: newTuple = new Tuple24<>(); break;
                case 24: newTuple = new Tuple25<>(); break;
                default:
                    throw new IllegalStateException("Excessive arity in tuple.");
            }

            int currentNode = state.value();
            int next = currentNode +1;
            next = next % this.getRuntimeContext().getNumberOfParallelSubtasks();
            state.update(next);

            newTuple.setField(currentNode,0);
            for (int i = 0; i < value.getArity(); i++) {
                newTuple.setField(value.getField(i),i+1);
            }

            return newTuple;

        }
    }
}
