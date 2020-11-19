package de.tub.dima.condor.flinkScottyConnector.processor.divide;

import de.tub.dima.condor.core.synopsis.Sampling.TimestampedElement;
import de.tub.dima.condor.flinkScottyConnector.processor.utils.IntegerState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

public class OrderAndIndex<T0> extends ProcessFunction<T0, Tuple2<Integer, T0>> {
    private int miniBatchSize;
    private int parKeys;

    private transient ValueState<Integer> state;
    private Tuple2<Integer, T0> newTuple;

    private PriorityQueue<TimestampedElement<T0>> dispatchList;

    public OrderAndIndex(int miniBatchSize, int parKeys) {
        this.miniBatchSize = miniBatchSize;
        this.parKeys = parKeys;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (parKeys < 1) {
            throw new IllegalArgumentException("The parallelism for the synopsis construction needs to be set with the BuildSynopsis.setParallelismKeys() method. "+parKeys);
        }
        state = new IntegerState();
        if (miniBatchSize > 1) {
            dispatchList = new PriorityQueue<>();
        }
        newTuple = new Tuple2<>();
    }


    @Override
    public void processElement(T0 value, Context ctx, Collector<Tuple2<Integer, T0>> out) throws Exception {
        if (miniBatchSize > 1) {

            dispatchList.add(new TimestampedElement(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime()));

            if (dispatchList.size() == miniBatchSize) {
                while (!dispatchList.isEmpty()) {
                    int currentNode = state.value();
                    int next = currentNode + 1;
                    next = next % parKeys;
                    state.update(next);
                    T0 tupleValue = dispatchList.poll().getValue();
                    newTuple.setField(tupleValue, 1);
                    newTuple.setField(currentNode, 0);
                    out.collect(newTuple);
                }
            }
        } else {
            int currentNode = state.value();
            int next = currentNode + 1;
            next = next % parKeys;
            state.update(next);
            newTuple.setField(value, 1);
            newTuple.setField(currentNode, 0);
            out.collect(newTuple);
        }
    }
}