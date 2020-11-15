package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.FlinkScottyConnector.BuildSynopsis;
import de.tub.dima.condor.core.Synopsis.Sampling.TimestampedElement;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

public class OrderAndIndex<T0> extends ProcessFunction<T0, Tuple2<Integer, Object>> {
    private int keyField;
    private int miniBatchSize;
    private int parKeys;

    private transient ValueState<Integer> state;
    private Tuple2<Integer, Object> newTuple;

    private PriorityQueue<TimestampedElement> dispatchList;

    public OrderAndIndex(int keyField, int miniBatchSize, int parKeys) {
        this.keyField = keyField;
        this.miniBatchSize = miniBatchSize;
        this.parKeys = parKeys;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (parKeys < 1) {
            throw new IllegalArgumentException("The parallelism for the synopsis construction needs to be set with the BuildSynopsis.setParallelismKeys() method. "+parKeys);
        }
        state = new BuildSynopsis.IntegerState();
        if (miniBatchSize > 1) {
            dispatchList = new PriorityQueue<>();
        }
        newTuple = new Tuple2<>();
    }

    @Override
    public void processElement(T0 value, Context ctx, Collector<Tuple2<Integer, Object>> out) throws Exception {
        if (miniBatchSize > 1) {
            if (value instanceof Tuple && keyField != -1) {
                dispatchList.add(new TimestampedElement(((Tuple) value).getField(keyField), ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime()));
            } else {
                dispatchList.add(new TimestampedElement(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime()));
            }

            if (dispatchList.size() == miniBatchSize) {
                while (!dispatchList.isEmpty()) {
                    int currentNode = state.value();
                    int next = currentNode + 1;
                    next = next % parKeys;
                    state.update(next);

                    Object tupleValue = dispatchList.poll().getValue();

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

            if (value instanceof Tuple && keyField != -1) {
                newTuple.setField(((Tuple) value).getField(keyField), 1);
            } else {
                newTuple.setField(value, 1);
            }
            newTuple.setField(currentNode, 0);
            out.collect(newTuple);
        }
    }
}