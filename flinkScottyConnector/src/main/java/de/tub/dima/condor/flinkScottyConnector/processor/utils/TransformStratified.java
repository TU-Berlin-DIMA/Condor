package de.tub.dima.condor.flinkScottyConnector.processor.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Stateful map functions to add the parallelism variable
 *
 * @param <T0> type of input elements
 */
public class TransformStratified<T0 extends Tuple> implements MapFunction<T0, Tuple2<String, Object>> {

    public int partitionField;
    public int keyField;
    private Tuple2<String, Object> newTuple;

    public TransformStratified(int partitionField, int keyField) {
        this.keyField = keyField;
        this.partitionField = partitionField;
        newTuple = new Tuple2<>();
    }

    @Override
    public Tuple2<String, Object> map(T0 value) throws Exception {
        if (keyField != -1) {
            newTuple.setField(value.getField(keyField), 1);
        } else {
            newTuple.setField(value, 1);
        }
        newTuple.setField(value.getField(partitionField).toString(), 0);
        return newTuple;
    }
}
