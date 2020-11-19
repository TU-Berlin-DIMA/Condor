package de.tub.dima.condor.benchmark.sources.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple11;

public class NYCExtractKeyField<T> implements MapFunction<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>, T> {
    private int keyField;

    public NYCExtractKeyField(int keyField) {
        if (keyField < 0 || keyField > 10){
            throw new IllegalArgumentException("Not a valid key field for the NYC Source.");
        }
        this.keyField = keyField;
    }

    @Override
    public T map(Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> tuple) throws Exception {
        return tuple.getField(keyField);
    }
}
