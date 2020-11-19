package de.tub.dima.condor.benchmark.sources.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple3;

public class SyntecticExtractKeyField<T> implements MapFunction<Tuple3<Integer, Integer, Long>, T> {
    private int keyField;

    public SyntecticExtractKeyField(int keyField) {
        if (keyField < 0 || keyField > 2){
            throw new IllegalArgumentException("Not a valid key field for the selected Source.");
        }
        this.keyField = keyField;
    }

    @Override
    public T map(Tuple3<Integer, Integer, Long> tuple) throws Exception {
        return tuple.getField(keyField);
    }
}
