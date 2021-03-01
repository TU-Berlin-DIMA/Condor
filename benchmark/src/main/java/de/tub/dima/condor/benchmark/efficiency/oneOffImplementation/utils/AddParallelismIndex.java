package de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class AddParallelismIndex<T> extends RichMapFunction<T, Tuple2<Integer, T>> {
    @Override
    public Tuple2<Integer, T> map(T value) throws Exception {

        return new Tuple2<>(this.getRuntimeContext().getIndexOfThisSubtask(), value);
    }
}