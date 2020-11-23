package de.tub.dima.condor.benchmark.sources.utils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class UniformStratifier extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

    private int stratification;

    public UniformStratifier(int stratification) {
        this.stratification = stratification;
    }

    @Override
    public Tuple2<Integer, Integer> map(Integer value) throws Exception {
        int key = (int)(value / 1000d * stratification);
        if (key >= stratification){
            key = stratification -1;
        }
        return new Tuple2<>(key, value);
    }
}