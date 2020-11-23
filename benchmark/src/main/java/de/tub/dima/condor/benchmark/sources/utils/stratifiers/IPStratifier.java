package de.tub.dima.condor.benchmark.sources.utils.stratifiers;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class IPStratifier extends RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> {
    private int stratification;
    private double factor;

    public IPStratifier(int stratification) {
        this.stratification = stratification;
        this.factor = 2147483647/stratification;
    }

    @Override
    public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Long> value) throws Exception {
        int key = (int) (value.f0 / factor);
        if (key >= stratification){
            key = stratification -1;
        }
        return new Tuple2<>(key, value.f0);
    }
}