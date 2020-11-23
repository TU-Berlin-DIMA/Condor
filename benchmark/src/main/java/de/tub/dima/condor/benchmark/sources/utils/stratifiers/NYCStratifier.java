package de.tub.dima.condor.benchmark.sources.utils.stratifiers;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class NYCStratifier extends RichMapFunction<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>, Tuple2<Integer, Long>> {

    private int stratification;
    private double factor;

    public NYCStratifier(int stratification) {
        this.stratification = stratification;
        this.factor = 13222 / stratification;
    }

    @Override
    public Tuple2<Integer, Long> map(Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> value) throws Exception {
        int key = (int)((value.f1 - 2013000001) / factor);
        if (key >= stratification){
            key = stratification -1;
        }
        return new Tuple2<>(key, value.f1);
    }
}