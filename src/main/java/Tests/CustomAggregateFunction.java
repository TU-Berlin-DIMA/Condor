package Tests;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class CustomAggregateFunction<T0> implements AggregateFunction<Tuple2<Integer, T0>, HashMap<Integer, Integer>, HashMap<Integer, Integer>> {

    @Override
    public HashMap<Integer, Integer> createAccumulator() {
        return new HashMap<Integer, Integer>();
    }

    @Override
    public HashMap<Integer, Integer> add(Tuple2<Integer, T0> value, HashMap<Integer, Integer> accumulator) {
        accumulator.put(value.f0, 1);
        return accumulator;
    }

    @Override
    public HashMap<Integer, Integer> getResult(HashMap<Integer, Integer> accumulator) {
        return accumulator;
    }

    @Override
    public HashMap<Integer, Integer> merge(HashMap<Integer, Integer> a, HashMap<Integer, Integer> b) {
        a.putAll(b);
        return a;
    }
}
