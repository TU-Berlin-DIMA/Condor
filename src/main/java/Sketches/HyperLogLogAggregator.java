package Sketches;

import org.apache.flink.api.common.functions.AggregateFunction;

public class HyperLogLogAggregator<T> implements AggregateFunction<T, HyperLogLogSketch, Long> {

    long seed;

    @Override
    public HyperLogLogSketch createAccumulator() {

        return null;
    }

    @Override
    public HyperLogLogSketch add(T value, HyperLogLogSketch accumulator) {
        return null;
    }

    @Override
    public Long getResult(HyperLogLogSketch accumulator) {
        return null;
    }

    @Override
    public HyperLogLogSketch merge(HyperLogLogSketch a, HyperLogLogSketch b) {
        return null;
    }
}
