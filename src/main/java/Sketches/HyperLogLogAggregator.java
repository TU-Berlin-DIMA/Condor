package Sketches;

import org.apache.flink.api.common.functions.AggregateFunction;

public class HyperLogLogAggregator<T> implements AggregateFunction<T, HyperLogLogSketch, HyperLogLogSketch> {

    private int logRegNum, seed;

    public HyperLogLogAggregator(int logRegNum, int seed){
        this.logRegNum = logRegNum;
        this.seed = seed;
    }

    @Override
    public HyperLogLogSketch createAccumulator() {

        return new HyperLogLogSketch(logRegNum, seed);
    }

    @Override
    public HyperLogLogSketch add(T value, HyperLogLogSketch accumulator) {
        accumulator.update(value);
        return accumulator;
    }

    @Override
    public HyperLogLogSketch getResult(HyperLogLogSketch accumulator) {

        return accumulator;
    }

    @Override
    public HyperLogLogSketch merge(HyperLogLogSketch a, HyperLogLogSketch b) {

        return (HyperLogLogSketch) a.merge(b);
    }
}
