package Sketches;

import akka.util.HashCode;
import org.apache.flink.api.common.functions.AggregateFunction;

public class HyperLogLogAggregator<T> implements AggregateFunction<T, HyperLogLog, Long> {

    long seed;

    @Override
    public HyperLogLog createAccumulator() {
        return null;
    }

    @Override
    public HyperLogLog add(T value, HyperLogLog accumulator) {
        return null;
    }

    @Override
    public Long getResult(HyperLogLog accumulator) {
        return null;
    }

    @Override
    public HyperLogLog merge(HyperLogLog a, HyperLogLog b) {
        return null;
    }
}
