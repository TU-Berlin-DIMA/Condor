package de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils;

import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import org.apache.flink.api.common.functions.ReduceFunction;

public class MergeSynopsis implements ReduceFunction<CountMinSketch> {
    public CountMinSketch reduce(CountMinSketch value1, CountMinSketch value2) throws Exception {
        return value1.merge(value2);
    }
}
