package Old;

import Sketches.CountMinSketch;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.InvertibleAggregateFunction;
import de.tub.dima.scotty.core.windowFunction.InvertibleReduceAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class ScottyCountMinSketch<Input> implements InvertibleAggregateFunction<Input, CountMinSketch<Input>, CountMinSketch<Input>>, Serializable {
    private int width;
    private int height;
    private long seed;

    public ScottyCountMinSketch(int width, int height, long seed) {
        this.width = width;
        this.height = height;
        this.seed = seed;
    }

    @Override
    public CountMinSketch<Input> invert(CountMinSketch<Input> partialAggregate, CountMinSketch<Input> toRemove) {
        try {
            return partialAggregate.invert(toRemove);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public CountMinSketch<Input> liftAndInvert(CountMinSketch<Input> partialAggregate, Input toRemove) {
        CountMinSketch<Input> lifted = lift(toRemove);
        return invert(partialAggregate, lifted);
    }

    @Override
    public CountMinSketch<Input> lift(Input input) {
        CountMinSketch<Input> cm = new CountMinSketch<>(width,height,seed);
        cm.update(input);
        return cm;
    }

    @Override
    public CountMinSketch<Input> combine(CountMinSketch<Input> inputCountMinSketch, CountMinSketch<Input> partialAggregateType1) {
        try {
            return inputCountMinSketch.merge(partialAggregateType1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public CountMinSketch<Input> liftAndCombine(CountMinSketch<Input> partialAggregate, Input inputTuple) {
        CountMinSketch clone = partialAggregate.clone();
        clone.update(inputTuple);
        return clone;
    }

    @Override
    public CountMinSketch<Input> lower(CountMinSketch<Input> inputCountMinSketch) {
        return inputCountMinSketch;
    }
}
