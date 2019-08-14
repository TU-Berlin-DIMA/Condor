package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.Random;

public class CountMinSketchAggregator<T> implements AggregateFunction<T , CountMinSketch, CountMinSketch> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

    private int height;
    private int width;
    private long seed;
    private int count;
    private int keyField;
    private PairwiseIndependentHashFunctions hashFunctions;


    public CountMinSketchAggregator(int height, int width, long s, int k) throws IOException {
        this.keyField = k;
        this.height = height;
        this.width = width;
        this.seed = s;
        this.count = 0;
    }
    /**
     * Creates a new accumulator, starting a new aggregate.
     *
     * <p>The new accumulator is typically meaningless unless a value is added
     * via {@link #add(Object, CountMinSketch)}
     *
     * <p>The accumulator is the state of a running aggregation. When a program has multiple
     * aggregates in progress (such as per key and window), the state (per key and window)
     * is the size of the accumulator.
     *
     * @return A new accumulator, corresponding to an empty aggregate.
     */
    @Override
    public CountMinSketch createAccumulator() {
        hashFunctions = new PairwiseIndependentHashFunctions(height, seed);
        CountMinSketch<T> tCountMinSketch = new CountMinSketch<T>(width, height, hashFunctions);

        return tCountMinSketch;
    }

    /**
     * Adds the given input value to the given accumulator, returning the
     * new accumulator value.
     *
     * <p>For efficiency, the input accumulator may be modified and returned.
     *
     * @param value       The value to add
     * @param accumulator The accumulator to add the value to
     */
    @Override
    public CountMinSketch add(T value, CountMinSketch accumulator) {
        count++;
        if(value instanceof Tuple){
            Object field = ((Tuple) value).getField(this.keyField);
            accumulator.update(field);
            return accumulator;
        }
        accumulator.update(value);
        return accumulator;
    }

    /**
     * Gets the result of the aggregation from the accumulator.
     *
     * @param accumulator The accumulator of the aggregation
     * @return The final aggregation result.
     */
    @Override
    public CountMinSketch getResult(CountMinSketch accumulator) {

        return accumulator;
    }

    /**
     * Merges two accumulators, returning an accumulator with the merged state.
     *
     * <p>This function may reuse any of the given accumulators as the target for the merge
     * and return that. The assumption is that the given accumulators will not be used any
     * more after having been passed to this function.
     *
     * @param a An accumulator to merge
     * @param b Another accumulator to merge
     * @return The accumulator with the merged state
     */
    @Override
    public CountMinSketch merge(CountMinSketch a, CountMinSketch b) {
        LOG.info("Accumulator ends: " + count);
        try {
            return a.merge(b);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(height);
        out.writeInt(width);
        out.writeInt(count);
        out.writeInt(keyField);
        out.writeLong(seed);
        out.writeObject(hashFunctions);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        height = in.readInt();
        width = in.readInt();
        count = in.readInt();
        keyField = in.readInt();
        seed = in.readLong();
        hashFunctions = (PairwiseIndependentHashFunctions) in.readObject();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}


