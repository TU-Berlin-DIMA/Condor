package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;

public class SketchAggregator<T1, S extends Sketch> implements AggregateFunction<Tuple2<Integer,T1>, S, S> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);
    private S sketch;
    private int keyField;

    public SketchAggregator(S sketch, int keyField){
        this.keyField = keyField;
        this.sketch = sketch;
        Class<? extends Sketch> c = this.sketch.getClass();
    }
    /**
     * Creates a new accumulator, starting a new aggregate.
     *
     * <p>The new accumulator is typically meaningless unless a value is added
     * via
     *
     * <p>The accumulator is the state of a running aggregation. When a program has multiple
     * aggregates in progress (such as per key and window), the state (per key and window)
     * is the size of the accumulator.
     *
     * @return A new accumulator, corresponding to an empty aggregate.
     */
    @Override
    public S createAccumulator() {
        return sketch;
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
    public S add(Tuple2<Integer,T1> value, S accumulator) {
        if(value.f1 instanceof Tuple){
            Object field = ((Tuple) value.f1).getField(this.keyField);
            accumulator.update(field);
            return accumulator;
        }
        accumulator.update(value.f1);
        return accumulator;
    }

    /**
     * Gets the result of the aggregation from the accumulator.
     *
     * @param accumulator The accumulator of the aggregation
     * @return The final aggregation result.
     */
    @Override
    public S getResult(S accumulator) {
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
    public S merge(S a, S b) {
        try {
            return (S) a.merge(b);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(keyField);
        out.writeObject(sketch);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        keyField = in.readInt();
        sketch = (S) in.readObject();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}


