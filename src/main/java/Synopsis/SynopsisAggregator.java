package Synopsis;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * General {@link AggregateFunction} to build a customized Synopsis in an incremental way.
 *
 * @param <T1>
 * @author Rudi Poepsel Lemaitre
 */
public class SynopsisAggregator<T1> implements AggregateFunction<Tuple2<Integer,T1>, Synopsis, Synopsis> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);
    private int keyField;
    private Class<? extends Synopsis> sketchClass;
    private Object[] constructorParam;

    /**
     * Construct a new Synopsis Aggregator Function.
     *
     * @param sketchClass   the Synopsis.class
     * @param params        The parameters of the Synopsis as an Object array
     * @param keyField      The keyField with which to update the Synopsis. To update with the whole Tuple use -1!
     */
    public SynopsisAggregator(Class<? extends Synopsis> sketchClass, Object[] params, int keyField){
        this.keyField = keyField;
        this.sketchClass = sketchClass;
        this.constructorParam = params;
    }

    /**
     * Creates a new Synopsis (accumulator), starting a new aggregate.
     * The accumulator is the state of a running aggregation. When a program has multiple
     * aggregates in progress (such as per key and window), the state (per key and window)
     * is the size of the accumulator.
     *
     * @return A new Synopsis (accumulator), corresponding to an empty Synopsis.
     */
    @Override
    public Synopsis createAccumulator() {
        Class<?>[] parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        try {
            Constructor<? extends Synopsis> constructor = sketchClass.getConstructor(parameterClasses);
            Synopsis synopsis = constructor.newInstance(constructorParam);
            return synopsis;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Updates the Synopsis structure by the given input value, returning the
     * new accumulator value.
     *
     * For efficiency, the input accumulator is modified and returned.
     *
     * @param value       The value to add
     * @param accumulator The Synopsis to add the value to
     */
    @Override
    public Synopsis add(Tuple2<Integer,T1> value, Synopsis accumulator) {

        if(value.f1 instanceof Tuple && keyField != -1){
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
    public Synopsis getResult(Synopsis accumulator) {
        return accumulator;
    }

    /**
     * Merges two accumulators, returning an accumulator with the merged state.
     *
     * This function may reuse any of the given accumulators as the target for the merge
     * and return that. The assumption is that the given accumulators will not be used any
     * more after having been passed to this function.
     *
     * @param a An accumulator to merge
     * @param b Another accumulator to merge
     * @return The accumulator with the merged state
     */
    @Override
    public Synopsis merge(Synopsis a, Synopsis b) {
        try {
            return a.merge(b);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(keyField);
        out.writeObject(constructorParam);
        out.writeObject(sketchClass);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        keyField = in.readInt();
        constructorParam = (Object[]) in.readObject();
        sketchClass = (Class<? extends Synopsis>) in.readObject();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}


