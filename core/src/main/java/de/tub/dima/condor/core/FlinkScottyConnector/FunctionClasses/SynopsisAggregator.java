package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.MergeableSynopsis;
import de.tub.dima.condor.core.Synopsis.StratifiedSynopsis;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


/**
 * General {@link AggregateFunction} to build a customized MergeableSynopsis in an incremental way.
 *
 * @param <T1>
 * @author Rudi Poepsel Lemaitre
 */
public class SynopsisAggregator<T1> extends RichAggregateFunction<T1, MergeableSynopsis, MergeableSynopsis> {

    private transient Counter counter;

    private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);
    private boolean stratified = false;
    private Class<? extends MergeableSynopsis> sketchClass;
    private Object[] constructorParam;

    /**
     * Construct a new MergeableSynopsis Aggregator Function.
     *
     * @param sketchClass the MergeableSynopsis.class
     * @param params      The parameters of the MergeableSynopsis as an Object array
     */
    public SynopsisAggregator(Class<? extends MergeableSynopsis> sketchClass, Object[] params) {
        this.sketchClass = sketchClass;
        this.constructorParam = params;
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("synopsesCounter");
    }

    /**
     * Construct a new MergeableSynopsis Aggregator Function.
     *
     * @param sketchClass the MergeableSynopsis.class
     * @param params      The parameters of the MergeableSynopsis as an Object array
     */
    public SynopsisAggregator(boolean stratified, Class<? extends MergeableSynopsis> sketchClass, Object[] params) {
        this.sketchClass = sketchClass;
        this.constructorParam = params;
        this.stratified = stratified;
    }

    /**
     * Creates a new MergeableSynopsis (accumulator), starting a new aggregate.
     * The accumulator is the state of a running aggregation. When a program has multiple
     * aggregates in progress (such as per key and window), the state (per key and window)
     * is the size of the accumulator.
     *
     * @return A new MergeableSynopsis (accumulator), corresponding to an empty MergeableSynopsis.
     */
    @Override
    public MergeableSynopsis createAccumulator() {
        Class<?>[] parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        try {
            Constructor<? extends MergeableSynopsis> constructor = sketchClass.getConstructor(parameterClasses);
            MergeableSynopsis synopsis = constructor.newInstance(constructorParam);

            counter.inc();
            return synopsis;
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("There is no constructor in class " + sketchClass + " that match with the given parameters.");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    /**
     * Updates the MergeableSynopsis structure by the given input value, returning the
     * new accumulator value.
     * <p>
     * For efficiency, the input accumulator is modified and returned.
     *
     * @param value       The value to add
     * @param accumulator The MergeableSynopsis to add the value to
     */
    @Override
    public MergeableSynopsis add(T1 value, MergeableSynopsis accumulator) {
        if (!(value instanceof Tuple2)) {
            throw new IllegalArgumentException("Incoming elements must be from type Tuple2 to build a synopsis.");
        }
        Tuple2 tupleValue = (Tuple2) value;
        accumulator.update(tupleValue.f1);
        if (stratified) {
            ((StratifiedSynopsis) accumulator).setPartitionValue(tupleValue.f0);
        }
        return accumulator;
    }

    /**
     * Gets the result of the aggregation from the accumulator.
     *
     * @param accumulator The accumulator of the aggregation
     * @return The final aggregation result.
     */
    @Override
    public MergeableSynopsis getResult(MergeableSynopsis accumulator) {
        return accumulator;
    }

    /**
     * Merges two accumulators, returning an accumulator with the merged state.
     * <p>
     * This function may reuse any of the given accumulators as the target for the merge
     * and return that. The assumption is that the given accumulators will not be used any
     * more after having been passed to this function.
     *
     * @param a An accumulator to merge
     * @param b Another accumulator to merge
     * @return The accumulator with the merged state
     */
    @Override
    public MergeableSynopsis merge(MergeableSynopsis a, MergeableSynopsis b) {
        try {
            return a.merge(b);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(constructorParam);
        out.writeObject(sketchClass);
        out.writeBoolean(stratified);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        constructorParam = (Object[]) in.readObject();
        sketchClass = (Class<? extends MergeableSynopsis>) in.readObject();
        stratified = in.readBoolean();
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }
}


