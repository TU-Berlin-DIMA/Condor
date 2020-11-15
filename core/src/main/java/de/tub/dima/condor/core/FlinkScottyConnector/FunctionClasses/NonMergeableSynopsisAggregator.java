package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.MergeableSynopsis;
import de.tub.dima.condor.core.Synopsis.Sampling.TimestampedElement;
import de.tub.dima.condor.core.Synopsis.StratifiedSynopsis;
import de.tub.dima.condor.core.Synopsis.Synopsis;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.PriorityQueue;

/**
 * General {@link AggregateFunction} to build a customized MergeableSynopsis in an incremental way.
 *
 * @param <T1>
 * @author Rudi Poepsel Lemaitre
 */
public class NonMergeableSynopsisAggregator<T1> implements AggregateFunction<T1, Synopsis, Synopsis> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);
    private boolean stratified = false;
    private Class<? extends Synopsis> sketchClass;
    private Object[] constructorParam;
    private int miniBatchSize;
    private PriorityQueue<TimestampedElement<Tuple2>> dispatchList;

    /**
     * Construct a new MergeableSynopsis Aggregator Function.
     *
     * @param sketchClass the MergeableSynopsis.class
     * @param params      The parameters of the MergeableSynopsis as an Object array
     */
    public NonMergeableSynopsisAggregator(Class<? extends Synopsis> sketchClass, Object[] params) {
        this.sketchClass = sketchClass;
        this.constructorParam = params;
    }

    /**
     * Construct a new MergeableSynopsis Aggregator Function.
     *
     * @param sketchClass the MergeableSynopsis.class
     * @param params      The parameters of the MergeableSynopsis as an Object array
     */
    public NonMergeableSynopsisAggregator(boolean stratified, Class<? extends MergeableSynopsis> sketchClass, Object[] params) {
        this.sketchClass = sketchClass;
        this.constructorParam = params;
        this.stratified = stratified;
    }

    /**
     * Construct a new MergeableSynopsis Aggregator Function.
     *
     * @param sketchClass the MergeableSynopsis.class
     * @param params      The parameters of the MergeableSynopsis as an Object array
     */
    public NonMergeableSynopsisAggregator(int miniBatchSize, Class<? extends MergeableSynopsis> sketchClass, Object[] params) {
        this.sketchClass = sketchClass;
        this.constructorParam = params;
        this.stratified = true;
        if (miniBatchSize > 0) {
            dispatchList = new PriorityQueue<>();
        }
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
    public Synopsis add(T1 value, Synopsis accumulator) {
        if (miniBatchSize > 0) {
            if (!(value instanceof TimestampedElement)) {
                throw new IllegalArgumentException("Incoming elements must be from type TimestampedElement to build a stratified non mergeable synopsis with order.");
            }
            TimestampedElement<Tuple2> inputTuple = (TimestampedElement<Tuple2>) value;
            dispatchList.add(inputTuple);
            ((StratifiedSynopsis) accumulator).setPartitionValue(inputTuple.getValue().f0);

            if (dispatchList.size() == miniBatchSize) {
                while (!dispatchList.isEmpty()) {
                    Tuple2 tupleValue = dispatchList.poll().getValue();
                    accumulator.update(tupleValue.f1);
                }
            }
        } else {
            if (!(value instanceof Tuple2)) {
                throw new IllegalArgumentException("Incoming elements must be from type Tuple2 to build a synopsis.");
            }
            Tuple2 tupleValue = (Tuple2) value;
            accumulator.update(tupleValue.f1);
            if (stratified) {
                ((StratifiedSynopsis) accumulator).setPartitionValue(tupleValue.f0);
            }
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
    public Synopsis getResult(Synopsis accumulator) {
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
    public Synopsis merge(Synopsis a, Synopsis b) {
        return null;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(constructorParam);
        out.writeObject(sketchClass);
        out.writeBoolean(stratified);
        out.writeInt(miniBatchSize);
        if (miniBatchSize > 0){
            out.writeObject(dispatchList);
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        constructorParam = (Object[]) in.readObject();
        sketchClass = (Class<? extends MergeableSynopsis>) in.readObject();
        stratified = in.readBoolean();
        this.miniBatchSize = in.readInt();
        if (miniBatchSize > 0){
            dispatchList = (PriorityQueue<TimestampedElement<Tuple2>>) in.readObject();
        }
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }
}


