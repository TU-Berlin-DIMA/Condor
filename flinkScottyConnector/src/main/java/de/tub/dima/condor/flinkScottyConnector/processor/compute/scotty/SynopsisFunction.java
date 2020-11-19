package de.tub.dima.condor.flinkScottyConnector.processor.compute.scotty;

import de.tub.dima.condor.core.synopsis.MergeableSynopsis;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsis;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class SynopsisFunction<Input extends Tuple2, T extends MergeableSynopsis> implements AggregateFunction<Input, MergeableSynopsis, MergeableSynopsis>, Serializable {
    private Class<T> synopsisClass;
    private Object[] constructorParam;
    private Class<?>[] parameterClasses;
    private boolean stratified = false;

    public SynopsisFunction(boolean stratified, Class<T> synopsisClass, Object[] constructorParam) {
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
        if (stratified && !StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to be a subclass of StratifiedSynopsis in order to build on personalized partitions.");
        }
        this.stratified = stratified;
    }


    public SynopsisFunction(Class<T> synopsisClass, Object[] constructorParam) {
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
    }

    public MergeableSynopsis createAggregate() {
        try {
            Constructor<T> constructor = synopsisClass.getConstructor(parameterClasses);
            return constructor.newInstance(constructorParam);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("MergeableSynopsis parameters didn't match any constructor");
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Couldn't instantiate class");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Access not permitted");
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("InvocationTargetException");
        }
    }

    @Override
    public MergeableSynopsis lift(Input inputTuple) {
        MergeableSynopsis partialAggregate = createAggregate();
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public MergeableSynopsis combine(MergeableSynopsis input, MergeableSynopsis partialAggregate) {
        try {
            if (stratified) {
                Object partitionValue = ((StratifiedSynopsis) partialAggregate).getPartitionValue();
                Object partitionValue2 = ((StratifiedSynopsis) input).getPartitionValue();
                if (partitionValue != null
                        && partitionValue2 == null) {
                    ((StratifiedSynopsis) input).setPartitionValue(partitionValue);
                } else if (partitionValue == null
                        && partitionValue2 != null) {
                    ((StratifiedSynopsis) partialAggregate).setPartitionValue(partitionValue2);
                } else if (!partitionValue.equals(partitionValue2)) {
                    throw new IllegalArgumentException("Some internal error occurred and the synopses to be merged have not the same partition value.");
                }
            }
            return input.merge(partialAggregate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public MergeableSynopsis liftAndCombine(MergeableSynopsis partialAggregate, Input inputTuple) {
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public MergeableSynopsis lower(MergeableSynopsis inputSynopsis) {
        return inputSynopsis;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeBoolean(stratified);
        out.writeObject(synopsisClass);
        out.writeInt(constructorParam.length);
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(constructorParam[i]);
        }
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(parameterClasses[i]);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.stratified = in.readBoolean();
        this.synopsisClass = (Class<T>) in.readObject();
        int nParameters = in.readInt();
        this.constructorParam = new Object[nParameters];
        for (int i = 0; i < nParameters; i++) {
            constructorParam[i] = in.readObject();
        }
        this.parameterClasses = new Class<?>[nParameters];
        for (int i = 0; i < nParameters; i++) {
            parameterClasses[i] = (Class<?>) in.readObject();
        }
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
