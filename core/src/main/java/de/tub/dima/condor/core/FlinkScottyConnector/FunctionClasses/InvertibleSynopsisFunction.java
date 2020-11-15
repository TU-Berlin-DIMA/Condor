package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.Synopsis.InvertibleSynopsis;
import de.tub.dima.condor.core.Synopsis.StratifiedSynopsis;
import de.tub.dima.scotty.core.windowFunction.CommutativeAggregateFunction;
import de.tub.dima.scotty.core.windowFunction.InvertibleAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class InvertibleSynopsisFunction<Input extends Tuple2, T extends InvertibleSynopsis> implements InvertibleAggregateFunction<Input, InvertibleSynopsis, InvertibleSynopsis>, CommutativeAggregateFunction<Input, InvertibleSynopsis, InvertibleSynopsis>, Serializable {
    private Class<T> synopsisClass;
    private Object[] constructorParam;
    private Class<?>[] parameterClasses;
    private boolean stratified = false;


    public InvertibleSynopsisFunction(boolean stratified, Class<T> synopsisClass, Object... constructorParam) {
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

    public InvertibleSynopsisFunction(Class<T> synopsisClass, Object... constructorParam) {
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
    }

    public InvertibleSynopsis createAggregate() {
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
    public InvertibleSynopsis invert(InvertibleSynopsis partialAggregate, InvertibleSynopsis toRemove) {
        try {
            return partialAggregate.invert(toRemove);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public InvertibleSynopsis liftAndInvert(InvertibleSynopsis partialAggregate, Input inputTuple) {
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        partialAggregate.decrement(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public InvertibleSynopsis lift(Input inputTuple) {
        InvertibleSynopsis partialAggregate = createAggregate();
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public InvertibleSynopsis combine(InvertibleSynopsis input, InvertibleSynopsis partialAggregate) {
        try {
            return input.merge(partialAggregate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public InvertibleSynopsis liftAndCombine(InvertibleSynopsis partialAggregate, Input inputTuple) {
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public InvertibleSynopsis lower(InvertibleSynopsis inputInvertibleSynopsis) {
        return inputInvertibleSynopsis;
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
