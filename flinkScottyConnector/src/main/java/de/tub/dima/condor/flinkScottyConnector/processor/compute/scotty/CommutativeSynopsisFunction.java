package de.tub.dima.condor.flinkScottyConnector.processor.compute.scotty;

import de.tub.dima.condor.core.synopsis.CommutativeSynopsis;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsis;
import de.tub.dima.scotty.core.windowFunction.CommutativeAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class CommutativeSynopsisFunction<Input extends Tuple2,T extends CommutativeSynopsis> implements CommutativeAggregateFunction<Input, CommutativeSynopsis, CommutativeSynopsis>, Serializable {
    private Class<T> synopsisClass;
    private Object[] constructorParam;
    private Class<?>[] parameterClasses;
    private boolean stratified = false;


    public CommutativeSynopsisFunction(Class<T> synopsisClass, Object... constructorParam){
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
    }

    public CommutativeSynopsisFunction(boolean stratified, Class<T> synopsisClass, Object... constructorParam){
        this.stratified = stratified;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        if (stratified && !StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to be a subclass of StratifiedSynopsis in order to build on personalized partitions.");
        }
        this.synopsisClass = synopsisClass;
    }


    public CommutativeSynopsis createAggregate() {
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
    public CommutativeSynopsis lift(Input inputTuple) {
        CommutativeSynopsis partialAggregate = createAggregate();
        partialAggregate.update(inputTuple.f1);
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        return partialAggregate;
    }

    @Override
    public CommutativeSynopsis combine(CommutativeSynopsis input, CommutativeSynopsis partialAggregate) {
        try {
            return input.merge(partialAggregate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public CommutativeSynopsis liftAndCombine(CommutativeSynopsis partialAggregate, Input inputTuple) {
        partialAggregate.update(inputTuple.f1);
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        return partialAggregate;
    }

    @Override
    public CommutativeSynopsis lower(CommutativeSynopsis inputCommutativeSynopsis) {
        return inputCommutativeSynopsis;
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
