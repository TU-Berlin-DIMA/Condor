package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.MergeableSynopsis;
import de.tub.dima.condor.core.Synopsis.Sampling.TimestampedElement;
import de.tub.dima.condor.core.Synopsis.StratifiedSynopsis;
import de.tub.dima.condor.core.Synopsis.Synopsis;
import de.tub.dima.condor.core.Synopsis.NonMergeableSynopsisManager;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.PriorityQueue;

public class StratifiedNonMergeableSynopsisFunction<Input, S extends Synopsis, SM extends NonMergeableSynopsisManager> implements AggregateFunction<Input, NonMergeableSynopsisManager, NonMergeableSynopsisManager>, Serializable {
    private Class<S> synopsisClass;
    private Class<SM> sliceManagerClass;
    private Object[] constructorParam;
    private Class<?>[] parameterClasses;
    private int miniBatchSize;
    private PriorityQueue<TimestampedElement<Tuple2>> dispatchList;

    public StratifiedNonMergeableSynopsisFunction(int miniBatchSize, Class<S> synopsisClass, Class<SM> sliceManagerClass, Object[] constructorParam) {
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
        this.sliceManagerClass = sliceManagerClass;
        this.miniBatchSize = miniBatchSize;
        if (miniBatchSize > 0) {
            dispatchList = new PriorityQueue<>();
        }
    }

    public StratifiedNonMergeableSynopsisFunction(Class<S> synopsisClass, Class<SM> sliceManagerClass, Object[] constructorParam) {
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
        this.sliceManagerClass = sliceManagerClass;
    }

    public NonMergeableSynopsisManager createAggregate() {
        try {
            Constructor<S> constructor = synopsisClass.getConstructor(parameterClasses);
            Constructor<SM> managerConstructor = sliceManagerClass.getConstructor();
            SM agg = managerConstructor.newInstance();
            agg.addSynopsis(constructor.newInstance(constructorParam));
            return agg;
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
    public NonMergeableSynopsisManager lift(Input input) {
        if (miniBatchSize <= 0) {
            Tuple2 inputTuple;
            if (input instanceof TimestampedElement) {
                if (!(((TimestampedElement)input).getValue() instanceof Tuple2)) {
                    throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a stratified non mergeable synopsis.");
                }
                inputTuple = (Tuple2) ((TimestampedElement) input).getValue();
            } else if (input instanceof Tuple2) {
                inputTuple = (Tuple2) input;
            } else {
                throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a stratified non mergeable synopsis.");
            }
            NonMergeableSynopsisManager partialAggregate = createAggregate();
            partialAggregate.update(inputTuple.f1);
            partialAggregate.setPartitionValue(inputTuple.f0);
            return partialAggregate;
        } else {
            if (!(input instanceof TimestampedElement)) {
                throw new IllegalArgumentException("Input elements must be from type TimestampedElement to build a stratified non mergeable synopsis with order.");
            }
            NonMergeableSynopsisManager partialAggregate = createAggregate();
            TimestampedElement<Tuple2> inputTuple = (TimestampedElement<Tuple2>) input;
            partialAggregate.setPartitionValue(inputTuple.getValue().f0);
            dispatchList.add(inputTuple);
            if (dispatchList.size() == miniBatchSize) {
                while (!dispatchList.isEmpty()) {
                    Tuple2 tupleValue = dispatchList.poll().getValue();
                    partialAggregate.update(tupleValue.f1);
                }
            }
            return partialAggregate;
        }

    }

    @Override
    public NonMergeableSynopsisManager combine(NonMergeableSynopsisManager input, NonMergeableSynopsisManager partialAggregate) {
        Object partitionValue = partialAggregate.getPartitionValue();
        Object partitionValue2 = input.getPartitionValue();
        if (partitionValue != null
                && partitionValue2 == null) {
            input.setPartitionValue(partitionValue);
        } else if (partitionValue == null
                && partitionValue2 != null) {
            partialAggregate.setPartitionValue(partitionValue2);
        } else if (!partitionValue.equals(partitionValue2)) {
            throw new IllegalArgumentException("Some internal error occurred and the synopses to be merged have not the same partition value.");
        }
        input.unify(partialAggregate);
        return input;
    }

    @Override
    public NonMergeableSynopsisManager liftAndCombine(NonMergeableSynopsisManager partialAggregate, Input input) {
        if (miniBatchSize <= 0) {
            Tuple2 inputTuple;
            if (input instanceof TimestampedElement) {
                if (!(((TimestampedElement)input).getValue() instanceof Tuple2)) {
                    throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a stratified non mergeable synopsis.");
                }
                inputTuple = (Tuple2) ((TimestampedElement) input).getValue();
            } else if (input instanceof Tuple2) {
                inputTuple = (Tuple2) input;
            } else {
                throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a stratified non mergeable synopsis.");
            }
            partialAggregate.update(inputTuple.f1);
            partialAggregate.setPartitionValue(inputTuple.f0);
            return partialAggregate;
        } else {
            if (!(input instanceof TimestampedElement)) {
                throw new IllegalArgumentException("Input elements must be from type TimestampedElement to build a stratified non mergeable synopsis with order.");
            }
            TimestampedElement<Tuple2> inputTuple = (TimestampedElement<Tuple2>) input;
            partialAggregate.setPartitionValue(inputTuple.getValue().f0);
            dispatchList.add(inputTuple);
            if (dispatchList.size() == miniBatchSize) {
                while (!dispatchList.isEmpty()) {
                    Tuple2 tupleValue = dispatchList.poll().getValue();
                    partialAggregate.update(tupleValue.f1);
                }
            }
            return partialAggregate;
        }
    }

    @Override
    public NonMergeableSynopsisManager lower(NonMergeableSynopsisManager inputSynopsis) {
        return inputSynopsis;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(synopsisClass);
        out.writeObject(sliceManagerClass);
        out.writeInt(constructorParam.length);
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(constructorParam[i]);
        }
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(parameterClasses[i]);
        }
        out.writeInt(miniBatchSize);
        if (miniBatchSize > 0){
            out.writeObject(dispatchList);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.synopsisClass = (Class<S>) in.readObject();
        this.sliceManagerClass = (Class<SM>) in.readObject();
        int nParameters = in.readInt();
        this.constructorParam = new Object[nParameters];
        for (int i = 0; i < nParameters; i++) {
            constructorParam[i] = in.readObject();
        }
        this.parameterClasses = new Class<?>[nParameters];
        for (int i = 0; i < nParameters; i++) {
            parameterClasses[i] = (Class<?>) in.readObject();
        }
        this.miniBatchSize = in.readInt();
        if (miniBatchSize > 0){
            dispatchList = (PriorityQueue<TimestampedElement<Tuple2>>) in.readObject();
        }
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
