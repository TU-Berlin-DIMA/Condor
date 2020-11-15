package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.MergeableSynopsis;
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

public class NonMergeableSynopsisFunction<Input, S extends Synopsis, SM extends NonMergeableSynopsisManager> implements AggregateFunction<Input, NonMergeableSynopsisManager, NonMergeableSynopsisManager>, Serializable {
    private int keyField;
    private Class<S> synopsisClass;
    private Class<SM> sliceManagerClass;
    private Object[] constructorParam;
    private Class<?>[] parameterClasses;

    public NonMergeableSynopsisFunction(int keyField, int partitionField, Class<S> synopsisClass, Class<SM> sliceManagerClass, Object[] constructorParam) {
        this.keyField = keyField;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
        this.sliceManagerClass = sliceManagerClass;
        if (partitionField >= 0 && !StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to be a subclass of StratifiedSynopsis in order to build on personalized partitions.");
        }
    }

    public NonMergeableSynopsisFunction(Class<S> synopsisClass, Class<SM> sliceManagerClass, Object[] constructorParam) {
        this.keyField = -1;
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
        if (!(input instanceof Tuple2)) {
            throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a synopsis.");
        }
        Tuple2 inputTuple = (Tuple2) input;
        NonMergeableSynopsisManager partialAggregate = createAggregate();
        if (inputTuple.f1 instanceof Tuple && keyField != -1) {
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;


    }

    @Override
    public NonMergeableSynopsisManager combine(NonMergeableSynopsisManager input, NonMergeableSynopsisManager partialAggregate) {
        input.unify(partialAggregate);
        return input;
    }

    @Override
    public NonMergeableSynopsisManager liftAndCombine(NonMergeableSynopsisManager partialAggregate, Input input) {

        if (!(input instanceof Tuple2)) {
            throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a synopsis.");
        }
        Tuple2 inputTuple = (Tuple2) input;
        if (inputTuple.f1 instanceof Tuple && keyField != -1) {
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;

    }

    @Override
    public NonMergeableSynopsisManager lower(NonMergeableSynopsisManager inputSynopsis) {
        return inputSynopsis;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(keyField);
        out.writeObject(synopsisClass);
        out.writeObject(sliceManagerClass);
        out.writeInt(constructorParam.length);
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(constructorParam[i]);
        }
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(parameterClasses[i]);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.keyField = in.readInt();
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
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
