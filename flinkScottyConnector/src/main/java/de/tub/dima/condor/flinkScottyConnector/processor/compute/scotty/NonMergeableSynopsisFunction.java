package de.tub.dima.condor.flinkScottyConnector.processor.compute.scotty;

import de.tub.dima.condor.core.synopsis.NonMergeableSynopsisManager;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsis;
import de.tub.dima.condor.core.synopsis.Synopsis;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class NonMergeableSynopsisFunction<T, S extends Synopsis, SM extends NonMergeableSynopsisManager> implements AggregateFunction<Tuple2<Integer, T>, NonMergeableSynopsisManager, NonMergeableSynopsisManager>, Serializable {
    private Class<S> synopsisClass;
    private Class<SM> sliceManagerClass;
    private Object[] constructorParam;
    private Class<?>[] parameterClasses;


    public NonMergeableSynopsisFunction(Class<S> synopsisClass, Class<SM> sliceManagerClass, Object[] constructorParam) {
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
    public NonMergeableSynopsisManager lift(Tuple2<Integer, T> input) {
        NonMergeableSynopsisManager partialAggregate = createAggregate();
        partialAggregate.update(input.f1);
        return partialAggregate;


    }

    @Override
    public NonMergeableSynopsisManager combine(NonMergeableSynopsisManager input, NonMergeableSynopsisManager partialAggregate) {
        input.unify(partialAggregate);
        return input;
    }

    @Override
    public NonMergeableSynopsisManager liftAndCombine(NonMergeableSynopsisManager partialAggregate, Tuple2<Integer, T> input) {
        partialAggregate.update(input.f1);
        return partialAggregate;
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
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
