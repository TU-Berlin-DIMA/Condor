package Synopsis;

import Sketches.CountMinSketch;
import de.tub.dima.scotty.core.windowFunction.CommutativeAggregateFunction;
import de.tub.dima.scotty.core.windowFunction.InvertibleAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class InvertibleSynopsisFunction<Input> implements InvertibleAggregateFunction<Tuple2<Integer,Input>, InvertibleSynopsis, InvertibleSynopsis>, CommutativeAggregateFunction<Tuple2<Integer,Input>, InvertibleSynopsis, InvertibleSynopsis>, Serializable {
    private int keyField;
    private Object[] constructorParam;
    private Constructor<? extends InvertibleSynopsis> constructor;

    public InvertibleSynopsisFunction(int keyField, Class<? extends InvertibleSynopsis> synopsisClass, Object... constructorParam){
        this.keyField = keyField;
        this.constructorParam = constructorParam;
        Class<?>[] parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        try {
            this.constructor = synopsisClass.getConstructor(parameterClasses);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Synopsis parameters didn't match any constructor");
        }
    }

    public InvertibleSynopsisFunction(Class<? extends InvertibleSynopsis> synopsisClass, Object... constructorParam){
        this.keyField = -1;
        this.constructorParam = constructorParam;
        Class<?>[] parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        try {
            this.constructor = synopsisClass.getConstructor(parameterClasses);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Synopsis parameters didn't match any constructor");
        }
    }

    public InvertibleSynopsis createAggregate() {
        InvertibleSynopsis synopsis = null;
        try {
            synopsis = constructor.newInstance(constructorParam);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return synopsis;
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
    public InvertibleSynopsis liftAndInvert(InvertibleSynopsis partialAggregate, Tuple2<Integer,Input> toRemove) {
        if(toRemove.f1 instanceof Tuple && keyField != -1){
            Input field = ((Tuple) toRemove.f1).getField(this.keyField);
            partialAggregate.decrement(field);
            return partialAggregate;
        }
        partialAggregate.decrement(toRemove.f1);
        return partialAggregate;
    }

    @Override
    public InvertibleSynopsis lift(Tuple2<Integer,Input> inputTuple) {
        InvertibleSynopsis partialAggregate = createAggregate();
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
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
    public InvertibleSynopsis liftAndCombine(InvertibleSynopsis partialAggregate, Tuple2<Integer,Input> inputTuple) {
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public InvertibleSynopsis lower(InvertibleSynopsis inputInvertibleSynopsis) {
        return inputInvertibleSynopsis;
    }
}
