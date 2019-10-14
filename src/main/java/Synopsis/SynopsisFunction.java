package Synopsis;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CommutativeAggregateFunction;
import de.tub.dima.scotty.core.windowFunction.InvertibleAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class SynopsisFunction<Input> implements AggregateFunction<Tuple2<Integer,Input>, Synopsis, Synopsis>, Serializable {
    private int keyField;
    private Object[] constructorParam;
    private Constructor<? extends Synopsis> constructor;

    public SynopsisFunction(int keyField, Class<? extends InvertibleSynopsis> synopsisClass, Object[] constructorParam){
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

    public SynopsisFunction(Class<? extends Synopsis> synopsisClass, Object[] constructorParam){
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

    public Synopsis createAggregate() {
        Synopsis synopsis = null;
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
    public Synopsis lift(Tuple2<Integer,Input> inputTuple) {
        Synopsis partialAggregate = createAggregate();
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public Synopsis combine(Synopsis input, Synopsis partialAggregate) {
        try {
            return input.merge(partialAggregate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Synopsis liftAndCombine(Synopsis partialAggregate, Tuple2<Integer,Input> inputTuple) {
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public Synopsis lower(Synopsis inputInvertibleSynopsis) {
        return inputInvertibleSynopsis;
    }
}
