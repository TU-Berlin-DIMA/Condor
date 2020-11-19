package de.tub.dima.condor.flinkScottyConnector.processor.merge;

import de.tub.dima.condor.core.synopsis.NonMergeableSynopsisManager;
import de.tub.dima.condor.core.synopsis.Synopsis;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class NonMergeableSynopsisUnifier<S extends Synopsis> implements AggregateFunction<S, NonMergeableSynopsisManager, NonMergeableSynopsisManager> {
    private Class<? extends NonMergeableSynopsisManager> managerClass;

    public NonMergeableSynopsisUnifier(Class<? extends NonMergeableSynopsisManager> managerClass) {
        this.managerClass = managerClass;
    }

    @Override
    public NonMergeableSynopsisManager createAccumulator() {
        Constructor<? extends NonMergeableSynopsisManager> constructor = null;
        try {
            constructor = managerClass.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
        }
        NonMergeableSynopsisManager manager = null;
        try {
            manager = constructor.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
        } catch (IllegalAccessException e) {
            throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
        } catch (InvocationTargetException e) {
            throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
        }
        return manager;
    }

    @Override
    public NonMergeableSynopsisManager add(S value, NonMergeableSynopsisManager accumulator) {
        accumulator.addSynopsis(value);
        return accumulator;
    }

    @Override
    public NonMergeableSynopsisManager getResult(NonMergeableSynopsisManager accumulator) {
        return accumulator;
    }

    @Override
    public NonMergeableSynopsisManager merge(NonMergeableSynopsisManager a, NonMergeableSynopsisManager b) {
        for (int i = 0; i < b.getUnifiedSynopses().size(); i++) {
            a.addSynopsis((S) b.getUnifiedSynopses().get(i));
        }
        return a;
    }
}
