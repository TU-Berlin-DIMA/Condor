package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.NonMergeableSynopsisManager;
import de.tub.dima.condor.core.Synopsis.Synopsis;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class NonMergeableSynopsisWindowState<M extends NonMergeableSynopsisManager> implements AggregateWindow<M> {
    private final long start;
    private final long endTs;
    private final WindowMeasure measure;
    private List<M> aggValues;

    public NonMergeableSynopsisWindowState(AggregateWindow<Synopsis> aggWindow, Class<M> managerClass) {
        this(aggWindow.getStart(), aggWindow.getEnd(), aggWindow.getMeasure());
        Constructor<M> constructor;
        try {
            constructor = managerClass.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
        }
        for (int i = 0; i < aggWindow.getAggValues().size(); i++) {
            M manager;
            try {
                manager = constructor.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            } catch (IllegalAccessException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            } catch (InvocationTargetException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            }
            manager.addSynopsis(aggWindow.getAggValues().get(i));
            aggValues.add(manager);
        }
    }

    public NonMergeableSynopsisWindowState(long start, long endTs, WindowMeasure measure) {
        this.start = start;
        this.endTs = endTs;
        this.measure = measure;
        this.aggValues = new ArrayList<>();
    }

    @Override
    public WindowMeasure getMeasure() {
        return measure;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getEnd() {
        return endTs;
    }

    @Override
    public List<M> getAggValues() {
        return aggValues;
    }

    @Override
    public boolean hasValue() {
        return !aggValues.isEmpty();
    }
}