package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.Synopsis;
import de.tub.dima.condor.core.Synopsis.NonMergeableSynopsisManager;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class NonMergeableSynopsisCountUnifier<S extends Synopsis> extends RichFlatMapFunction<S, NonMergeableSynopsisManager> {

    private int count;
    private NonMergeableSynopsisManager currentManager;

    private Class<? extends NonMergeableSynopsisManager> managerClass;
    private int parKeys;

    public NonMergeableSynopsisCountUnifier(Class<? extends NonMergeableSynopsisManager> managerClass, int parKeys) {
        this.managerClass = managerClass;
        this.parKeys = parKeys;
    }

    public NonMergeableSynopsisManager newManager() {
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
    public void open(Configuration parameters) throws Exception {
        currentManager = newManager();
    }

    @Override
    public void flatMap(S value, Collector<NonMergeableSynopsisManager> out) throws Exception {
        if (currentManager.getUnifiedSynopses().size() < parKeys-1) {
            currentManager.addSynopsis(value);
        } else {
            currentManager.addSynopsis(value);
            out.collect(currentManager);
            currentManager.cleanManager();
        }
    }

}