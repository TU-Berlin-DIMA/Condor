package de.tub.dima.condor.core.FlinkScottyConnector.Configs;

import de.tub.dima.condor.core.Synopsis.NonMergeableSynopsisManager;

public class NonMergeableCountBasedConfig<M extends NonMergeableSynopsisManager> extends CountBasedConfig {

    public int miniBatchSize = 0;
    public Class<M> managerClass;

    public NonMergeableCountBasedConfig(Class synopsisClass, Object[] synParams, long windowSize, Class<M> managerClass) {
        super(synopsisClass, synParams, windowSize);
        this.managerClass = managerClass;
    }

    public int getMiniBatchSize() {
        return miniBatchSize;
    }

    public void setMiniBatchSize(int miniBatchSize) {
        this.miniBatchSize = miniBatchSize;
    }

    public Class<M> getManagerClass() {
        return managerClass;
    }

    public void setManagerClass(Class<M> managerClass) {
        this.managerClass = managerClass;
    }

    public NonMergeableCountBasedConfig(int keyField, Class synopsisClass, long windowSize, long slideSize, int miniBatchSize, Class<M> managerClass, Object... params) {
        super(keyField, synopsisClass, windowSize, slideSize, params);
        this.miniBatchSize = miniBatchSize;
        this.managerClass = managerClass;
    }
}
