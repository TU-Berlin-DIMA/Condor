package de.tub.dima.condor.core.FlinkScottyConnector.Configs;

import de.tub.dima.condor.core.Synopsis.NonMergeableSynopsisManager;
import org.apache.flink.streaming.api.windowing.time.Time;

public class NonMergeableTimeBasedConfig<M extends NonMergeableSynopsisManager> extends TimeBasedConfig {

    public int miniBatchSize = 0;
    public Class<M> managerClass;

    public NonMergeableTimeBasedConfig(Class synopsisClass, Object[] synParams, Time windowTime, Class<M> managerClass) {
        super(synopsisClass, synParams, windowTime);
        this.managerClass = managerClass;
    }

    public NonMergeableTimeBasedConfig(int keyField, Class synopsisClass, Time windowTime, Time slideTime, int miniBatchSize, Class<M> managerClass, Object... params) {
        super(keyField, synopsisClass, windowTime, slideTime, params);
        this.miniBatchSize = miniBatchSize;
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
}