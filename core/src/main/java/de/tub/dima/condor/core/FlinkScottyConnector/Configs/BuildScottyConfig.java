package de.tub.dima.condor.core.FlinkScottyConnector.Configs;

import de.tub.dima.condor.core.Synopsis.NonMergeableSynopsisManager;
import de.tub.dima.scotty.core.windowType.Window;

/**
 * Configuration Class used to build Scotty Jobs.
 *
 * miniBatchSize, SliceManagerClass and managerClass only need to be used in case of non-mergeable synopsis
 * @param <SM>
 * @param <M>
 */
public class BuildScottyConfig<SM extends NonMergeableSynopsisManager, M extends NonMergeableSynopsisManager> extends BuildSynopsisConfig {
    public Window[] windows;
    public int miniBatchSize;
    public Class<SM> sliceManagerClass;
    public Class<M> managerClass;

    public BuildScottyConfig(Class synopsisClass, Object[] synParams, Window[] windows) {
        super(synopsisClass, synParams);
        this.windows = windows;
    }

    public BuildScottyConfig(int keyField, Class synopsisClass, Window[] windows, Object... params) {
        super(keyField, synopsisClass, params);
        this.windows = windows;
    }

    public BuildScottyConfig(int keyField, Class synopsisClass, Window[] windows, int miniBatchSize, Class<SM> sliceManagerClass, Class<M> managerClass, Object... params) {
        super(keyField, synopsisClass, params);
        this.windows = windows;
        this.miniBatchSize = miniBatchSize;
        this.sliceManagerClass = sliceManagerClass;
        this.managerClass = managerClass;
    }

    public BuildScottyConfig(Class synopsisClass, Object[] synParams, Window[] windows, Class<SM> sliceManagerClass, Class<M> managerClass) {
        super(synopsisClass, synParams);
        this.windows = windows;
        this.sliceManagerClass = sliceManagerClass;
        this.managerClass = managerClass;
    }

    public Window[] getWindows() {
        return windows;
    }

    public void setWindows(Window[] windows) {
        this.windows = windows;
    }

    public int getMiniBatchSize() {
        return miniBatchSize;
    }

    public void setMiniBatchSize(int miniBatchSize) {
        this.miniBatchSize = miniBatchSize;
    }

    public Class<SM> getSliceManagerClass() {
        return sliceManagerClass;
    }

    public void setSliceManagerClass(Class<SM> sliceManagerClass) {
        this.sliceManagerClass = sliceManagerClass;
    }

    public Class<M> getManagerClass() {
        return managerClass;
    }

    public void setManagerClass(Class<M> managerClass) {
        this.managerClass = managerClass;
    }
}
