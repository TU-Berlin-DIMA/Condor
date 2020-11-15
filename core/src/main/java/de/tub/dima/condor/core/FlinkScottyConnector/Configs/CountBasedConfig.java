package de.tub.dima.condor.core.FlinkScottyConnector.Configs;

/**
 * Configuration Class to build Count Based Synopsis.
 * slideSize and keyfield (from superclass) are optional
 */
public class CountBasedConfig extends BuildSynopsisConfig {

    public long windowSize;
    public long slideSize = -1;

    public long getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(long windowSize) {
        this.windowSize = windowSize;
    }

    public long getSlideSize() {
        return slideSize;
    }

    public void setSlideSize(long slideSize) {
        this.slideSize = slideSize;
    }

    public CountBasedConfig(Class synopsisClass, Object[] synParams, long windowSize) {
        super(synopsisClass, synParams);
        this.windowSize = windowSize;
    }

    public CountBasedConfig(int keyField, Class synopsisClass, long windowSize, long slideSize, Object... params) {
        super(keyField, synopsisClass, params);
        this.windowSize = windowSize;
        this.slideSize = slideSize;
    }
}
