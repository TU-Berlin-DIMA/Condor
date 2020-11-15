package de.tub.dima.condor.core.FlinkScottyConnector.Configs;

import org.apache.flink.streaming.api.windowing.time.Time;

public class TimeBasedConfig extends BuildSynopsisConfig {
    public Time windowTime;
    public Time slideTime;

    public TimeBasedConfig(Class synopsisClass, Object[] synParams, Time windowTime) {
        super(synopsisClass, synParams);
        this.windowTime = windowTime;
    }

    public TimeBasedConfig(int keyField, Class synopsisClass, Time windowTime, Time slideTime, Object... params) {
        super(keyField, synopsisClass, params);
        this.windowTime = windowTime;
        this.slideTime = slideTime;
    }

    public Time getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(Time windowTime) {
        this.windowTime = windowTime;
    }

    public Time getSlideTime() {
        return slideTime;
    }

    public void setSlideTime(Time slideTime) {
        this.slideTime = slideTime;
    }

}
