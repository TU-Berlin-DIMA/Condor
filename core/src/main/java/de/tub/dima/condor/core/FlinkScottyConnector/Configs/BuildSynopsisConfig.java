package de.tub.dima.condor.core.FlinkScottyConnector.Configs;

import de.tub.dima.condor.core.Synopsis.Synopsis;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;


public class BuildSynopsisConfig <S extends Synopsis> implements Serializable {

    public Class<S> synopsisClass;
    public int keyField = -1;
    public Object[] synParams;

    public BuildSynopsisConfig(Class<S> synopsisClass, Object[] synParams) {
        this.synopsisClass = synopsisClass;
        this.synParams = synParams;
    }

    public BuildSynopsisConfig(int keyField, Class<S> synopsisClass, Object... params) {
        this.keyField = keyField;
        this.synopsisClass = synopsisClass;
        this.synParams = params;
    }

    public Class<S> getSynopsisClass() {
        return synopsisClass;
    }

    public void setSynopsisClass(Class<S> synopsisClass) {
        this.synopsisClass = synopsisClass;
    }


    public int getKeyField() {
        return keyField;
    }

    public Object[] getSynParams() {
        return synParams;
    }

    public void setSynParams(Object[] synParams) {
        this.synParams = synParams;
    }

    public void setKeyField(int keyField) {
        this.keyField = keyField;

    }
}
