package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.MergeableSynopsis;
import de.tub.dima.scotty.core.AggregateWindow;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.util.HashMap;

/**
 * Integer state for Stateful Functions
 */
public class WindowState implements ValueState<HashMap<WindowID, Tuple2<Integer, AggregateWindow<MergeableSynopsis>>>> {

    HashMap<WindowID, Tuple2<Integer, AggregateWindow<MergeableSynopsis>>> openWindows;
    int numberKeys;

    public WindowState(int numberKeys) {
        this.numberKeys = numberKeys;
        this.openWindows = new HashMap<>();
    }

    @Override
    public HashMap value() throws IOException {
        return openWindows;
    }

    @Override
    public void update(HashMap value) throws IOException {
        this.openWindows = value;
    }

    @Override
    public void clear() {
        openWindows.clear();
    }
}
