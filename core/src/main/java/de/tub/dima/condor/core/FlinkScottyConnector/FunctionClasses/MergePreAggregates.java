package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.MergeableSynopsis;
import de.tub.dima.scotty.core.AggregateWindow;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class MergePreAggregates<S extends MergeableSynopsis> extends RichFlatMapFunction<AggregateWindow<S>, AggregateWindow<S>> {

    WindowState state;
    int parKeys;

    public MergePreAggregates(int parKeys) {
        this.parKeys = parKeys;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = new WindowState(parKeys);
    }

    @Override
    public void flatMap(AggregateWindow<S> value, Collector<AggregateWindow<S>> out) throws Exception {
        HashMap<WindowID, Tuple2<Integer, AggregateWindow<S>>> openWindows = state.value();
        WindowID windowID = new WindowID(value.getStart(), value.getEnd());
        Tuple2<Integer, AggregateWindow<S>> synopsisAggregateWindow = openWindows.get(windowID);
        if (synopsisAggregateWindow == null) {
            openWindows.put(windowID, new Tuple2<>(1, value));
        } else if (synopsisAggregateWindow.f0 == parKeys - 1) {
            synopsisAggregateWindow.f1.getAggValues().get(0).merge(value.getAggValues().get(0));
            out.collect(synopsisAggregateWindow.f1);
            openWindows.remove(windowID);
        } else {
            synopsisAggregateWindow.f1.getAggValues().get(0).merge(value.getAggValues().get(0));
            synopsisAggregateWindow.f0 += 1;
        }
        state.update(openWindows);
    }

}
