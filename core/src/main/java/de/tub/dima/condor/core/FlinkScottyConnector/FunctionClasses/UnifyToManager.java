package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.NonMergeableSynopsisManager;
import de.tub.dima.scotty.core.AggregateWindow;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class UnifyToManager<M extends NonMergeableSynopsisManager> extends RichFlatMapFunction<AggregateWindow<NonMergeableSynopsisManager>, AggregateWindow<M>> {

    WindowState state;
    Class<M> managerClass;
    int parKeys;

    public UnifyToManager(int parKeys) {
        this.parKeys = parKeys;
    }

    public UnifyToManager(Class<M> managerClass) {
        this.managerClass = managerClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = new WindowState(parKeys);
    }

    @Override
    public void flatMap(AggregateWindow<NonMergeableSynopsisManager> value, Collector<AggregateWindow<M>> out) throws Exception {
        HashMap<WindowID, Tuple2<Integer, AggregateWindow<M>>> openWindows = state.value();
        WindowID windowID = new WindowID(value.getStart(), value.getEnd());
        Tuple2<Integer, AggregateWindow<M>> synopsisAggregateWindow = openWindows.get(windowID);
        if (synopsisAggregateWindow == null) {
            NonMergeableSynopsisWindowState<M> aggWindow = new NonMergeableSynopsisWindowState(value, managerClass);
            openWindows.put(windowID, new Tuple2<>(1, aggWindow));
        } else if (synopsisAggregateWindow.f0 == parKeys - 1) {
            synopsisAggregateWindow.f1.getAggValues().get(0).addSynopsis(value.getAggValues().get(0));
            out.collect(synopsisAggregateWindow.f1);
            openWindows.remove(windowID);
        } else {
            synopsisAggregateWindow.f1.getAggValues().get(0).addSynopsis(value.getAggValues().get(0));
            synopsisAggregateWindow.f0 += 1;
        }
        state.update(openWindows);
    }

}