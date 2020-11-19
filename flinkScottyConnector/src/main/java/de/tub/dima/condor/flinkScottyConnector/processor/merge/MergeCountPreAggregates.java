package de.tub.dima.condor.flinkScottyConnector.processor.merge;

import de.tub.dima.condor.core.synopsis.MergeableSynopsis;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class MergeCountPreAggregates<S extends MergeableSynopsis> extends RichFlatMapFunction<S, S> {

    private int count;
    private S currentMerged;

    int parKeys;

    public MergeCountPreAggregates(int parKeys) {
        this.parKeys = parKeys;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        count = 0;
    }

    @Override
    public void flatMap(S value, Collector<S> out) throws Exception {
        if (count < parKeys-1) {
            if (count == 0) {
                currentMerged = value;
            } else {
                currentMerged.merge(value);
            }
            count++;
        } else {
            count = 0;
            currentMerged.merge(value);
            out.collect(currentMerged);
        }
    }

}
