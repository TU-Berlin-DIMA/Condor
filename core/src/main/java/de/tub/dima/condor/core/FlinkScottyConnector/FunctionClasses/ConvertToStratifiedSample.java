package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.Sampling.TimestampedElement;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ConvertToStratifiedSample<T extends Tuple> extends ProcessFunction<T, Tuple2<String, TimestampedElement>> {
    private int keyField = -1;
    private final int partitionField;

    public ConvertToStratifiedSample(int partitionField, int keyField) {
        this.keyField = keyField;
        this.partitionField = partitionField;
    }

    public ConvertToStratifiedSample(int partitionField) {
        this.partitionField = partitionField;
    }

    @Override
    public void processElement(T value, Context ctx, Collector<Tuple2<String, TimestampedElement>> out) throws Exception {
        if (keyField >= 0) {
            TimestampedElement sample = new TimestampedElement<>(value.getField(keyField), ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
            out.collect(new Tuple2(value.getField(partitionField).toString(), sample));
        } else {
            TimestampedElement<T> sample = new TimestampedElement<>(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
            out.collect(new Tuple2(value.getField(partitionField).toString(), sample));
        }
    }
}
