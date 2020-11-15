package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.Sampling.TimestampedElement;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ConvertToSample<T>
        extends ProcessFunction<T, TimestampedElement> {
    private int keyField = -1;

    public ConvertToSample(int keyField) {
        this.keyField = keyField;
    }

    public ConvertToSample() {
    }

    @Override
    public void processElement(T value, Context ctx, Collector<TimestampedElement> out) throws Exception {
        if (keyField >= 0 && value instanceof Tuple) {
            TimestampedElement sample = new TimestampedElement<>(((Tuple) value).getField(keyField), ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
            out.collect(sample);
        } else {
            TimestampedElement<T> sample = new TimestampedElement<>(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
            out.collect(sample);
        }
    }
}
