package de.tub.dima.condor.flinkScottyConnector.processor.utils.sampling;

import de.tub.dima.condor.core.synopsis.Sampling.TimestampedElement;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class ConvertToStratifiedSample<T, Key extends Serializable> extends ProcessFunction<Tuple2<Key, T>, Tuple2<Key, Object>> {
    @Override
    public void processElement(Tuple2<Key, T> value, Context ctx, Collector<Tuple2<Key, Object>> out) throws Exception {
        TimestampedElement<T> sample = new TimestampedElement<>(value.f1, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
        out.collect(new Tuple2<>(value.f0, sample));
    }
}
