package de.tub.dima.condor.flinkScottyConnector.processor.utils.sampling;

import de.tub.dima.condor.core.synopsis.Sampling.TimestampedElement;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class StratifiedSampleTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple2<Object, TimestampedElement>> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Tuple2<Object, TimestampedElement> lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp);
    }


    @Override
    public long extractTimestamp(Tuple2<Object, TimestampedElement> element, long previousElementTimestamp) {
        return element.f1.getTimeStamp();
    }
}