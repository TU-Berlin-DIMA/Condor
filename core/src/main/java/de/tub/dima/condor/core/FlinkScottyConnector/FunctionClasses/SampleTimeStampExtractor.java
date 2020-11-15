package de.tub.dima.condor.core.FlinkScottyConnector.FunctionClasses;

import de.tub.dima.condor.core.Synopsis.Sampling.TimestampedElement;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * The Custom TimeStampExtractor which is used to assign Timestamps and Watermarks for our data
 */
public class SampleTimeStampExtractor implements AssignerWithPunctuatedWatermarks<TimestampedElement> {
    /**
     * Asks this implementation if it wants to emit a watermark. This method is called right after
     * the {@link #extractTimestamp(TimestampedElement, long)}   method.
     *
     * <p>The returned watermark will be emitted only if it is non-null and its timestamp
     * is larger than that of the previously emitted watermark (to preserve the contract of
     * ascending watermarks). If a null value is returned, or the timestamp of the returned
     * watermark is smaller than that of the last emitted one, then no new watermark will
     * be generated.
     *
     * <p>For an example how to use this method, see the documentation of
     * {@link AssignerWithPunctuatedWatermarks this class}.
     *
     * @param lastElement
     * @param extractedTimestamp
     * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
     */
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(TimestampedElement lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp);
    }

    /**
     * Assigns a timestamp to an element, in milliseconds since the Epoch.
     *
     * <p>The method is passed the previously assigned timestamp of the element.
     * That previous timestamp may have been assigned from a previous assigner,
     * by ingestion time. If the element did not carry a timestamp before, this value is
     * {@code Long.MIN_VALUE}.
     *
     * @param element                  The element that the timestamp will be assigned to.
     * @param previousElementTimestamp The previous internal timestamp of the element,
     *                                 or a negative value, if no timestamp has been assigned yet.
     * @return The new timestamp.
     */
    @Override
    public long extractTimestamp(TimestampedElement element, long previousElementTimestamp) {
        return element.getTimeStamp();
    }
}
