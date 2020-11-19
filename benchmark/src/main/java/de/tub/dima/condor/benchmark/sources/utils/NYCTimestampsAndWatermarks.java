package de.tub.dima.condor.benchmark.sources.utils;

import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class NYCTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> {
    private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
    private long currentMaxTimestamp;
    private long startTime = System.currentTimeMillis();

    @Override
    public long extractTimestamp(final Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> element, final long previousElementTimestamp) {
        long timestamp = getEventTime(element);
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    public long getEventTime(Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> ride) {
        if (ride.f3) {
            return ride.f4;
        } else {
            return ride.f5;
        }
    }
}