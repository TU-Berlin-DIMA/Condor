package de.tub.dima.condor.benchmark.sources.utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class SyntecticTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<Integer, Integer, Long>> {
    private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
    private long currentMaxTimestamp;
    private long startTime = System.currentTimeMillis();

    @Override
    public long extractTimestamp(final Tuple3<Integer, Integer, Long> element, final long previousElementTimestamp) {
        long timestamp = element.f2;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

}