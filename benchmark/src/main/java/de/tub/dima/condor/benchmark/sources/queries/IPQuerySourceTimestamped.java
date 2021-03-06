package de.tub.dima.condor.benchmark.sources.queries;

import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.TimestampedQuery;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class IPQuerySourceTimestamped extends RichParallelSourceFunction<TimestampedQuery<Integer>> {

    private final long runtime; // in millis
    private final int throughput;
    private final long wait; // in millis
    private final int synopsisRuntime; // in seconds

    public IPQuerySourceTimestamped(Time queryRuntime, int throughput, Time wait, Time synopsisRuntime) {
        this.wait = wait.toMilliseconds();
        this.runtime = queryRuntime.toMilliseconds();
        this.throughput = throughput;
        this.synopsisRuntime = (int)(synopsisRuntime.toMilliseconds() / 1000);
    }

    @Override
    public void run(SourceContext<TimestampedQuery<Integer>> ctx) throws Exception {
        Random random = new Random();

        long startTs = System.currentTimeMillis();
        long endTs = startTs + wait + runtime;

        while (System.currentTimeMillis() < startTs + wait) {
            // active waiting
        }

        while (System.currentTimeMillis() < endTs){

            long time = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                int relativeTimestamp = random.nextInt(synopsisRuntime); // upper bound in seconds from job start
                long query_timestamp = startTs + relativeTimestamp * 1000;
                ctx.collectWithTimestamp(new TimestampedQuery<Integer>(random.nextInt(	2147483647), query_timestamp), time);
            }

            while (System.currentTimeMillis() < time + 1000) {
                // active waiting
            }
        }
    }

    @Override
    public void cancel() {

    }
}