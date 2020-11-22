package de.tub.dima.condor.benchmark.sources.queries;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class IPQuerySource extends RichParallelSourceFunction<Integer> {

    private final long runtime;
    private final int throughput;
    private final long wait;

    public IPQuerySource(Time queryRuntime, int throughput, Time wait) {
        this.runtime = queryRuntime.toMilliseconds();
        this.throughput = throughput;
        this.wait = wait.toMilliseconds();
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        Random random = new Random();

        long startTs = System.currentTimeMillis();
        long endTs = startTs + runtime + wait;

        while (System.currentTimeMillis() < startTs + wait) {
            // active waiting
        }

        while (System.currentTimeMillis() < endTs){

            long time = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                ctx.collectWithTimestamp(random.nextInt(	2147483647), time);
            }

            while (System.currentTimeMillis() < time + 1000) {
                // active waiting
            }
        }
    }

    @Override
    public void cancel() { }
}
