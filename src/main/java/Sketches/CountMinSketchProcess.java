package Sketches;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class CountMinSketchProcess<KEY> extends ProcessWindowFunction<CountMinSketch, Tuple2<Long, CountMinSketch>, KEY, TimeWindow> {
    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param key      The key for which this window is evaluated.
     * @param context  The context in which the window is being evaluated.
     * @param elements The elements in the window being evaluated.
     * @param out      A collector for emitting elements.
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    @Override
    public void process(KEY key, Context context, Iterable<CountMinSketch> elements, Collector<Tuple2<Long, CountMinSketch>> out) throws Exception {
        CountMinSketch cm = elements.iterator().next();
        if (context.window() instanceof TimeWindow){
            out.collect(new Tuple2<>(((TimeWindow) context.window()).getEnd(), cm));
        } else{
            //TODO: Coount Windows?????
        }

    }
}
