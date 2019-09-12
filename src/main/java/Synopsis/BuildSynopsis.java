package Synopsis;

import Sampling.SampleElement;
import Synopsis.Synopsis;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Class to organize the static functions to generate window based Synopses.
 *
 * @author Rudi Poepsel Lemaitre
 */
public final class BuildSynopsis {

    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a Synopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single Synopsis representing the whole window.
     *
     * @param inputStream the data stream to build the Synopsis
     * @param windowTime the size of the time window
     * @param keyField the field of the tuple to build the Synopsis. Set to -1 to build the Synopsis over the whole tuple.
     * @param sketchClass the type of Synopsis to be computed
     * @param parameters the initialization parameters for the Synopsis
     * @param <T> the type of the input elements
     * @param <S> the type of the Synopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> sketchClass, Object... parameters){
        SynopsisAggregator agg = new SynopsisAggregator(sketchClass, parameters, keyField);

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismTuple())
                .keyBy(0)
                .timeWindow(windowTime)
                .aggregate(agg)
                .timeWindowAll(windowTime)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public Synopsis reduce(Synopsis value1, Synopsis value2) throws Exception {
                        Synopsis merged = value1.merge(value2);
                        return merged;
                    }
                }).returns(sketchClass);
        return reduce;
    }


    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a Synopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single Synopsis representing the whole window.
     *
     * @param inputStream the data stream to build the Synopsis
     * @param windowTime the size of the time window
     * @param sketchClass the type of Synopsis to be computed
     * @param parameters the initialization parameters for the Synopsis
     * @param <T> the type of the input elements
     * @param <S> the type of the Synopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Class<S> sketchClass, Object... parameters){
        return timeBased(inputStream, windowTime, -1, sketchClass, parameters);
    }

    /**
     * Debug function to print the output of the aggregators.
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a Synopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single Synopsis representing the whole window.
     *
     * @param inputStream the data stream to build the Synopsis
     * @param windowTime the size of the time window
     * @param keyField the field of the tuple to build the Synopsis. Set to -1 to build the Synopsis over the whole tuple.
     * @param sketchClass the type of Synopsis to be computed
     * @param parameters the initialization parameters for the Synopsis
     * @param <T> the type of the input elements
     * @param <S> the type of the Synopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> debugAggimeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> sketchClass, Object... parameters){
        SynopsisAggregator agg = new SynopsisAggregator(sketchClass, parameters, keyField);

        SingleOutputStreamOperator reduce1 = inputStream
                .map(new AddParallelismTuple())
                .keyBy(0)
                .timeWindow(windowTime)
                .aggregate(agg);
        reduce1.writeAsText("output/aggregators", FileSystem.WriteMode.OVERWRITE);
        SingleOutputStreamOperator reduce = reduce1
                .timeWindowAll(windowTime)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public Synopsis reduce(Synopsis value1, Synopsis value2) throws Exception {
                        Synopsis merged = value1.merge(value2);
                        return merged;
                    }
                }).returns(sketchClass);
        return reduce;
    }

    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a Synopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single Synopsis representing the whole window.
     *
     * @param inputStream the data stream to build the Synopsis
     * @param windowSize the size of the count window
     * @param keyField the field of the tuple to build the Synopsis. Set to -1 to build the Synopsis over the whole tuple.
     * @param sketchClass the type of Synopsis to be computed
     * @param parameters the initialization parameters for the Synopsis
     * @param <T> the type of the input elements
     * @param <S> the type of the Synopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, int keyField, Class<S> sketchClass, Object... parameters){
        SynopsisAggregator agg = new SynopsisAggregator(sketchClass, parameters, keyField);
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismTuple())
                .keyBy(0)
                .countWindow(windowSize/parallelism)
                .aggregate(agg)
                .countWindowAll(parallelism)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public Synopsis reduce(Synopsis value1, Synopsis value2) throws Exception {
                        return value1.merge(value2);
                    }
                }).returns(sketchClass);
        return reduce;
    }

    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a Synopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single Synopsis representing the whole window.
     *
     * @param inputStream the data stream to build the Synopsis
     * @param windowSize the size of the count window
     * @param sketchClass the type of Synopsis to be computed
     * @param parameters the initialization parameters for the Synopsis
     * @param <T> the type of the input elements
     * @param <S> the type of the Synopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, Class<S> sketchClass, Object... parameters){
        return countBased(inputStream, windowSize, -1, sketchClass, parameters);
    }



    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> sampleTimeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> sketchClass, Object... parameters){
        SynopsisAggregator agg = new SynopsisAggregator(sketchClass, parameters, keyField);
        SingleOutputStreamOperator reduce1 = inputStream
                .process(new ConvertToSample())
                .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                .map(new AddParallelismTuple())
                .keyBy(0)
                .timeWindow(windowTime)
                .aggregate(agg);
        reduce1.writeAsText("output/aggregators", FileSystem.WriteMode.OVERWRITE);
        SingleOutputStreamOperator reduce = reduce1.timeWindowAll(windowTime)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public Synopsis reduce(Synopsis value1, Synopsis value2) throws Exception {
                        return value1.merge(value2);
                    }
                }).returns(sketchClass);
        return reduce;
    }


    /**
     * Integer state for Stateful Functions
     */
    public static class IntegerState implements ValueState<Integer>{
        int value;

        @Override
        public Integer value() throws IOException {
            return value;
        }

        @Override
        public void update(Integer value) throws IOException {
            this.value = value;
        }

        @Override
        public void clear() {
            value = 0;
        }
    }


    /**
     * Stateful map functions to add the parallelism variable
     *
     * @param <T0> type of input elements
     */
    public static class AddParallelismTuple<T0> extends RichMapFunction<T0, Tuple2<Integer,T0>> {

        ValueState<Integer> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new IntegerState();
        }

        @Override
        public Tuple2<Integer,T0> map(T0 value) throws Exception {
            Tuple2 newTuple = new Tuple2<Integer,T0>();
            int currentNode = state.value();
            int next = currentNode +1;
            next = next % this.getRuntimeContext().getNumberOfParallelSubtasks();
            state.update(next);

            newTuple.setField(currentNode,0);
            newTuple.setField(value,1);

            return newTuple;
        }
    }

    public static class ConvertToSample<T>
            extends ProcessFunction<T, SampleElement> {

        /**
         * Process one element from the input stream.
         *
         * <p>This function can output zero or more elements using the {@link Collector} parameter
         * and also update internal state or set timers using the {@link Context} parameter.
         *
         * @param value The input value.
         * @param ctx   A {@link Context} that allows querying the timestamp of the element and getting
         *              a {@link TimerService} for registering timers and querying the time. The
         *              context is only valid during the invocation of this method, do not store it.
         * @param out   The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */
        @Override
        public void processElement(T value, Context ctx, Collector<SampleElement> out) throws Exception {
            SampleElement<T> sample = new SampleElement<>(value, ctx.timestamp());
            out.collect(sample);
        }
    }

    /**
     * The Custom TimeStampExtractor which is used to assign Timestamps and Watermarks for our data
     */
    public static class SampleTimeStampExtractor implements AssignerWithPunctuatedWatermarks<SampleElement> {
        /**
         * Asks this implementation if it wants to emit a watermark. This method is called right after
         * the {@link #extractTimestamp(SampleElement, long)}   method.
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
        public Watermark checkAndGetNextWatermark(SampleElement lastElement, long extractedTimestamp) {
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
        public long extractTimestamp(SampleElement element, long previousElementTimestamp) {
            return element.getTimeStamp();
        }
    }


}