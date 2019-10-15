package Synopsis;

import Sampling.SampleElement;
import Sampling.SamplerWithTimestamps;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
import java.util.HashMap;

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
     * @param inputStream   the data stream to build the Synopsis
     * @param windowTime    the size of the time window
     * @param keyField      the field of the tuple to build the Synopsis. Set to -1 to build the Synopsis over the whole tuple.
     * @param synopsisClass the type of Synopsis to be computed
     * @param parameters    the initialization parameters for the Synopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the Synopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);

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
                }).returns(synopsisClass);
        return reduce;
    }


    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a Synopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single Synopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the Synopsis
     * @param windowTime    the size of the time window
     * @param synopsisClass the type of Synopsis to be computed
     * @param parameters    the initialization parameters for the Synopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the Synopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, windowTime, -1, synopsisClass, parameters);
    }

    /**
     * Debug function to print the output of the aggregators.
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a Synopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single Synopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the Synopsis
     * @param windowTime    the size of the time window
     * @param keyField      the field of the tuple to build the Synopsis. Set to -1 to build the Synopsis over the whole tuple.
     * @param synopsisClass the type of Synopsis to be computed
     * @param parameters    the initialization parameters for the Synopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the Synopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> debugAggimeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);

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
                }).returns(synopsisClass);
        return reduce;
    }

    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a Synopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single Synopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the Synopsis
     * @param windowSize    the size of the count window
     * @param keyField      the field of the tuple to build the Synopsis. Set to -1 to build the Synopsis over the whole tuple.
     * @param synopsisClass the type of Synopsis to be computed
     * @param parameters    the initialization parameters for the Synopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the Synopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismTuple())
                .keyBy(0)
                .countWindow(windowSize / parallelism)
                .aggregate(agg)
                .countWindowAll(parallelism)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public Synopsis reduce(Synopsis value1, Synopsis value2) throws Exception {
                        return value1.merge(value2);
                    }
                }).returns(synopsisClass);
        return reduce;
    }

    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a Synopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single Synopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the Synopsis
     * @param windowSize    the size of the count window
     * @param synopsisClass the type of Synopsis to be computed
     * @param parameters    the initialization parameters for the Synopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the Synopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, Class<S> synopsisClass, Object... parameters) {
        return countBased(inputStream, windowSize, -1, synopsisClass, parameters);
    }


    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> sampleTimeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);
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
                }).returns(synopsisClass);
        return reduce;
    }

    public static <T, S extends Synopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            KeyedStream<Tuple2<Integer, SampleElement<T>>, Tuple> keyedStream = inputStream.process(new ConvertToSample<>()).map(new AddParallelismTuple<>()).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, SampleElement<T>>, S> processingFunction =
                    new KeyedScottyWindowOperator<>(new SynopsisFunction(keyField, synopsisClass, parameters));
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction).flatMap(new MergePreAggregates());
        } else {
            KeyedStream<Tuple2<Integer, T>, Tuple> keyedStream = inputStream.map(new AddParallelismTuple<>()).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, T>, S> processingFunction;
            if (InvertibleSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(keyField, synopsisClass, parameters));
            } else {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new SynopsisFunction(keyField, synopsisClass, parameters));
            }
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction).flatMap(new MergePreAggregates());
        }
    }


    /**
     * Integer state for Stateful Functions
     */
    public static class IntegerState implements ValueState<Integer> {
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
     * Integer state for Stateful Functions
     */
    public static class WindowState implements ValueState<HashMap<WindowID, Tuple2<Integer, AggregateWindow<Synopsis>>>> {
        HashMap<WindowID, Tuple2<Integer, AggregateWindow<Synopsis>>> openWindows;
        int numberKeys;

        public WindowState(int numberKeys) {
            this.numberKeys = numberKeys;
            this.openWindows = new HashMap<>();
        }

        @Override
        public HashMap value() throws IOException {
            return openWindows;
        }

        @Override
        public void update(HashMap value) throws IOException {
            this.openWindows = value;
        }

        @Override
        public void clear() {
            openWindows.clear();
        }
    }

    public static class WindowID implements Comparable {
        private long start;
        private long end;

        public WindowID(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public int compareTo( Object o) {
            if (o instanceof WindowID) {
                if (((WindowID) o).start > this.start) {
                    return -1;
                } else if (((WindowID) o).start < this.start) {
                    return 1;
                } else if (((WindowID) o).start == this.start && ((WindowID) o).end > this.end) {
                    return -1;
                } else if (((WindowID) o).start == this.start && ((WindowID) o).end < this.end) {
                    return 1;
                } else /*if(((WindowID) o).start == this.start && ((WindowID) o).end == this.end)*/ {
                    return 0;
                }
            }
            throw new IllegalArgumentException("Must be a WindowID to be compared");
        }


        @Override
        public boolean equals(Object o) {
            if (o instanceof WindowID) {
                if (((WindowID) o).start == this.start && ((WindowID) o).end == this.end) {
                    return true;
                }
            }
            return false;
        }
    }


    public static class MergePreAggregates<S extends Synopsis> extends RichFlatMapFunction<AggregateWindow<S>, AggregateWindow<S>> {

        WindowState state;
        int parallelismKeys;

        public MergePreAggregates() {
            this.parallelismKeys = this.getRuntimeContext().getMaxNumberOfParallelSubtasks();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new WindowState(getRuntimeContext().getMaxNumberOfParallelSubtasks());
        }

        @Override
        public void flatMap(AggregateWindow<S> value, Collector<AggregateWindow<S>> out) throws Exception {
            HashMap<WindowID, Tuple2<Integer, AggregateWindow<S>>> openWindows = state.value();
            WindowID windowID = new WindowID(value.getStart(), value.getEnd());
            Tuple2<Integer, AggregateWindow<S>> synopsisAggregateWindow = openWindows.get(windowID);
            if (synopsisAggregateWindow == null) {
                openWindows.put(windowID, new Tuple2<>(1, value));
            } else if (synopsisAggregateWindow.f0 == parallelismKeys - 1) {
                synopsisAggregateWindow.f1.getAggValues().addAll(value.getAggValues());
                out.collect(synopsisAggregateWindow.f1);
                openWindows.remove(windowID);
            } else {
                synopsisAggregateWindow.f1.getAggValues().addAll(value.getAggValues());
                synopsisAggregateWindow.f0 += 1;
            }
            state.update(openWindows);
        }

    }

    /**
     * Stateful map functions to add the parallelism variable
     *
     * @param <T0> type of input elements
     */
    public static class AddParallelismTuple<T0> extends RichMapFunction<T0, Tuple2<Integer, T0>> {

        ValueState<Integer> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new IntegerState();
        }

        @Override
        public Tuple2<Integer, T0> map(T0 value) throws Exception {
            Tuple2 newTuple = new Tuple2<Integer, T0>();
            int currentNode = state.value();
            int next = currentNode + 1;
            next = next % this.getRuntimeContext().getNumberOfParallelSubtasks();
            state.update(next);

            newTuple.setField(currentNode, 0);
            newTuple.setField(value, 1);

            return newTuple;
        }
    }


    public static class ConvertToSample<T>
            extends ProcessFunction<T, SampleElement<T>> {

        @Override
        public void processElement(T value, Context ctx, Collector<SampleElement<T>> out) throws Exception {
            SampleElement<T> sample = new SampleElement<>(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
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
