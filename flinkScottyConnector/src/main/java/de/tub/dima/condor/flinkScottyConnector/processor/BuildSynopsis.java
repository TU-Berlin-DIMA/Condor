package de.tub.dima.condor.flinkScottyConnector.processor;

import de.tub.dima.condor.core.synopsis.*;
import de.tub.dima.condor.core.synopsis.Sampling.SamplerWithTimestamps;
import de.tub.dima.condor.core.synopsis.Sampling.TimestampedElement;
import de.tub.dima.condor.flinkScottyConnector.processor.compute.flink.NonMergeableSynopsisAggregator;
import de.tub.dima.condor.flinkScottyConnector.processor.compute.flink.SynopsisAggregator;
import de.tub.dima.condor.flinkScottyConnector.processor.compute.scotty.CommutativeSynopsisFunction;
import de.tub.dima.condor.flinkScottyConnector.processor.compute.scotty.InvertibleSynopsisFunction;
import de.tub.dima.condor.flinkScottyConnector.processor.compute.scotty.NonMergeableSynopsisFunction;
import de.tub.dima.condor.flinkScottyConnector.processor.compute.scotty.SynopsisFunction;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Class to organize the static functions to generate window based Synopses.
 *
 * @author Rudi Poepsel Lemaitre
 */
public final class BuildSynopsis {

    private static int parallelismKeys = -1;

    public static void setParallelismKeys(int newParallelismKeys) {
        parallelismKeys = newParallelismKeys;
        System.out.println("BuildSynopsis Parallelism Keys changed to: "+parallelismKeys);
    }

    public static int getParallelismKeys(){
        return parallelismKeys;
    }

    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowTime    the size of the time window
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters);

        DataStream<T> rescaled = inputStream.rescale();
        KeyedStream keyBy;
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            keyBy = rescaled
                    .process(new ConvertToSample(keyField))
                    .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                    .map(new AddParallelismIndex(-1))
                    .keyBy(0);
        } else {
            keyBy = rescaled
                    .map(new AddParallelismIndex(keyField))
                    .keyBy(0);
        }

        WindowedStream windowedStream;
        if (slideTime == null) {
            windowedStream = keyBy.timeWindow(windowTime);
        } else {
            windowedStream = keyBy.timeWindow(windowTime, slideTime);
        }

        SingleOutputStreamOperator preAggregated = windowedStream
                .aggregate(agg);

        AllWindowedStream allWindowedStream;
        if (slideTime == null) {
            allWindowedStream = preAggregated.timeWindowAll(windowTime);
        } else {
            allWindowedStream = preAggregated.timeWindowAll(windowTime, slideTime);
        }

        SingleOutputStreamOperator result = allWindowedStream.reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
            @Override
            public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
                MergeableSynopsis merged = value1.merge(value2);
                return merged;
            }
        }).returns(synopsisClass);
        return result;
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, windowTime, null, -1, synopsisClass, parameters);
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, windowTime, slideTime, -1, synopsisClass, parameters);
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, windowTime, null, keyField, synopsisClass, parameters);
    }

    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowSize    the size of the count window
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, long slideSize, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters);
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        KeyedStream keyBy;
        DataStream<T> rescaled = inputStream.rescale();
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            keyBy = rescaled
                    .process(new ConvertToSample(keyField))
                    .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                    .map(new AddParallelismIndex(-1))
                    .keyBy(0);
        } else {
            keyBy = rescaled
                    .map(new AddParallelismIndex(keyField))
                    .keyBy(0);
        }
        WindowedStream windowedStream;
        if (slideSize == -1) {
            windowedStream = keyBy.countWindow(windowSize / parallelism);
        } else {
            windowedStream = keyBy.countWindow(windowSize / parallelism, slideSize / parallelism);
        }

        SingleOutputStreamOperator preAggregated = windowedStream
                .aggregate(agg);

        SingleOutputStreamOperator result = preAggregated.flatMap(new MergeCountPreAggregates(getParallelismKeys())).returns(synopsisClass);
        return result;
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, int keyField, Class<S> synopsisClass, Object... parameters) {
        return countBased(inputStream, windowSize, -1, keyField, synopsisClass, parameters);
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, Class<S> synopsisClass, Object... parameters) {
        return countBased(inputStream, windowSize, -1, synopsisClass, parameters);
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, long slideSize, Class<S> synopsisClass, Object... parameters) {
        return countBased(inputStream, windowSize, slideSize, -1, synopsisClass, parameters);
    }



    // NON-MERGEABLE !!!
    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> timeBased
            (DataStream<T> inputStream, int miniBatchSize, Time windowTime, Time slideTime, int keyField, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(synopsisClass, parameters);
        KeyedStream keyBy = inputStream
                .process(new OrderAndIndex(keyField, miniBatchSize, getParallelismKeys())).setParallelism(1)
                .keyBy(0);

        WindowedStream windowedStream;
        if (slideTime == null) {
            windowedStream = keyBy.timeWindow(windowTime);
        } else {
            windowedStream = keyBy.timeWindow(windowTime, slideTime);
        }

        SingleOutputStreamOperator preAggregated = windowedStream
                .aggregate(agg);

        AllWindowedStream allWindowedStream;
        if (slideTime == null) {
            allWindowedStream = preAggregated.timeWindowAll(windowTime);
        } else {
            allWindowedStream = preAggregated.timeWindowAll(windowTime, slideTime);
        }

        SingleOutputStreamOperator returns = allWindowedStream.aggregate(new NonMergeableSynopsisUnifier(managerClass))
                .returns(managerClass);
        return returns;
    }

    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> timeBased
            (DataStream<T> inputStream, Time windowTime, Time slideTime, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return timeBased(inputStream, 0, windowTime, slideTime, -1, synopsisClass, managerClass, parameters);
    }

    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> timeBased
            (DataStream<T> inputStream, int miniBatchSize, Time windowTime, Time slideTime, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return timeBased(inputStream, miniBatchSize, windowTime, slideTime, -1, synopsisClass, managerClass, parameters);
    }

    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> timeBased
            (DataStream<T> inputStream, Time windowTime, Time slideTime, int keyField, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return timeBased(inputStream, 0, windowTime, slideTime, keyField, synopsisClass, managerClass, parameters);
    }

    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> timeBased
            (DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return timeBased(inputStream, 0, windowTime, null, keyField, synopsisClass, managerClass, parameters);
    }

    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> timeBased
            (DataStream<T> inputStream, Time windowTime, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return timeBased(inputStream, 0, windowTime, null, -1, synopsisClass, managerClass, parameters);
    }


    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowSize    the size of the count window
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> countBased(DataStream<T> inputStream, int miniBatchSize, long windowSize, long slideSize, int keyField, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(synopsisClass, parameters);
        KeyedStream keyBy = inputStream
                .process(new OrderAndIndex(keyField, miniBatchSize, getParallelismKeys())).setParallelism(1)
                .keyBy(0);

        WindowedStream windowedStream;
        if (slideSize == -1) {
            windowedStream = keyBy.countWindow(windowSize / parallelism);
        } else {
            windowedStream = keyBy.countWindow(windowSize / parallelism, slideSize / parallelism);
        }

        SingleOutputStreamOperator preAggregated = windowedStream
                .aggregate(agg);

        SingleOutputStreamOperator returns = preAggregated.flatMap(new NonMergeableSynopsisCountUnifier(managerClass, getParallelismKeys()))
                .returns(managerClass);
        return returns;
    }

    public static <T, S extends MergeableSynopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> countBased(DataStream<T> inputStream, int miniBatchSize, long windowSize, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return countBased(inputStream, miniBatchSize, windowSize, -1, -1, synopsisClass, managerClass, parameters);
    }

    public static <T, S extends MergeableSynopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> countBased(DataStream<T> inputStream, long windowSize, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return countBased(inputStream, 0, windowSize, -1, -1, synopsisClass, managerClass, parameters);
    }

    public static <T, S extends MergeableSynopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> countBased(DataStream<T> inputStream, long windowSize, long slideSize, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return countBased(inputStream, 0, windowSize, slideSize, -1, synopsisClass, managerClass, parameters);
    }


    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, int keyField, Class<S> synopsisClass, Object... parameters) {
        DataStream<T> rescaled = inputStream.rescale();
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            KeyedStream<Tuple2<Integer, Object>, Tuple> keyedStream = rescaled.process(new ConvertToSample<>(keyField)).map(new AddParallelismIndex<>(-1)).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Object>, S> processingFunction =
                    new KeyedScottyWindowOperator<>(new SynopsisFunction(synopsisClass, parameters));
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction)
                    .flatMap(new MergePreAggregates(getParallelismKeys()))
                    .setParallelism(1);
        } else {
            KeyedStream<Tuple2<Integer, Object>, Tuple> keyedStream = rescaled.map(new AddParallelismIndex<>(keyField)).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Object>, S> processingFunction;
            if (InvertibleSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(synopsisClass, parameters));
            } else if (CommutativeSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new CommutativeSynopsisFunction(synopsisClass, parameters));
            } else {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new SynopsisFunction(synopsisClass, parameters));
            }
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction)
                    .flatMap(new MergePreAggregates(getParallelismKeys()))
                    .setParallelism(1);
        }
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, Class<S> synopsisClass, Object... parameters) {
        return scottyWindows(inputStream, windows, -1, synopsisClass, parameters);
    }

    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends Synopsis, SM extends NonMergeableSynopsisManager, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<AggregateWindow<M>> scottyWindows(DataStream<T> inputStream, int miniBatchSize, Window[] windows, int keyField, Class<S> synopsisClass, Class<SM> sliceManagerClass, Class<M> managerClass, Object... parameters) {

        KeyedStream<Tuple2<Integer, Object>, Tuple> keyedStream = inputStream
                .process(new OrderAndIndex(keyField, miniBatchSize, getParallelismKeys())).setParallelism(1)
                .keyBy(0);

        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Object>, NonMergeableSynopsisManager> processingFunction =
                new KeyedScottyWindowOperator<>(new NonMergeableSynopsisFunction(keyField, -1, synopsisClass, sliceManagerClass, parameters));

        for (int i = 0; i < windows.length; i++) {
            processingFunction.addWindow(windows[i]);
        }
        return keyedStream.process(processingFunction)
                .flatMap(new UnifyToManager<M>(managerClass))
                .setParallelism(1);
    }


    /**
     * Integer state for Stateful Functions
     */
    public static class IntegerState implements ValueState<Integer>, Serializable {
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

        private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            out.writeInt(value);
        }


        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            this.value = in.readInt();
        }

        private void readObjectNoData() throws ObjectStreamException {
            throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
        }
    }

    /**
     * Integer state for Stateful Functions
     */
    public static class WindowState implements ValueState<HashMap<WindowID, Tuple2<Integer, AggregateWindow<MergeableSynopsis>>>> {
        HashMap<WindowID, Tuple2<Integer, AggregateWindow<MergeableSynopsis>>> openWindows;
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
        public int compareTo(Object o) {
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

        @Override
        public int hashCode() {
            int result = (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (end ^ (end >>> 32));
            return result;
        }
    }


    public static class MergePreAggregates<S extends MergeableSynopsis> extends RichFlatMapFunction<AggregateWindow<S>, AggregateWindow<S>> {

        WindowState state;
        int parKeys;

        public MergePreAggregates(int parKeys) {
            this.parKeys = parKeys;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new WindowState(parKeys);
        }

        @Override
        public void flatMap(AggregateWindow<S> value, Collector<AggregateWindow<S>> out) throws Exception {
            HashMap<WindowID, Tuple2<Integer, AggregateWindow<S>>> openWindows = state.value();
            WindowID windowID = new WindowID(value.getStart(), value.getEnd());
            Tuple2<Integer, AggregateWindow<S>> synopsisAggregateWindow = openWindows.get(windowID);
            if (synopsisAggregateWindow == null) {
                openWindows.put(windowID, new Tuple2<>(1, value));
            } else if (synopsisAggregateWindow.f0 == parKeys - 1) {
                synopsisAggregateWindow.f1.getAggValues().get(0).merge(value.getAggValues().get(0));
                out.collect(synopsisAggregateWindow.f1);
                openWindows.remove(windowID);
            } else {
                synopsisAggregateWindow.f1.getAggValues().get(0).merge(value.getAggValues().get(0));
                synopsisAggregateWindow.f0 += 1;
            }
            state.update(openWindows);
        }

    }


    public static class MergeCountPreAggregates<S extends MergeableSynopsis> extends RichFlatMapFunction<S, S> {

        private int count;
        private S currentMerged;

        int parKeys;

        public MergeCountPreAggregates(int parKeys) {
            this.parKeys = parKeys;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            count = 0;
        }

        @Override
        public void flatMap(S value, Collector<S> out) throws Exception {
            if (count < parKeys-1) {
                if (count == 0) {
                    currentMerged = value;
                } else {
                    currentMerged.merge(value);
                }
                count++;
            } else {
                count = 0;
                currentMerged.merge(value);
                out.collect(currentMerged);
            }
        }

    }


    public static class UnifyToManager<M extends NonMergeableSynopsisManager> extends RichFlatMapFunction<AggregateWindow<NonMergeableSynopsisManager>, AggregateWindow<M>> {

        WindowState state;
        Class<M> managerClass;
        int parKeys;

        public UnifyToManager(int parKeys) {
            this.parKeys = parKeys;
        }

        public UnifyToManager(Class<M> managerClass) {
            this.managerClass = managerClass;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new WindowState(parKeys);
        }

        @Override
        public void flatMap(AggregateWindow<NonMergeableSynopsisManager> value, Collector<AggregateWindow<M>> out) throws Exception {
            HashMap<WindowID, Tuple2<Integer, AggregateWindow<M>>> openWindows = state.value();
            WindowID windowID = new WindowID(value.getStart(), value.getEnd());
            Tuple2<Integer, AggregateWindow<M>> synopsisAggregateWindow = openWindows.get(windowID);
            if (synopsisAggregateWindow == null) {
                NonMergeableSynopsisWindowState<M> aggWindow = new NonMergeableSynopsisWindowState(value, managerClass);
                openWindows.put(windowID, new Tuple2<>(1, aggWindow));
            } else if (synopsisAggregateWindow.f0 == parKeys - 1) {
                synopsisAggregateWindow.f1.getAggValues().get(0).addSynopsis(value.getAggValues().get(0));
                out.collect(synopsisAggregateWindow.f1);
                openWindows.remove(windowID);
            } else {
                synopsisAggregateWindow.f1.getAggValues().get(0).addSynopsis(value.getAggValues().get(0));
                synopsisAggregateWindow.f0 += 1;
            }
            state.update(openWindows);
        }

    }

    public static class NonMergeableSynopsisWindowState<M extends NonMergeableSynopsisManager> implements AggregateWindow<M> {
        private final long start;
        private final long endTs;
        private final WindowMeasure measure;
        private List<M> aggValues;

        public NonMergeableSynopsisWindowState(AggregateWindow<Synopsis> aggWindow, Class<M> managerClass) {
            this(aggWindow.getStart(), aggWindow.getEnd(), aggWindow.getMeasure());
            Constructor<M> constructor;
            try {
                constructor = managerClass.getConstructor();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            }
            for (int i = 0; i < aggWindow.getAggValues().size(); i++) {
                M manager;
                try {
                    manager = constructor.newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
                } catch (InvocationTargetException e) {
                    throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
                }
                manager.addSynopsis(aggWindow.getAggValues().get(i));
                aggValues.add(manager);
            }
        }

        public NonMergeableSynopsisWindowState(long start, long endTs, WindowMeasure measure) {
            this.start = start;
            this.endTs = endTs;
            this.measure = measure;
            this.aggValues = new ArrayList<>();
        }

        @Override
        public WindowMeasure getMeasure() {
            return measure;
        }

        @Override
        public long getStart() {
            return start;
        }

        @Override
        public long getEnd() {
            return endTs;
        }

        @Override
        public List<M> getAggValues() {
            return aggValues;
        }

        @Override
        public boolean hasValue() {
            return !aggValues.isEmpty();
        }
    }

    /**
     * Stateful map functions to add the parallelism variable
     *
     * @param <T0> type of input elements
     */
    public static class AddParallelismIndex<T0> extends RichMapFunction<T0, Tuple2<Integer, Object>> {
        public int keyField;
        private Tuple2<Integer, Object> newTuple;


        public AddParallelismIndex(int keyField) {
            this.keyField = keyField;
            newTuple = new Tuple2<Integer, Object>();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (parallelismKeys < 1) {
                setParallelismKeys(this.getRuntimeContext().getNumberOfParallelSubtasks());
            }
        }

        @Override
        public Tuple2<Integer, Object> map(T0 value) throws Exception {
            if (value instanceof Tuple && keyField != -1) {
                newTuple.setField(((Tuple) value).getField(keyField), 1);
            } else {
                newTuple.setField(value, 1);
            }
            newTuple.setField(this.getRuntimeContext().getIndexOfThisSubtask(), 0);
            return newTuple;
        }
    }


    public static class ConvertToSample<T>
            extends ProcessFunction<T, TimestampedElement> {
        private int keyField = -1;

        public ConvertToSample(int keyField) {
            this.keyField = keyField;
        }

        public ConvertToSample() {
        }

        @Override
        public void processElement(T value, Context ctx, Collector<TimestampedElement> out) throws Exception {
            if (keyField >= 0 && value instanceof Tuple) {
                TimestampedElement sample = new TimestampedElement<>(((Tuple) value).getField(keyField), ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
                out.collect(sample);
            } else {
                TimestampedElement<T> sample = new TimestampedElement<>(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
                out.collect(sample);
            }
        }
    }

    /**
     * The Custom TimeStampExtractor which is used to assign Timestamps and Watermarks for our data
     */
    public static class SampleTimeStampExtractor implements AssignerWithPunctuatedWatermarks<TimestampedElement> {
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

    public static class NonMergeableSynopsisCountUnifier<S extends Synopsis> extends RichFlatMapFunction<S, NonMergeableSynopsisManager> {

        private int count;
        private NonMergeableSynopsisManager currentManager;

        private Class<? extends NonMergeableSynopsisManager> managerClass;
        private int parKeys;

        public NonMergeableSynopsisCountUnifier(Class<? extends NonMergeableSynopsisManager> managerClass, int parKeys) {
            this.managerClass = managerClass;
            this.parKeys = parKeys;
        }

        public NonMergeableSynopsisManager newManager() {
            Constructor<? extends NonMergeableSynopsisManager> constructor = null;
            try {
                constructor = managerClass.getConstructor();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            }
            NonMergeableSynopsisManager manager = null;
            try {
                manager = constructor.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            } catch (IllegalAccessException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            } catch (InvocationTargetException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            }
            return manager;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            currentManager = newManager();
        }

        @Override
        public void flatMap(S value, Collector<NonMergeableSynopsisManager> out) throws Exception {
            if (currentManager.getUnifiedSynopses().size() < parKeys-1) {
                currentManager.addSynopsis(value);
            } else {
                currentManager.addSynopsis(value);
                out.collect(currentManager);
                currentManager.cleanManager();
            }
        }

    }

    public static class NonMergeableSynopsisUnifier<S extends Synopsis> implements AggregateFunction<S, NonMergeableSynopsisManager, NonMergeableSynopsisManager> {
        private Class<? extends NonMergeableSynopsisManager> managerClass;

        public NonMergeableSynopsisUnifier(Class<? extends NonMergeableSynopsisManager> managerClass) {
            this.managerClass = managerClass;
        }

        @Override
        public NonMergeableSynopsisManager createAccumulator() {
            Constructor<? extends NonMergeableSynopsisManager> constructor = null;
            try {
                constructor = managerClass.getConstructor();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            }
            NonMergeableSynopsisManager manager = null;
            try {
                manager = constructor.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            } catch (IllegalAccessException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            } catch (InvocationTargetException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            }
            return manager;
        }

        @Override
        public NonMergeableSynopsisManager add(S value, NonMergeableSynopsisManager accumulator) {
            accumulator.addSynopsis(value);
            return accumulator;
        }

        @Override
        public NonMergeableSynopsisManager getResult(NonMergeableSynopsisManager accumulator) {
            return accumulator;
        }

        @Override
        public NonMergeableSynopsisManager merge(NonMergeableSynopsisManager a, NonMergeableSynopsisManager b) {
            for (int i = 0; i < b.getUnifiedSynopses().size(); i++) {
                a.addSynopsis((S) b.getUnifiedSynopses().get(i));
            }
            return a;
        }
    }

    private static class OrderAndIndex<T0> extends ProcessFunction<T0, Tuple2<Integer, Object>> {
        private int keyField;
        private int miniBatchSize;
        private int parKeys;

        private transient ValueState<Integer> state;
        private Tuple2<Integer, Object> newTuple;

        private PriorityQueue<TimestampedElement> dispatchList;

        public OrderAndIndex(int keyField, int miniBatchSize, int parKeys) {
            this.keyField = keyField;
            this.miniBatchSize = miniBatchSize;
            this.parKeys = parKeys;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (parKeys < 1) {
                throw new IllegalArgumentException("The parallelism for the synopsis construction needs to be set with the BuildSynopsis.setParallelismKeys() method. "+parKeys);
            }
            state = new IntegerState();
            if (miniBatchSize > 1) {
                dispatchList = new PriorityQueue<>();
            }
            newTuple = new Tuple2<>();
        }

        @Override
        public void processElement(T0 value, Context ctx, Collector<Tuple2<Integer, Object>> out) throws Exception {
            if (miniBatchSize > 1) {
                if (value instanceof Tuple && keyField != -1) {
                    dispatchList.add(new TimestampedElement(((Tuple) value).getField(keyField), ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime()));
                } else {
                    dispatchList.add(new TimestampedElement(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime()));
                }

                if (dispatchList.size() == miniBatchSize) {
                    while (!dispatchList.isEmpty()) {
                        int currentNode = state.value();
                        int next = currentNode + 1;
                        next = next % parKeys;
                        state.update(next);

                        Object tupleValue = dispatchList.poll().getValue();

                        newTuple.setField(tupleValue, 1);
                        newTuple.setField(currentNode, 0);
                        out.collect(newTuple);
                    }
                }
            } else {
                int currentNode = state.value();
                int next = currentNode + 1;
                next = next % parKeys;
                state.update(next);

                if (value instanceof Tuple && keyField != -1) {
                    newTuple.setField(((Tuple) value).getField(keyField), 1);
                } else {
                    newTuple.setField(value, 1);
                }
                newTuple.setField(currentNode, 0);
                out.collect(newTuple);
            }
        }
    }
}
