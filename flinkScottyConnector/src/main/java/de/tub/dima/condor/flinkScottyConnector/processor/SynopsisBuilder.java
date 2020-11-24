package de.tub.dima.condor.flinkScottyConnector.processor;

import de.tub.dima.condor.flinkScottyConnector.processor.compute.flink.NonMergeableSynopsisAggregator;
import de.tub.dima.condor.flinkScottyConnector.processor.compute.flink.SynopsisAggregator;
import de.tub.dima.condor.flinkScottyConnector.processor.compute.scotty.*;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.condor.core.synopsis.*;
import de.tub.dima.condor.core.synopsis.Sampling.SamplerWithTimestamps;
import de.tub.dima.condor.flinkScottyConnector.processor.divide.OrderAndIndex;
import de.tub.dima.condor.flinkScottyConnector.processor.merge.MergePreAggregates;
import de.tub.dima.condor.flinkScottyConnector.processor.merge.NonMergeableSynopsisUnifier;
import de.tub.dima.condor.flinkScottyConnector.processor.merge.UnifyToManager;
import de.tub.dima.condor.flinkScottyConnector.processor.utils.sampling.ConvertToSample;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Class which facilitates the building of the Data -> Synopsis Pipeline.
 * In order to do so, simply use the build() or buildStratified() methods.
 *
 * @author joschavonhein
 */
public class SynopsisBuilder {

    /**
     * Main function to create a normal (not stratified) synopsis stream. Depending on which of the optional parameters in the BuildConfiguration
     * are set, the pipeline will be constructed differently. See the documentation of the BuildConfiguration for more details.
     *
     * @param env       Execution Environment
     * @param config    Configuration
     * @param <S>       The Synopsis Class
     * @return          The synopsis stream wrapped in the WindowedSynopsis class (contains additional Window Information)
     * @throws Exception
     */
    public static <S extends Synopsis> SingleOutputStreamOperator<WindowedSynopsis<S>> build
            (StreamExecutionEnvironment env, BuildConfiguration config) throws Exception {

        if(config.stratificationKeyExtractor == null){ // Stratified
            if (config.isForceBucketing() && config.windows.length == 1 && config.windows[0] instanceof SlidingWindow){
                return buildSlidingFlink(config);
            } else if(config.windows[0] instanceof TumblingWindow && config.windows.length == 1) {
                return buildFlink(config);
            } else {
                if (MergeableSynopsis.class.isAssignableFrom(config.synopsisClass)){
                    return buildScottyMergeable(config);
                }else {
                    return buildScottyNonMergeable(config);
                }
            }
        } else {
            throw new IllegalArgumentException("for stratified Methods use buildStratified() Method");
        }
    }

    /**
     * Main function to create a stratified synopsis stream. Depending on which of the optional parameters in the BuildConfiguration
     * are set, the pipeline will be constructed differently. See the documentation of the BuildConfiguration for more details.
     *
     * @param env       Execution Environment
     * @param config    Configuration
     * @param <S>       The Synopsis Class
     * @return          The stratified synopsis stream wrapped in the StatifiedSynopsisWrapper
     * @throws Exception
     */
    public static <S extends Synopsis, Key extends Serializable> SingleOutputStreamOperator<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> buildStratified
            (StreamExecutionEnvironment env, BuildConfiguration config) throws Exception {

        if(config.stratificationKeyExtractor != null){ // Stratified
            if(config.windows[0] instanceof TumblingWindow && config.windows.length == 1) {

                return buildFlinkStratified(config);

            } else {

                return buildScottyStratified(config);
            }
        }else{  // Not Stratified
            throw new IllegalArgumentException("Configuration needs a Stratification Key Extractor in order to build stratified synopsis");
        }
    }


    private static <T, S extends Synopsis, Key extends Serializable> SingleOutputStreamOperator<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> buildFlinkStratified(BuildConfiguration config){

        KeyedStream<Tuple2<Key, T>, Tuple> keyBy;
        if(SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)){
            keyBy = config.inputStream
                    .process(new ConvertToSample()).setParallelism(config.parallelism)
                    // .assignTimestampsAndWatermarks(new SampleTimeStampExtractor()) // TODO: potentially needed
                    .map(new AddParallelismIndex()).setParallelism(config.parallelism)
                    .keyBy(0);
        } else {
            keyBy = config.inputStream
                    .map(config.stratificationKeyExtractor).setParallelism(config.parallelism)
                    .keyBy(0);
        }


        TumblingWindow window = (TumblingWindow) config.windows[0];
        WindowedStream windowedStream = window.getWindowMeasure() == WindowMeasure.Count ? keyBy.countWindow(window.getSize()) : keyBy.timeWindow(Time.milliseconds(window.getSize()));

        AggregateFunction aggregateFunction = MergeableSynopsis.class.isAssignableFrom(config.synopsisClass)
                ? new SynopsisAggregator(true, config.synopsisClass, config.synParams)
                : new NonMergeableSynopsisAggregator(true, config.synopsisClass, config.synParams);

        return windowedStream
                .aggregate(aggregateFunction, new WindowFunction<S, StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>,Key, TimeWindow>() {
                    @Override
                    public void apply(Key key, TimeWindow window, Iterable<S> values, Collector<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> out) throws Exception {
                        values.forEach(new Consumer<S>() {
                            @Override
                            public void accept(S synopsis) {
                                WindowedSynopsis windowedSynopsis = new WindowedSynopsis<S>(synopsis, window.getStart(), window.getEnd());
                                out.collect(new StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>(key, windowedSynopsis));
                            }
                        });
                    }
                }).setParallelism(config.parallelism).returns(new TypeHint<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>>() {});
    }

    private static <S extends Synopsis, Key extends Serializable, Value> SingleOutputStreamOperator<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> buildScottyStratified
            (BuildConfiguration config) throws Exception {

        if (!StratifiedSynopsis.class.isAssignableFrom(config.synopsisClass)){
            throw new Exception("Synopsis needs to extend from StratifiedSynopsis Abstract Class");
        }

        KeyedStream keyedStream;
        KeyedScottyWindowOperator<Tuple, Tuple2<Key, Value>, S> processingFunction;

        if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)){
            keyedStream = config.inputStream
                    .process(new ConvertToSample()).setParallelism(config.parallelism)
                    .map(new AddParallelismIndex()).setParallelism(config.parallelism)
                    .keyBy(0);

            processingFunction = new KeyedScottyWindowOperator<>(new SynopsisFunction(true, config.synopsisClass, config.synParams));
        }else{
            keyedStream = config.inputStream
                    .map(config.stratificationKeyExtractor).setParallelism(config.parallelism)
                    .keyBy(0);

            if (!MergeableSynopsis.class.isAssignableFrom(config.synopsisClass)){ // Non-Mergeable Synopsis!

                processingFunction = new KeyedScottyWindowOperator<>(new StratifiedNonMergeableSynopsisFunction(config.synopsisClass, config.sliceManagerClass, config.synParams));
            } else if (InvertibleSynopsis.class.isAssignableFrom(config.synopsisClass)) {

                processingFunction = new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(true, config.synopsisClass, config.synParams));
            } else if (CommutativeSynopsis.class.isAssignableFrom(config.synopsisClass)) {

                processingFunction = new KeyedScottyWindowOperator<>(new CommutativeSynopsisFunction(true, config.synopsisClass, config.synParams));
            } else {

                processingFunction = new KeyedScottyWindowOperator<>(new SynopsisFunction(true, config.synopsisClass, config.synParams));
            }
        }

        for (Window window : config.windows) {
            processingFunction.addWindow(window);
        }

        return keyedStream
                .process(processingFunction).setParallelism(config.parallelism)
                .map(new MapFunction<AggregateWindow<S>, StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>>() {
                    @Override
                    public StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>> map(AggregateWindow<S> value) throws Exception {
                        StratifiedSynopsis<Key> stratifiedSynopsis = (StratifiedSynopsis) value.getAggValues().get(0);
                        return new StratifiedSynopsisWrapper(stratifiedSynopsis.getPartitionValue(), new WindowedSynopsis(value.getAggValues().get(0), value.getStart(), value.getEnd()));
                    }
                }).setParallelism(config.parallelism);

    }

    private static <T, S extends Synopsis> SingleOutputStreamOperator<WindowedSynopsis<S>> buildFlink(BuildConfiguration config){

        boolean mergeable = MergeableSynopsis.class.isAssignableFrom(config.synopsisClass);

        DataStream<T> rescaled = config.inputStream.rescale();
        KeyedStream keyBy;
        if(!mergeable){

            // Non-Mergeable with miniBatchSize (!)
            keyBy = config.inputStream
                    .process(new OrderAndIndex(config.miniBatchSize, config.parallelism)).setParallelism(1)
                    .keyBy(0);

        } else if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)) {
            keyBy = rescaled
                    .process(new ConvertToSample()).setParallelism(config.parallelism)
                    // .assignTimestampsAndWatermarks(new SampleTimeStampExtractor()) // TODO: potentially needed
                    .map(new AddParallelismIndex()).setParallelism(config.parallelism)
                    .keyBy(0);
        } else {
            keyBy = rescaled
                    .map(new AddParallelismIndex()).setParallelism(config.parallelism)
                    .keyBy(0);
        }

        TumblingWindow window = (TumblingWindow) config.windows[0];
        WindowedStream windowedStream = window.getWindowMeasure() == WindowMeasure.Count
                ? keyBy.countWindow(window.getSize() / config.parallelism)
                : keyBy.timeWindow(Time.milliseconds(window.getSize()));


        AggregateFunction synopsisFunction = mergeable
                ? new SynopsisAggregator(config.synopsisClass, config.synParams)
                : new NonMergeableSynopsisAggregator(config.miniBatchSize, config.synopsisClass, config.synParams);


        SingleOutputStreamOperator preAggregated = windowedStream.aggregate(synopsisFunction).setParallelism(config.parallelism);


        AllWindowedStream partialAggregate = window.getWindowMeasure() == WindowMeasure.Count
                ? preAggregated.countWindowAll(config.parallelism)
                : preAggregated.timeWindowAll(Time.milliseconds(window.getSize()));


        if (mergeable) {
            return partialAggregate.reduce(new MergeSynopsis(), new AddWindowTimes())
                    .returns(new TypeHint<WindowedSynopsis<S>>() {});
        } else {
            return partialAggregate.aggregate(new NonMergeableSynopsisUnifier(config.managerClass), new AddWindowTimes())
                    .returns(new TypeHint<WindowedSynopsis<NonMergeableSynopsisManager>>() {});
        }

    }

    private static <T, S extends Synopsis> SingleOutputStreamOperator<WindowedSynopsis<S>> buildSlidingFlink(BuildConfiguration config){

        boolean mergeable = MergeableSynopsis.class.isAssignableFrom(config.synopsisClass);

        DataStream<T> rescaled = config.inputStream.rescale();
        KeyedStream keyBy;
        if(!mergeable){
            // Non-Mergeable with miniBatchSize (!)
            keyBy = config.inputStream.process(new OrderAndIndex(config.miniBatchSize, config.parallelism)).setParallelism(1)
                    .keyBy(0);

        } else if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)) {
            keyBy = rescaled.process(new ConvertToSample()).setParallelism(config.parallelism)
                    // .assignTimestampsAndWatermarks(new SampleTimeStampExtractor()) // TODO: potentially needed
                    .map(new AddParallelismIndex()).setParallelism(config.parallelism)
                    .keyBy(0);
        } else {
            keyBy = rescaled.map(new AddParallelismIndex()).setParallelism(config.parallelism)
                    .keyBy(0);
        }

        SlidingWindow window = (SlidingWindow) config.windows[0];
        WindowedStream windowedStream = window.getWindowMeasure() == WindowMeasure.Count
                ? keyBy.countWindow(window.getSize() / config.parallelism, window.getSlide() / config.parallelism)
                : keyBy.timeWindow(Time.milliseconds(window.getSize()), Time.milliseconds(window.getSlide()));


        AggregateFunction synopsisFunction = mergeable
                ? new SynopsisAggregator(config.synopsisClass, config.synParams)
                : new NonMergeableSynopsisAggregator(config.miniBatchSize, config.synopsisClass, config.synParams);


        SingleOutputStreamOperator preAggregated = windowedStream.aggregate(synopsisFunction).setParallelism(config.parallelism);


        AllWindowedStream partialAggregate = window.getWindowMeasure() == WindowMeasure.Count
                ? preAggregated.countWindowAll(config.parallelism)
                : preAggregated.timeWindowAll(Time.milliseconds(window.getSize()), Time.milliseconds(window.getSlide()));

        if (mergeable) {
            return partialAggregate
                    .reduce(new MergeSynopsis(), new AddWindowTimes()).setParallelism(1)
                    .returns(new TypeHint<WindowedSynopsis<S>>() {});
        } else {
            return partialAggregate
                    .aggregate(new NonMergeableSynopsisUnifier(config.managerClass), new AddWindowTimes()).setParallelism(1)
                    .returns(new TypeHint<WindowedSynopsis<NonMergeableSynopsisManager>>() {});
        }

    }

    private static<T, S extends Synopsis> SingleOutputStreamOperator<WindowedSynopsis<S>> buildScottyMergeable(BuildConfiguration config){

        DataStream<T> rescaled = config.inputStream.rescale();

        KeyedStream<Tuple2<Integer, Object>, Tuple> keyedStream;
        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Object>, S> processingFunction;


        if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)) {

            keyedStream = rescaled
                    .process(new ConvertToSample<>()).setParallelism(config.parallelism)
                    .map(new AddParallelismIndex<>()).setParallelism(config.parallelism)
                    .keyBy(0);

            processingFunction = new KeyedScottyWindowOperator<>(new SynopsisFunction(config.synopsisClass, config.synParams));

        } else {

            keyedStream = rescaled
                    .map(new AddParallelismIndex()).setParallelism(config.parallelism)
                    .keyBy(0);

            if (InvertibleSynopsis.class.isAssignableFrom(config.synopsisClass)) {

                processingFunction = new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(config.synopsisClass, config.synParams));

            } else if (CommutativeSynopsis.class.isAssignableFrom(config.synopsisClass)) {

                processingFunction = new KeyedScottyWindowOperator<>(new CommutativeSynopsisFunction(config.synopsisClass, config.synParams));

            } else {

                processingFunction = new KeyedScottyWindowOperator<>(new SynopsisFunction(config.synopsisClass, config.synParams));
            }
        }

        for (int i = 0; i < config.windows.length; i++) {
            processingFunction.addWindow(config.windows[i]);
        }

        return keyedStream.process(processingFunction).setParallelism(config.parallelism)
            .flatMap(new MergePreAggregates(config.parallelism)).setParallelism(1)
            .map(new AddWindowTimesScotty()).setParallelism(1);
    }


    private static<T, M extends Synopsis> SingleOutputStreamOperator<WindowedSynopsis<M>> buildScottyNonMergeable(BuildConfiguration config){

        DataStream<T> inputStream = config.inputStream;

        final KeyedStream<Tuple2<Integer, T>, Tuple> keyedStream = inputStream
                .process(new OrderAndIndex(config.miniBatchSize, config.parallelism)).setParallelism(1)
                .keyBy(0);

        KeyedScottyWindowOperator processingFunction = new KeyedScottyWindowOperator<>(new NonMergeableSynopsisFunction(config.synopsisClass, config.sliceManagerClass, config.synParams));
        for (int i = 0; i < config.windows.length; i++) {
            processingFunction.addWindow(config.windows[i]);
        }

        return keyedStream
                .process(processingFunction).setParallelism(config.parallelism)
                .flatMap(new UnifyToManager<>(config.managerClass, config.parallelism)).setParallelism(1)
                .map(new AddWindowTimesScotty()).setParallelism(1);
    }


    static class AddParallelismIndex<T> extends RichMapFunction<T, Tuple2<Integer, T>> {
        @Override
        public Tuple2<Integer, T> map(T value) throws Exception {

            return new Tuple2<>(this.getRuntimeContext().getIndexOfThisSubtask(), value);
        }
    }

    static class AddWindowTimes<S extends Synopsis> implements AllWindowFunction<S, WindowedSynopsis<S>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable values, Collector out) throws Exception {
            values.forEach(new Consumer<S>() {
                @Override
                public void accept(S synopsis) {
                    out.collect(new WindowedSynopsis<S>(synopsis, window.getStart(), window.getEnd()));
                }
            });
        }
    }

    static class AddWindowTimesScotty<S extends Synopsis> implements MapFunction<AggregateWindow<S>, WindowedSynopsis<S>> {
        @Override
        public WindowedSynopsis<S> map(AggregateWindow<S> value) throws Exception {
            return new WindowedSynopsis<S>(value.getAggValues().get(0), value.getStart(), value.getEnd());
        }
    }

    static class MergeSynopsis implements ReduceFunction<MergeableSynopsis> {
        public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
            return value1.merge(value2);
        }
    }
}

