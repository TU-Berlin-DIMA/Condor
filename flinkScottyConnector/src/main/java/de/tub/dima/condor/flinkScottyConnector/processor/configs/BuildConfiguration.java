package de.tub.dima.condor.flinkScottyConnector.processor.configs;

import de.tub.dima.condor.core.synopsis.NonMergeableSynopsisManager;
import de.tub.dima.condor.core.synopsis.Synopsis;
import de.tub.dima.scotty.core.windowType.Window;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

/**
 *  Configuration Class needed for the SynopsisBuilder.
 *  mandatory parameters have to be present in order for the whole process to function.
 *
 *  Depending on which optional parameters are given the SynopsisBuilder builds the Pipeline differently
 */
public class BuildConfiguration <S extends Synopsis, SM extends NonMergeableSynopsisManager, M extends NonMergeableSynopsisManager, T, Key extends Serializable, Value> implements Serializable {

    // mandatory parameters
    public DataStream<T> inputStream;   // DataStream with the incoming elements
    public Class<S> synopsisClass;      // synopsis Class
    public Window[] windows;            // the Window Definition (Count vs. Time and metric)
    public Object[] synParams;          // the synopsis Parameters (depending on actual synopsis the user wants to create)
    public int parallelism;             // Operator Parallelism

    // optional parameters - change the pipeline behavior if they are set
    public RichMapFunction<T, Tuple2<Key, Value>> stratificationKeyExtractor = null;    // needs to be set in order to build Stratified Synopsis. function which transforms the inputStream into the needed shape with the stratification key in field0.
    public Integer miniBatchSize = 0;               // only needed in case of non-mergeable synopsis. if set, this amount of incoming elements are put in a queue and and ordered by their timestamp and then sent in their respective order.
    public Class<SM> sliceManagerClass = null;      // needed for non-mergeable synopsis and sliding windows (-> scotty instead of Flink used)
    public Class<M> managerClass = null;            // only needed for non-mergeable non-stratified synopsis
    private boolean forceBucketing = false;         // only for benchmarking proposes turn to true or the performance can be compromised.

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }


    public BuildConfiguration(DataStream<T> inputStream, Class<S> synopsisClass, Window[] windows, Object[] synParams, int parallelism, RichMapFunction<T, Tuple2<Key, Value>> stratificationKeyExtractor, Class<SM> sliceManagerClass, Class<M> managerClass) {
        this.inputStream = inputStream;
        this.synopsisClass = synopsisClass;
        this.windows = windows;
        this.synParams = synParams;
        this.parallelism = parallelism;
        this.stratificationKeyExtractor = stratificationKeyExtractor;
        this.sliceManagerClass = sliceManagerClass;
        this.managerClass = managerClass;
    }

    public BuildConfiguration(DataStream<T> inputStream, Class<S> synopsisClass, Window[] windows, Object[] synParams, int parallelism, Class<SM> sliceManagerClass, Class<M> managerClass) {
        this.inputStream = inputStream;
        this.synopsisClass = synopsisClass;
        this.windows = windows;
        this.synParams = synParams;
        this.parallelism = parallelism;
        this.sliceManagerClass = sliceManagerClass;
        this.managerClass = managerClass;
    }

    public BuildConfiguration(DataStream<T> inputStream, Class<S> synopsisClass, Window[] windows, Object[] synParams, int parallelism, Integer miniBatchSize, Class<SM> sliceManagerClass, Class<M> managerClass) {
        this.inputStream = inputStream;
        this.synopsisClass = synopsisClass;
        this.windows = windows;
        this.synParams = synParams;
        this.parallelism = parallelism;
        this.miniBatchSize = miniBatchSize;
        this.sliceManagerClass = sliceManagerClass;
        this.managerClass = managerClass;
    }

    public BuildConfiguration(DataStream<T> inputStream, Class<S> synopsisClass, Window[] windows, Object[] synParams, int parallelism, RichMapFunction<T, Tuple2<Key, Value>> stratificationKeyExtractor) {
        this.inputStream = inputStream;
        this.synopsisClass = synopsisClass;
        this.windows = windows;
        this.synParams = synParams;
        this.parallelism = parallelism;
        this.stratificationKeyExtractor = stratificationKeyExtractor;
    }

    public BuildConfiguration(DataStream<T> inputStream, Class<S> synopsisClass, Window[] windows, Object[] synParams, int parallelism) {
        this.inputStream = inputStream;
        this.synopsisClass = synopsisClass;
        this.windows = windows;
        this.synParams = synParams;
        this.parallelism = parallelism;
    }


    public DataStream<T> getInputStream() {
        return inputStream;
    }

    public void setInputStream(DataStream<T> inputStream) {
        this.inputStream = inputStream;
    }

    public Class<S> getSynopsisClass() {
        return synopsisClass;
    }

    public void setSynopsisClass(Class<S> synopsisClass) {
        this.synopsisClass = synopsisClass;
    }

    public Window[] getWindows() {
        return windows;
    }

    public void setWindows(Window[] windows) {
        this.windows = windows;
    }

    public Object[] getSynParams() {
        return synParams;
    }

    public void setSynParams(Object[] synParams) {
        this.synParams = synParams;
    }

    public RichMapFunction<T, Tuple2<Key, Value>> getStratificationKeyExtractor() {
        return stratificationKeyExtractor;
    }

    public void setStratificationKeyExtractor(RichMapFunction<T, Tuple2<Key, Value>> stratificationKeyExtractor) {
        this.stratificationKeyExtractor = stratificationKeyExtractor;
    }

    public Integer getMiniBatchSize() {
        return miniBatchSize;
    }

    public void setMiniBatchSize(Integer miniBatchSize) {
        this.miniBatchSize = miniBatchSize;
    }

    public Class<SM> getSliceManagerClass() {
        return sliceManagerClass;
    }

    public void setSliceManagerClass(Class<SM> sliceManagerClass) {
        this.sliceManagerClass = sliceManagerClass;
    }

    public Class<M> getManagerClass() {
        return managerClass;
    }

    public void setManagerClass(Class<M> managerClass) {
        this.managerClass = managerClass;
    }

    /**
     * Call only for benchmarking proposes! Performance may be compromised.
     * */
    public void forceBucketing(){
        this.forceBucketing = true;
    }

    public boolean isForceBucketing() {
        return forceBucketing;
    }
}
