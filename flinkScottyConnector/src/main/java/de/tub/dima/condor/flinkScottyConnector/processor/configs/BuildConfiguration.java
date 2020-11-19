package de.tub.dima.condor.flinkScottyConnector.processor.configs;

import de.tub.dima.condor.core.synopsis.NonMergeableSynopsisManager;
import de.tub.dima.condor.core.synopsis.Synopsis;
import de.tub.dima.scotty.core.windowType.Window;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class BuildConfiguration <S extends Synopsis, SM extends NonMergeableSynopsisManager, M extends NonMergeableSynopsisManager, T, Key extends Serializable, Value> implements Serializable {

    // mandatory parameters
    public DataStream<T> inputStream;
    public Class<S> synopsisClass;
    public Window[] windows;
    public Object[] synParams;
    public int parallelism; // Operator Parallelism

    // optional parameters
    public RichMapFunction<T, Tuple2<Key, Value>> stratificationKeyExtractor = null;
    public Integer miniBatchSize = 0;
    public Class<SM> sliceManagerClass = null;
    public Class<M> managerClass = null;

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public BuildConfiguration(DataStream<T> inputStream, Class<S> synopsisClass, Window[] windows, Object[] synParams, int parallelism, RichMapFunction<T, Tuple2<Key, Value>> stratificationKeyExtractor, Integer miniBatchSize, Class<SM> sliceManagerClass, Class<M> managerClass) {
        this.inputStream = inputStream;
        this.synopsisClass = synopsisClass;
        this.windows = windows;
        this.synParams = synParams;
        this.parallelism = parallelism;
        this.stratificationKeyExtractor = stratificationKeyExtractor;
        this.miniBatchSize = miniBatchSize;
        this.sliceManagerClass = sliceManagerClass;
        this.managerClass = managerClass;
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

}
