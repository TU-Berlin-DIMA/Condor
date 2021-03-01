package de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils;

import de.tub.dima.condor.core.synopsis.MergeableSynopsis;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsis;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


/**
 * @param <T1>
 * @author Rudi Poepsel Lemaitre
 */
public class CountMinAggregator<T1> implements AggregateFunction<T1, CountMinSketch, CountMinSketch> {
    private int width;
    private int height;
    private long seed;

    public CountMinAggregator(Integer width, Integer height, Long seed) {
        this.width = width;
        this.height = height;
        this.seed = seed;
    }

    @Override
    public CountMinSketch createAccumulator() {
        return new CountMinSketch(width, height, seed);
    }

    @Override
    public CountMinSketch add(T1 value, CountMinSketch accumulator) {
        accumulator.update(value);
        return accumulator;
    }

    @Override
    public CountMinSketch getResult(CountMinSketch accumulator) {
        return accumulator;
    }


    @Override
    public CountMinSketch merge(CountMinSketch a, CountMinSketch b) {
        try {
            return a.merge(b);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(width);
        out.writeInt(height);
        out.writeLong(seed);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        width = in.readInt();
        height = in.readInt();
        seed = in.readLong();
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }
}


