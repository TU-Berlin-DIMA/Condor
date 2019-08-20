package Sampling;

import Sketches.Sketch;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

public class FiFoSampler<T> implements Sketch<T>, Serializable {
    private PriorityQueue<SampleElement<T>> sample;
    private int sampleSize;
    private long expireTime;
    private StreamExecutionEnvironment env;
    private boolean eventTime;

    public FiFoSampler(Integer sampleSize, StreamExecutionEnvironment env) {
        this.sample = new PriorityQueue<>();
        this.sampleSize = sampleSize;
        this.expireTime = -1;
        this.env = env;
        if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime){
            this.eventTime = true;
        } else{
            this.eventTime = false;
        }
    }

    public FiFoSampler(Integer sampleSize, Time expireTime, StreamExecutionEnvironment env) {
        this.sample = new PriorityQueue<>();
        this.sampleSize = sampleSize;
        this.expireTime = expireTime.toMilliseconds();
        this.env = env;
        if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime){
            this.eventTime = true;
        } else{
            this.eventTime = false;
        }
    }

    /**
     * Update the sketch with a value T
     *
     * @param element
     */
    @Override
    public void update(T element) {
//        if (eventTime){
//            Tuple3 t = new Tuple3();
//            RichMapFunction
//        }
//        sample.add(new SampleElement<>(element, ))
    }

    /**
     * Function to Merge two Sketches
     *
     * @param other
     * @return
     * @throws Exception
     */
    @Override
    public Sketch merge(Sketch other) throws Exception {
        return null;
    }
}
