package Sampling.FlinkVersion;

import Synopsis.Synopsis;
import org.apache.flink.util.XORShiftRandom;

import java.io.Serializable;

public class GreedySampler<T> implements Synopsis<T>, Serializable {
    private T sample[];
    private int sampleSize;
    private XORShiftRandom rand;
    private int processedElements;

    /**
     * Construct a new empty Reservoir Sampler with a bounded size.
     *
     * @param sampleSize maximal sample size
     */
    public GreedySampler(Integer sampleSize) {
        this.sample = (T[]) new Object[sampleSize];
        this.sampleSize = sampleSize;
        this.rand = new XORShiftRandom();
        this.processedElements = 0;
    }

    /**
     * Update the Synopsis structure with a new incoming element.
     *
     * @param element new incoming element
     */
    @Override
    public void update(T element) {

    }

    /**
     * Function to Merge two Synopses.
     *
     * @param other synopsis to be merged with
     * @return merged synopsis
     * @throws Exception
     */
    @Override
    public GreedySampler merge(Synopsis other) {
        return null;
    }
}
