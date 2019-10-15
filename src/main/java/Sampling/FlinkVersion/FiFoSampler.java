package Sampling.FlinkVersion;

import Synopsis.Synopsis;
import java.io.Serializable;
import java.util.*;

/**
 * Implementation of the traditional FiFo Sampling algorithm with a given sample size. This Sampler just collect
 * the elements and put them in a Queue of bounded size, if the Queue has reached its maximum capacity the oldest
 * element will be removed from it.
 *
 * @param <T> the type of elements maintained by this sampler
 *
 * @author Rudi Poepsel Lemaitre
 */
public class FiFoSampler<T> implements Synopsis<T>, Serializable {
    private LinkedList<T> sample;
    private int sampleSize;
    private int merged;

    /**
     * Construct an empty FiFoSampler
     *
     * @param sampleSize maximal size of the sampler
     */
    public FiFoSampler(Integer sampleSize) {
        this.sample = new LinkedList<>();
        this.sampleSize = sampleSize;
        this.merged = 1;
    }


    /**
     * Insert the new element to the Queue and remove oldest if the Queue has reached the desired sampleSize.
     *
     * @param element to be added in the Queue
     */
    @Override
    public void update(T element) {
        if (sample.size() < sampleSize) {
            sample.addLast(element);
        } else {
            sample.pollFirst();
            sample.addLast(element);
        }

    }

    public LinkedList<T> getSample() {
        return sample;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    /**
     * Function to Merge two FiFo Samples. This function takes advantage of the ordering of the elements given by
     * the {@code Synopsis.BuildSynopsis} retaining only the newest elements that entered the window.
     *
     * @param other FiFo Sample to be merged with
     * @return merged FiFo Sample
     * @throws Exception
     */
    @Override
    public FiFoSampler merge(Synopsis other){
        if (other instanceof FiFoSampler
                && ((FiFoSampler) other).getSampleSize() == this.sampleSize) {

            LinkedList<T> otherSample = ((FiFoSampler) other).getSample();
            LinkedList<T> mergeResult = new LinkedList<>();
            while (mergeResult.size() != sampleSize && !(otherSample.isEmpty() && this.sample.isEmpty())) {
                if(!otherSample.isEmpty()){
                    mergeResult.addFirst(otherSample.pollLast());
                }
                for (int i = 0; i < merged; i++) {
                    if (mergeResult.size() != sampleSize && !this.sample.isEmpty()){
                        mergeResult.addFirst(this.sample.pollLast());
                    }
                }
            }
            this.merged += 1;
            this.sample = mergeResult;
        } else {
            throw new IllegalArgumentException("FiFoSamplers to merge have to be the same size");
        }
        return this;
    }

    @Override
    public String toString(){
        String s = new String(merged+" FiFo sample size: " + this.sampleSize+"\n");
        Iterator<T> iterator = this.sample.iterator();
        while (iterator.hasNext()){
            s += iterator.next().toString()+", ";
        }
        s = s.substring(0,s.length()-2);
        s += "\n";
        return s;
    }
}
