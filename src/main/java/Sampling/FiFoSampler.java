package Sampling;

import Sketches.Sketch;
import java.io.Serializable;
import java.util.*;

public class FiFoSampler<T> implements Sketch<T>, Serializable {
    private LinkedList<T> sample;
    private int sampleSize;
    private int merged;

    public FiFoSampler(Integer sampleSize) {
        this.sample = new LinkedList<>();
        this.sampleSize = sampleSize;
        this.merged = 1;
    }


    /**
     * Update the sketch with a value T
     *
     * @param element
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
     * Function to Merge two Sketches
     *
     * @param other
     * @return
     * @throws Exception
     */
    @Override
    public FiFoSampler merge(Sketch other) throws Exception {
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
            throw new Exception("FiFoSamplers to merge have to be the same size");
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
