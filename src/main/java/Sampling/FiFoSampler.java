package Sampling;

import Synopsis.Synopsis;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

public class FiFoSampler<T> implements SamplerWithTimestamps<T>, Serializable {
    private TreeSet<SampleElement<T>> sample;
    private int sampleSize;
    private boolean eventTime;

    public FiFoSampler(Integer sampleSize, TimeCharacteristic timeCharacteristic) {
        this.sample = new TreeSet<>();
        this.sampleSize = sampleSize;
        if (timeCharacteristic == TimeCharacteristic.EventTime) {
            this.eventTime = true;
        } else {
            this.eventTime = false;
        }
    }


    /**
     * Update the sketch with a value T
     *
     * @param element
     */
    @Override
    public void update(SampleElement element) {
        if (sample.size() < sampleSize) {
            sample.add(element);
        } else if(sample.first().getTimeStamp() < element.getTimeStamp()){
            sample.pollFirst();
            sample.add(element);
        }

    }

    public TreeSet<SampleElement<T>> getSample() {
        return sample;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public boolean isEventTime() {
        return eventTime;
    }

    /**
     * Function to Merge two Sketches
     *
     * @param other
     * @return
     * @throws Exception
     */
    @Override
    public FiFoSampler merge(Synopsis other) {
        if (other instanceof FiFoSampler
                && ((FiFoSampler) other).getSampleSize() == this.sampleSize
                && ((FiFoSampler) other).isEventTime() == this.eventTime) {

            TreeSet<SampleElement<T>> otherSample = ((FiFoSampler) other).getSample();
            TreeSet<SampleElement<T>> mergeResult = new TreeSet<>();
            while (mergeResult.size() != sampleSize && !(otherSample.isEmpty() && this.sample.isEmpty())) {
                if (!otherSample.isEmpty() && !this.sample.isEmpty()){
                    if (otherSample.last().compareTo(this.sample.last()) > 0){
                        mergeResult.add(otherSample.pollLast());
                    } else {
                        mergeResult.add(this.sample.pollLast());
                    }
                } else if (otherSample.isEmpty()){
                    mergeResult.add(this.sample.pollLast());
                } else if (this.sample.isEmpty()){
                    mergeResult.add(otherSample.pollLast());
                }
            }
            this.sample = mergeResult;
        } else {
            throw new IllegalArgumentException("FiFoSamplers to merge have to be the same size");
        }
        return this;
    }

    @Override
    public String toString(){
        String s = new String("FiFo sample size: " + this.sampleSize+"\n");
        Iterator<SampleElement<T>> iterator = this.sample.iterator();
        while (iterator.hasNext()){
            s += iterator.next().toString()+", ";
        }
        s = s.substring(0,s.length()-2);
        s += "\n";
        return s;
    }
}
