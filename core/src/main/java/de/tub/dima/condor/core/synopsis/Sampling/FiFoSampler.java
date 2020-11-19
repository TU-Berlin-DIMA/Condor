package de.tub.dima.condor.core.synopsis.Sampling;

import de.tub.dima.condor.core.synopsis.MergeableSynopsis;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsis;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

public class FiFoSampler<T> extends StratifiedSynopsis implements SamplerWithTimestamps<T>, Serializable {
    //TODO: change treeset to priorityQueue due duplicated timestamps

    private TreeSet<TimestampedElement<T>> sample;
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
    public FiFoSampler(Integer sampleSize) {
        this.sample = new TreeSet<>();
        this.sampleSize = sampleSize;
       this.eventTime=true;
    }

    /**
     * Update the sketch with a value T
     *
     * @param element
     */
    @Override
    public void update(TimestampedElement element) {
        if (sample.size() < sampleSize) {
            sample.add(element);
        } else if(sample.first().getTimeStamp() < element.getTimeStamp()){
            sample.pollFirst();
            sample.add(element);
        }

    }

    public TreeSet<TimestampedElement<T>> getSample() {
        return sample;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public boolean isEventTime() {
        return eventTime;
    }

    /**
     * Function to Merge two MergeableSynopsis.Sketches
     *
     * @param other
     * @return
     * @throws Exception
     */
    @Override
    public FiFoSampler merge(MergeableSynopsis other) {
        if (other instanceof FiFoSampler
                && ((FiFoSampler) other).getSampleSize() == this.sampleSize
                && ((FiFoSampler) other).isEventTime() == this.eventTime) {

            TreeSet<TimestampedElement<T>> otherSample = ((FiFoSampler) other).getSample();
            TreeSet<TimestampedElement<T>> mergeResult = new TreeSet<>();
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
        Iterator<TimestampedElement<T>> iterator = this.sample.iterator();
        while (iterator.hasNext()){
            s += iterator.next().toString()+", ";
        }
        s = s.substring(0,s.length()-2);
        s += "\n";
        return s;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(sampleSize);
        out.writeObject(sample);
        out.writeBoolean(eventTime);
        out.writeObject(this.getPartitionValue());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        sampleSize = in.readInt();
        sample = (TreeSet<TimestampedElement<T>>) in.readObject();
        eventTime = in.readBoolean();
        this.setPartitionValue(in.readObject());
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }

}
