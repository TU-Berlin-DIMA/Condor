package Sampling;

import java.io.Serializable;

/**
 * Object to represent a sample element in the case the synopsis need the timestamp of each elements.
 * @see Synopsis.BuildSynopsis.ConvertToSample
 *
 * @param <T> the type of the elements
 *
 * @author Rudi Poepsel Lemaitre
 */
public class SampleElement<T> implements Serializable, Comparable<SampleElement> {
    private T value;
    private long timeStamp;

    /**
     * Construct a SampleElement with a given timestamp
     *
     * @param value value of the element
     * @param timeStamp of the element (It can be the Event-timestamp or Process-Timestamp)
     */
    public SampleElement(T value, long timeStamp) {
        this.value = value;
        this.timeStamp = timeStamp;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getTimeStamp() {
        return timeStamp;
    }


    @Override
    public int compareTo(SampleElement o) {
        int diff = (int) (this.timeStamp - o.timeStamp);
        if(diff == 0 && !o.getValue().equals(this.value)){
                return -1;
        }
        return diff;
    }

    @Override
    public String toString() {
        return new String("(" + value.toString() + " | " + timeStamp + ")");
    }

}
