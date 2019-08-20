package Sampling;

import java.io.Serializable;

public class SampleElement<T> implements Serializable, Comparable<SampleElement> {
    private T value;
    private long timeStamp;

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

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }


    @Override
    public int compareTo(SampleElement o) {
        return (int) (this.timeStamp - o.timeStamp);
    }
}
