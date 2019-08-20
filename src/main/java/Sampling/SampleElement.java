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

//    @Override
//    public boolean equals(Object obj){
//
//        if (obj instanceof SampleElement){
//            if (this.compareTo((SampleElement) obj) == 0){
//                return ((SampleElement) obj).getValue().equals(this.value);
//            }
//        }
//        return false;
//    }

}
