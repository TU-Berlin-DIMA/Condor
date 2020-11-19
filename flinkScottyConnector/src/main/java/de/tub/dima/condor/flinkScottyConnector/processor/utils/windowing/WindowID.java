package de.tub.dima.condor.flinkScottyConnector.processor.utils.windowing;

public class WindowID implements Comparable {
    private long start;
    private long end;

    public WindowID(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof WindowID) {
            if (((WindowID) o).start > this.start) {
                return -1;
            } else if (((WindowID) o).start < this.start) {
                return 1;
            } else if (((WindowID) o).start == this.start && ((WindowID) o).end > this.end) {
                return -1;
            } else if (((WindowID) o).start == this.start && ((WindowID) o).end < this.end) {
                return 1;
            } else /*if(((WindowID) o).start == this.start && ((WindowID) o).end == this.end)*/ {
                return 0;
            }
        }
        throw new IllegalArgumentException("Must be a WindowID to be compared");
    }


    @Override
    public boolean equals(Object o) {
        if (o instanceof WindowID) {
            if (((WindowID) o).start == this.start && ((WindowID) o).end == this.end) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (end ^ (end >>> 32));
        return result;
    }
}