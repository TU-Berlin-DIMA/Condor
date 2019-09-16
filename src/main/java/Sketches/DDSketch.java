package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import Synopsis.Synopsis;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public class DDSketch<T extends Number> implements Synopsis<T>, Serializable {
    private int maxNumBins;
    private boolean isCollapsed;
    private double relativeAccuracy;
    private double logGamma;
    private int zeroCount;
    private int globalCount;

    private double minIndexedValue;
    private double maxIndexedValue;

    private TreeMap<Integer, Integer> counts;

    public DDSketch(double relativeAccuracy, int maxNumBins) {
        if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
            throw new IllegalArgumentException("The relative accuracy must be between 0 and 1.");
        }
        this.relativeAccuracy = relativeAccuracy;
        this.logGamma = Math.log((1 + relativeAccuracy) / (1 - relativeAccuracy));
        this.maxNumBins = maxNumBins;
        this.isCollapsed = false;
        this.minIndexedValue = Math.max(0, minIndexableValue());
        this.maxIndexedValue = maxIndexableValue();
        this.zeroCount = 0;
        this.globalCount = 0;

        this.counts = new TreeMap<>();
    }

    public double minIndexableValue() {
        return Math.max(
                Math.exp((Integer.MIN_VALUE + 1) * logGamma), // so that index >= Integer.MIN_VALUE
                Double.MIN_NORMAL * Math.exp(logGamma) // so that Math.exp(index * logGamma) >= Double.MIN_NORMAL
        );
    }

    public double maxIndexableValue() {
        return Math.min(
                Math.exp(Integer.MAX_VALUE * logGamma), // so that index <= Integer.MAX_VALUE
                Double.MAX_VALUE / (1 + relativeAccuracy) // so that value >= Double.MAX_VALUE
        );
    }

    private void checkValueTrackable(double value) {
        if (value < 0 || value > maxIndexedValue) {
            throw new IllegalArgumentException("The input value is outside the range that is tracked by the sketch.");
        }
    }

    /**
     * Update the Synopsis structure with a new incoming element.
     *
     * @param element new incoming element
     */
    @Override
    public void update(T element) {
        double elemValue = element.doubleValue();
        checkValueTrackable(elemValue);
        if (elemValue < minIndexedValue) {
            zeroCount++;
        } else {
            globalCount++;
            int index = index(elemValue);
            counts.merge(index, 1, (a, b) -> a + b);
            if (counts.size() > maxNumBins) {
                Map.Entry<Integer, Integer> bin = counts.pollFirstEntry();
                counts.merge(counts.firstKey(), bin.getValue(), (a, b) -> a + b);
                isCollapsed = true;
            }
        }
    }

    public int index(double value) {
        final double index = Math.log(value) / logGamma;
        return index >= 0 ? (int) index : (int) index - 1;
    }

    public double value(int index) {
        return Math.exp(index * logGamma) * (1 + relativeAccuracy);
    }

    public double getMinValue() {
        if (zeroCount > 0) {
            return 0;
        } else {
            return value(counts.firstKey());
        }
    }

    public double getMaxValue() {
        if (zeroCount > 0 && counts.isEmpty()) {
            return 0;
        } else {
            return value(counts.lastKey());
        }
    }

    public double getValueAtQuantile(double quantile) {
        return getValueAtQuantile(quantile, zeroCount + globalCount);
    }

    public double[] getValuesAtQuantiles(double[] quantiles) {
        final long count = zeroCount + globalCount;
        return Arrays.stream(quantiles)
                .map(quantile -> getValueAtQuantile(quantile, count))
                .toArray();
    }

    private double getValueAtQuantile(double quantile, long count) {
        if (quantile < 0 || quantile > 1) {
            throw new IllegalArgumentException("The quantile must be between 0 and 1.");
        }

        if (count == 0) {
            throw new NoSuchElementException();
        }

        final long rank = (long) (quantile * (count - 1));
        if (rank < zeroCount) {
            return 0;
        }

        if (quantile <= 0.5) {
            long n = zeroCount;
            for(Map.Entry<Integer,Integer> bin : counts.entrySet()) {
                if (n > rank){
                    return value(bin.getKey());
                }
                n += bin.getValue();
            }
            return getMaxValue();
        } else {
            long n = count;
            for(Map.Entry<Integer,Integer> bin : counts.descendingMap().entrySet()) {
                if (n <= rank){
                    return value(bin.getKey());
                }
                n -= bin.getValue();
            }
            return getMinValue();
        }
    }

    public TreeMap<Integer, Integer> getCounts() {
        return counts;
    }

    /**
     * Function to Merge two Synopses.
     *
     * @param other synopsis to be merged with
     * @return merged synopsis
     * @throws Exception
     */
    @Override
    public DDSketch merge(Synopsis other) throws Exception {
        if (other instanceof DDSketch) {
            DDSketch otherDD = (DDSketch) other;
            if (this.relativeAccuracy == otherDD.relativeAccuracy && this.maxNumBins == otherDD.maxNumBins){
                if (otherDD.getCounts().isEmpty()){
                    return this;
                }
                ((TreeMap<Integer, Integer>) otherDD.getCounts()).forEach(
                        (key, value) -> counts.merge(key, value, (a, b) -> a + b)
                );
                while (counts.size() > maxNumBins) {
                    Map.Entry<Integer, Integer> bin = counts.pollFirstEntry();
                    counts.merge(counts.firstKey(), bin.getValue(), (a, b) -> a + b);
                    isCollapsed = true;
                }
                this.globalCount += otherDD.globalCount;
                this.zeroCount += otherDD.zeroCount;
                return this;
            }
        }
        throw new Exception("Sketches to merge have to be the same size and hash Functions");
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(maxNumBins);
        out.writeBoolean(isCollapsed);
        out.writeDouble(relativeAccuracy);
        out.writeDouble(logGamma);
        out.writeInt(zeroCount);
        out.writeInt(globalCount);
        out.writeDouble(minIndexedValue);
        out.writeDouble(maxIndexedValue);
        out.writeObject(counts);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        maxNumBins = in.readInt();
        isCollapsed = in.readBoolean();
        relativeAccuracy = in.readDouble();
        logGamma = in.readDouble();
        zeroCount = in.readInt();
        globalCount = in.readInt();
        minIndexedValue = in.readDouble();
        maxIndexedValue = in.readDouble();
        counts = (TreeMap<Integer, Integer>) in.readObject();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
