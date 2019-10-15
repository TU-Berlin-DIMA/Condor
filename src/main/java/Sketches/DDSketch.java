package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import Synopsis.InvertibleSynopsis;
import Synopsis.Synopsis;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * Implementation of DDSketch to estimate every p-Quantile with relative error Bounds and fixed
 * maximum Memory usage. If the maximum number of bins is exceeded the lowest bins maintained will be merged
 * losing the error guarantees for lowest Quantiles but preserving the relative error for middle and high
 * Quantiles.
 * This algorithm was proposed by DataDog.
 *
 * @param <T> the type of elements maintained by this sketch
 * @author Rudi Poepsel Lemaitre
 */
public class DDSketch<T extends Number> implements InvertibleSynopsis<T>, Serializable {
    private int maxNumBins;
    private boolean isCollapsed;
    private double relativeAccuracy;
    private double logGamma;
    private int zeroCount;
    private int globalCount;

    private double minIndexedValue;
    private double maxIndexedValue;

    private TreeMap<Integer, Integer> counts;

    /**
     * Construct a DDSketch
     *
     * @param relativeAccuracy to define the query error bounds for each Quantile
     * @param maxNumBins       Maximum number of bins to be maintained, if this value is exceeded the lowest bins
     *                         will be merged
     */
    public DDSketch(Double relativeAccuracy, Integer maxNumBins) {
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

    /**
     * @return the lowest value that can be indexed
     */
    public double minIndexableValue() {
        return Math.max(
                Math.exp((Integer.MIN_VALUE + 1) * logGamma), // so that index >= Integer.MIN_VALUE
                Double.MIN_NORMAL * Math.exp(logGamma) // so that Math.exp(index * logGamma) >= Double.MIN_NORMAL
        );
    }

    /**
     * @return the highest value that can be indexed
     */
    public double maxIndexableValue() {
        return Math.min(
                Math.exp(Integer.MAX_VALUE * logGamma), // so that index <= Integer.MAX_VALUE
                Double.MAX_VALUE / (1 + relativeAccuracy) // so that value >= Double.MAX_VALUE
        );
    }

    /**
     * Test if the value can be inserted in the structure.
     */
    private void checkValueTrackable(double value) {
        if (value < 0 || value > maxIndexedValue) {
            throw new IllegalArgumentException("The input value is outside the range that is tracked by the sketch.");
        }
    }

    /**
     * Update the DDSketch index structure with a new incoming element, by incrementing the counter value if the
     * Bin already exists and creating a new Bin in the case this element is the first element from its Bin.
     * In the case the maximum number of Bins is exceeded the lowest Bins will be merged.
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


    /**
     * Given a value calculate the index of the corresponding Bin.
     *
     * @param value to get the index from
     * @return the log index correspondig the accuracy factor (logGamma)
     */
    public int index(double value) {
        final double index = Math.log(value) / logGamma;
        return index >= 0 ? (int) index : (int) index - 1;
    }

    /**
     * Calculate the representative value from the given index acording to the relative accuracy
     *
     * @param index to calcule the value from
     * @return the reprentative value
     */
    public double value(int index) {
        return Math.exp(index * logGamma) * (1 + relativeAccuracy);
    }

    /**
     * @return the value of the maintained Bin with the lowest index
     */
    public double getMinValue() {
        if (zeroCount > 0) {
            return 0;
        } else {
            return value(counts.firstKey());
        }
    }

    /**
     * @return the value of the maintained Bin with the highest index
     */
    public double getMaxValue() {
        if (zeroCount > 0 && counts.isEmpty()) {
            return 0;
        } else {
            return value(counts.lastKey());
        }
    }

    /**
     * Estimate the p-Quantile value considering all the elements in the actual structure
     *
     * @param quantile p value of the quantile (0 < p < 1)
     * @return the esimated quantile value with a relative accuracy
     */
    public double getValueAtQuantile(double quantile) {
        return getValueAtQuantile(quantile, zeroCount + globalCount);
    }

    /**
     * Estimate different p-Quantile values considering all the elements in the actual structure
     *
     * @param quantiles an array containing all p values from each quantile (0 < p < 1)
     * @return an array containing the estimated quantiles in the same order as the input
     */
    public double[] getValuesAtQuantiles(double[] quantiles) {
        final long count = zeroCount + globalCount;
        return Arrays.stream(quantiles)
                .map(quantile -> getValueAtQuantile(quantile, count))
                .toArray();
    }

    /**
     * Estimate the p-Quantile value considering only a given number of elements
     *
     * @param quantile p value of the quantile (0 < p < 1)
     * @param count    the number of elements to be considered as total
     * @return the estimated quantile value considering a especified number
     */
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
            for (Map.Entry<Integer, Integer> bin : counts.entrySet()) {
                if (n > rank) {
                    return value(bin.getKey());
                }
                n += bin.getValue();
            }
            return getMaxValue();
        } else {
            long n = count;
            for (Map.Entry<Integer, Integer> bin : counts.descendingMap().entrySet()) {
                if (n <= rank) {
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

    @Override
    public InvertibleSynopsis<T> invert(InvertibleSynopsis<T> toRemove) {
        if (toRemove instanceof DDSketch) {
            DDSketch otherDD = (DDSketch) toRemove;
            if (this.relativeAccuracy == otherDD.relativeAccuracy && this.maxNumBins == otherDD.maxNumBins) {
                if (otherDD.getCounts().isEmpty()) {
                    return this;
                }
                ((TreeMap<Integer, Integer>) otherDD.getCounts()).forEach(
                        (key, value) -> counts.merge(key, value, (a, b) -> a - b)
                );
                int newGlobalCount = 0;
                for(Map.Entry<Integer,Integer> entry : counts.entrySet()) {
                    if (entry.getValue() <= 0){
                        counts.remove(entry.getKey());
                    } else{
                        globalCount += entry.getValue();
                    }
                }

                this.globalCount = newGlobalCount;
                if (this.zeroCount > otherDD.zeroCount){
                    this.zeroCount -= otherDD.zeroCount;
                } else {
                    this.zeroCount = 0;
                }

                return this;
            }
        }
        throw new IllegalArgumentException("Sketches to merge have to be the same size and hash Functions");
    }

    @Override
    public void decrement(T toDecrement) {
        double elemValue = toDecrement.doubleValue();
        checkValueTrackable(elemValue);
        if (elemValue < minIndexedValue && zeroCount > 0) {
            zeroCount--;
        } else {
            if (globalCount > 0) {
                globalCount--;
            }
            int index = index(elemValue);
            Integer bucket = counts.get(index);
            if (bucket != null) {
                if (bucket <= 1){
                    counts.remove(index);
                }else{
                    counts.merge(index, -1, (a, b) -> a + b);
                }
            }
        }
    }

    /**
     * Function to Merge two DDSketches by adding the content of all the Bins.
     *
     * @param other DDSketch to be merged with
     * @return merged DDSketch
     * @throws Exception in case
     */
    @Override
    public DDSketch merge(Synopsis other) {
        if (other instanceof DDSketch) {
            DDSketch otherDD = (DDSketch) other;
            if (this.relativeAccuracy == otherDD.relativeAccuracy && this.maxNumBins == otherDD.maxNumBins) {
                if (otherDD.getCounts().isEmpty()) {
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
        throw new IllegalArgumentException("Sketches to merge have to be the same size and hash Functions");
    }

    @Override
    public String toString() {
        String sketch = new String();
        sketch += "Relative Accuracy: " + relativeAccuracy + "\n";
        sketch += "Max Number of Bins: " + maxNumBins + "\n";
        sketch += "Collapsed: " + isCollapsed + "\n";
        sketch += "Count: " + (globalCount + zeroCount) + "\n";

        sketch += counts.toString() + "\n";
        //sketch += "Quantile: " + getValueAtQuantile(0.5) + "\n";
        return sketch;
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
