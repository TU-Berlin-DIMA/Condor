package Histograms;

import java.util.Arrays;

/**
 * Creates an Equi-width histogram using 4LT buckets from an ordinary equi-width histogram
 * @author joschavonhein
 */
public class EquiWidthHistogram4LT {
    double lowerBound;
    double upperBound;
    int numBuckets;
    double bucketLength;
    RealValuedBucket4LT[] buckets;

    public EquiWidthHistogram4LT(EquiWidthHistogram old) throws Exception {
        lowerBound = old.getLowerBound();
        upperBound = old.getUpperBound();
        int oldNumBuckets = old.getNumBuckets();
        double oldBucketLength = (upperBound-lowerBound) / oldNumBuckets;
        bucketLength = oldBucketLength * 8;
        numBuckets = (int) Math.ceil(oldNumBuckets / 8d);
        int[] oldFrequencies = old.getFrequency();
        int extraEmptyBuckets = oldNumBuckets % 8;
        upperBound += extraEmptyBuckets * oldBucketLength;
        buckets = new RealValuedBucket4LT[numBuckets];
        int[] moduloEightFrequencies = Arrays.copyOf(oldFrequencies, oldNumBuckets+extraEmptyBuckets);

        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = new RealValuedBucket4LT(lowerBound + bucketLength*i, lowerBound + bucketLength * (i+1));
            int[] bucketFrequencies = Arrays.copyOfRange(moduloEightFrequencies, i*8, (i*8)+8);
            buckets[i].build(bucketFrequencies);
        }
    }

    public int rangeQuery(double lowerBound, double upperBound){
        int result = 0;
        int leftIndex = Math.max((int)((lowerBound-this.lowerBound)/bucketLength),0);
        int rightIndex = Math.min(numBuckets-1,(int) Math.ceil((upperBound-this.lowerBound)/bucketLength));
        for (int i = leftIndex; i < rightIndex; i++) {
            result += buckets[i].getFrequency(lowerBound, upperBound);
        }
        return  result;
    }

    public double getLowerBound() {
        return lowerBound;
    }

    public double getUpperBound() {
        return upperBound;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public double getBucketLength() {
        return bucketLength;
    }

    public RealValuedBucket4LT[] getBuckets() {
        return buckets;
    }

    public EquiWidthHistogram4LT merge(EquiWidthHistogram4LT other){
        if (other.getLowerBound() != this.lowerBound || other.getUpperBound() != this.upperBound
            || other.getNumBuckets() != this.getNumBuckets()){
            throw new IllegalArgumentException("Histograms to need to have the same boundaries and the same number of buckets!");
        }
        // TODO: implement this
        return null;
    }

    public String toString(){
        String s ="";
        for (int i = 0; i < numBuckets; i++) {
            s += buckets[i].toString() + "\n\n";
        }
        return s;
    }
}
