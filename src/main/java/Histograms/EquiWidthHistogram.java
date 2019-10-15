package Histograms;

import Synopsis.Synopsis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Equi-Width Histogram with given bucket boundaries.
 * @param <T>
 * @author joschavonhein
 */
public class EquiWidthHistogram<T extends Number> implements Synopsis<T> {

    private static final Logger LOG = LoggerFactory.getLogger(EquiWidthHistogram.class);

    double lowerBound, upperBound;
    int numBuckets;
    int[] frequency;
    double bucketLength;

    /**
     * Creates an equi-width histogram with the given boundaries and number of buckets
     * @param lowerBound lower bound of the Histogram inclusive
     * @param upperBound upper bound of the Histogram exclusive
     * @param numBuckets number of Buckets
     * @throws IllegalArgumentException
     */
    public EquiWidthHistogram(Double lowerBound, Double upperBound, Integer numBuckets) throws IllegalArgumentException {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.numBuckets = numBuckets;
        this.frequency = new int[numBuckets];
        if (lowerBound == null || upperBound == null || upperBound - lowerBound <= 0){
            throw new IllegalArgumentException("lower bound has to be smaller than upper bound!");
        }
        this.bucketLength = (upperBound - lowerBound) / (double) numBuckets;
    }


    @Override
    public void update(T number) {
        double input = number.doubleValue();
        if (input >= upperBound || input < lowerBound){
            throw new IllegalArgumentException("input is out of Bounds!");
        }
        int index = (int) ((input - lowerBound) / bucketLength);

        frequency[index]++;
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

    public int[] getFrequency() {
        return frequency;
    }

    @Override
    public EquiWidthHistogram merge(Synopsis other) {
        if (other instanceof EquiWidthHistogram){
            EquiWidthHistogram o = (EquiWidthHistogram) other;
            if (lowerBound == o.getLowerBound()
                    && upperBound == o.getUpperBound()
                    && numBuckets == o.getNumBuckets()){
                int[] otherFrequencies = o.getFrequency();
                for (int i = 0; i < numBuckets; i++) {
                    frequency[i] += otherFrequencies[i];
                }
                return this;
            }else throw new IllegalArgumentException("Histograms have to have the same properties (boundaries and number of buckets) in order to be merged!");
        }
        throw new IllegalArgumentException("merged Class has to be of Class Equi-Width Histogram!");
    }


    /**
     * Method to approximately answer a range query using the continuous value assumption!
     * @param lowerBound    lower bound of the query
     * @param upperBound    upper bound of the query
     * @return              approximate frequency for the given range
     */
    public double rangeQuery(double lowerBound, double upperBound){
        if (upperBound - lowerBound <= 0){
            throw new IllegalArgumentException("lower bound has to be smaller than upper bound!");
        }
        if (upperBound < this.lowerBound){
            return 0;
        }
        int indexLB = (int) Math.floor((lowerBound - this.lowerBound) / bucketLength); // the index of the leftmost bucket of the query range
        int indexUB = (int) Math.floor((upperBound - this.lowerBound) / bucketLength); // the index of the rightmost bucket of the query range
        double leftMostBucketShare = 0, rightMostBucketShare = 0;
        if (indexLB >= 0 && indexLB <numBuckets){
            double bucketUB = this.lowerBound + (indexLB+1) * bucketLength;
            leftMostBucketShare = ((bucketUB - lowerBound) / bucketLength) * frequency[indexLB]; //compute the frequency of the part of the leftmost bucket
            indexLB++;
        }else {
            indexLB = 0;
        }
        if (indexUB >= 0 && indexUB < numBuckets){
            double bucketUB = this.lowerBound + (indexUB+1) * bucketLength;
            rightMostBucketShare = ((1-(bucketUB - upperBound) / bucketLength)) * frequency[indexUB]; // compute the frequency of the part of the rightmost bucket
        } else {
            indexUB = numBuckets;
        }
        double resultFrequency = leftMostBucketShare + rightMostBucketShare;
        for (int i = indexLB; i < indexUB; i++) {
            resultFrequency += frequency[i];
        }
        return resultFrequency;
    }

    @Override
    public String toString(){
        String s = "Equi-Width Histogram properties:\n" +
                "number of Buckets: " + numBuckets +
                "\n lower Bound: " + lowerBound + " - upper Bound: " + upperBound + "\n Frequencies: \n |";

        for (int i = 0; i < numBuckets; i++) {
            s += frequency[i] + "|";
        }
        return s + "\n\n";
    }
}
