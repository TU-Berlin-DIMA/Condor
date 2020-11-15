package de.tub.dima.condor.core.Synopsis.Histograms;

import java.util.Arrays;

/**
 * classic EquiDepthHistogram with range query capabilites.
 * Does not contain methods to build the histogram but rather takes boundaries and frequencies as input.
 * This way multiple classes with different build methods can all use this class.
 *
 * @author joschavonhein
 */
public class EquiDepthHistogram {
    private double[] leftBoundaries;
    private double rightMostBoundary;
    private double totalFrequencies;
    private int numBuckets;
    private double perbucketFrequency;

    /**
     * Constructor with all necessary parameters
     * @param leftBoundaries    Array which contains the left boundaries of all buckets in ascending order (!)
     * @param rightMostBoundary the rightmost boundary (inclusive)
     * @param totalFrequencies  the total amount of frequencies
     */
    public EquiDepthHistogram(double[] leftBoundaries, double rightMostBoundary, double totalFrequencies) {
        this.leftBoundaries = leftBoundaries;
        this.rightMostBoundary = rightMostBoundary;
        this.totalFrequencies = totalFrequencies;
        this.numBuckets = leftBoundaries.length;
        this.perbucketFrequency = this.totalFrequencies /this.numBuckets;
    }

    /**
     * Return frequency of a range query. lower bound is inclusive, upper bound is exclusive
     * @param lowerBound    inclusive
     * @param upperBound    upper bound of the range query
     * @return  estimated result frequency of the range query
     */
    public double rangeQuery(double lowerBound, double upperBound){

        if (upperBound - lowerBound < 0){
            throw new IllegalArgumentException("upper Bound can't be smaller than lower Bound!");
        }
        if (upperBound < leftBoundaries[0] || lowerBound > rightMostBoundary){
            throw new IllegalArgumentException("query Bound can't be out of histogram domain");
        }

        boolean first = false;
        boolean last = false;
        double result = 0;
        double rightMostBucketBound=0;
        int lowerBucket=-1;//numBuckets-1;
        int upperBucket=-3;//numBuckets-1;

        //case Query range starts from amount lower than first leftboundry and ends in histogram domain
        if(lowerBound < leftBoundaries[0] && upperBound >= leftBoundaries[0]){
            lowerBucket = 0;
            lowerBound= leftBoundaries[0];
            first=true;}
        //case Query range starts in histogram domain and ends in amounts greater than rightmostboundry
        if(upperBound >= rightMostBoundary && lowerBound <= rightMostBoundary){
            upperBucket=numBuckets-1;
            upperBound=rightMostBoundary;
            last=true;
        }

        //case Query range starts at last bucket
        if(lowerBound >= leftBoundaries[numBuckets-1] && lowerBound <= rightMostBoundary){
            lowerBucket=numBuckets-1;
            first=true;
        }
        //case Query range ends at last bucket
        if(upperBound >= leftBoundaries[numBuckets-1] && upperBound <= rightMostBoundary){
            upperBucket=numBuckets-1;
            last=true;
        }


        for (int i = 0; i < numBuckets-1; i++) {

            if (!first) {
                if (lowerBound >= leftBoundaries[i] && lowerBound <= leftBoundaries[i + 1]) {
                    lowerBucket = i;
                    first = true;
                }
            }
            if (!last) {
                if (upperBound <= leftBoundaries[i + 1] ) {
                    upperBucket = i;
                    last = true;
                }
            }
        }
        if(upperBucket==numBuckets-1){
            rightMostBucketBound=rightMostBoundary;

        }
        else{
            rightMostBucketBound=leftBoundaries[upperBucket+1];
        }
        if (upperBucket == lowerBucket) {
            double fraction = (upperBound-lowerBound)/(rightMostBucketBound-leftBoundaries[lowerBucket]);
            result= fraction * perbucketFrequency;
        }
        else{
            int midBucket= upperBucket-lowerBucket-1;
            double leftmostFraction = (Math.min(upperBound,leftBoundaries[lowerBucket+1])-lowerBound)/(leftBoundaries[lowerBucket+1]-leftBoundaries [lowerBucket]);
            double rightmostFraction = (upperBound-leftBoundaries[upperBucket])/(rightMostBucketBound-leftBoundaries[upperBucket]);

            result = (midBucket+leftmostFraction+rightmostFraction) * perbucketFrequency;

        }
        return result;

    }

    @Override
    public String toString() {
        return "EquiDepthHistogram{" +
                "leftBoundaries: \n" + Arrays.toString(leftBoundaries)  +
                ", rightMostBoundary=" + rightMostBoundary + "\n" +
                ", totalFrequencies=" + totalFrequencies +
                ", numBuckets=" + numBuckets +
                "}\n";
    }

    public double[] getLeftBoundaries() {
        return leftBoundaries;
    }

    public double getRightMostBoundary() {
        return rightMostBoundary;
    }

    public double getTotalFrequencies() {
        return totalFrequencies;
    }

    public int getNumBuckets() {
        return numBuckets;
    }
}
