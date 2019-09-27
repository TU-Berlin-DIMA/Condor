package Histograms;

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
        int bucketsInRange = 0;
        double result = 0;

        // edge case that lower Bound is in last Bucket
        if (lowerBound >= leftBoundaries[numBuckets-1]){  
            System.out.println("at upper");
            double fraction = (Math.min(rightMostBoundary, upperBound)-lowerBound)/(rightMostBoundary-leftBoundaries[numBuckets-1]);
            return fraction * totalFrequencies / numBuckets;
        }

        for (int i = 0; i < numBuckets; i++) {
            // edge case that range is contained in a single bucket
            if (lowerBound >= leftBoundaries[i] && i < numBuckets-1 && upperBound < leftBoundaries[i+1]){
                //System.out.println("yessssssssssssss");
                double fraction = (upperBound-lowerBound) / (leftBoundaries[i+1]-leftBoundaries[i]);
                return fraction * totalFrequencies / numBuckets;
            }

            // add leftmost bucket part to query result
            if (!first && leftBoundaries[i] >= lowerBound){
               // System.out.println("firssstttt");
                first = true;
                if (i > 0){
                    //System.out.println("non-zeroooo");
                    double leftMostBucketFraction = (leftBoundaries[i] - lowerBound) / (leftBoundaries[i] - leftBoundaries[i-1]);
                    result += leftMostBucketFraction * totalFrequencies/numBuckets;
                }
            }

            // count amount of fully contained buckets in range
            if (first && !last){
                //System.out.println("noottfirssstttt");
                if (upperBound < leftBoundaries[i]){
                    //System.out.println("innnnnoottfirssstttt");
                    last = true;
                    double rightmostBucketFraction = (upperBound - leftBoundaries[i-1]) / (leftBoundaries[i] - leftBoundaries[i-1]);
                    result += rightmostBucketFraction * totalFrequencies/numBuckets; // add rightmost bucket part to query result
                    System.out.println(result);
                }
                bucketsInRange++;
            }
        }
        result += bucketsInRange * totalFrequencies / numBuckets;
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
