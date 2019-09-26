package Histograms;

import java.util.TreeMap;

/**
 * Classical Equi-Depth Histogram based on sorted input (relatively trivial case)
 *
 * @author joschavonhein
 */
public class EquiDepthHistBuilder {

    double[] leftBoundaries; // boundaries are inclusive
    int rightmostBoundary; // rightmost boundary - inclusive
    int totalFrequencies;
    int numBuckets;

    public EquiDepthHistBuilder() { }

    public EquiDepthHistogram buildEquiDepthHistogram(TreeMap<Integer, Integer> sortedInput, int numBuckets, int total){
        this.numBuckets = numBuckets;
        if(sortedInput.size() < numBuckets){
            this.numBuckets = sortedInput.size();   // number of buckets cannot exceed actual number of input items
        }
        this.leftBoundaries = new double[this.numBuckets];
        this.totalFrequencies = total;
        double bucketSize = (double) total / this.numBuckets;
        int currentLeftBoundary = sortedInput.firstKey();
        leftBoundaries[0] = currentLeftBoundary;
        this.rightmostBoundary = sortedInput.lastKey();
        double tempBucketSize = 0;
        int index = 1;
        int previousVal;

        while(!sortedInput.isEmpty()) {
            previousVal = sortedInput.firstKey();
            tempBucketSize += sortedInput.pollFirstEntry().getValue();
            currentLeftBoundary = (sortedInput.isEmpty()) ? rightmostBoundary : sortedInput.firstKey();
            double fraction;

            while (tempBucketSize >= bucketSize && index < numBuckets){
                tempBucketSize -= bucketSize;
                fraction = Math.min(tempBucketSize / bucketSize, 1);
                leftBoundaries[index] = previousVal + (1-fraction) * (currentLeftBoundary-previousVal);
                index++;
            }
        }
        return new EquiDepthHistogram(leftBoundaries, rightmostBoundary, totalFrequencies);
    }

}
