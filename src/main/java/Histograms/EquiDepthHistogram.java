package Histograms;

import java.util.TreeMap;

public class EquiDepthHistogram {

    int[] leftBoundaries; // boundaries are inclusive
    int rightmostBoundary; // rightmost boundary - inclusive
    int totalFrequencies;
    int numBuckets;

    public EquiDepthHistogram(TreeMap<Integer, Integer> sortedInput, int numBuckets, int total) {

        this.totalFrequencies = total;
        int bucketSize = total / numBuckets;
        int currentLeftBoundary = sortedInput.firstKey();
        leftBoundaries[0] = currentLeftBoundary;
        this.rightmostBoundary = sortedInput.lastKey();
        int tempBucketSize = 0;
        int index = 1;
        this.numBuckets = numBuckets;

        for (int i = 0; i < sortedInput.size(); i++) {
            tempBucketSize += sortedInput.get(currentLeftBoundary);
            currentLeftBoundary = sortedInput.higherKey(currentLeftBoundary);
            if (tempBucketSize > bucketSize){
                leftBoundaries[index] = currentLeftBoundary;
                index++;
                tempBucketSize=0;
            }
        }
    }

    public int rangeQuery(int lowerBound, int upperBound){

        if (upperBound - lowerBound < 0){
            throw new IllegalArgumentException("upper Bound can't be smaller than lower Bound!");
        }

        boolean first = false;
        boolean last = false;
        int bucketsInRange = 0;
        int result = 0;

        if (lowerBound > leftBoundaries[numBuckets-1]){
            double fraction = Math.min(rightmostBoundary, upperBound)/(rightmostBoundary-leftBoundaries[numBuckets-1]);
            return (int)Math.round(fraction*totalFrequencies/numBuckets);
        }

        for (int i = 0; i < numBuckets; i++) {
            if (lowerBound >= leftBoundaries[i] && i < numBuckets-1 && upperBound < leftBoundaries[i+1]){
                double fraction = (upperBound-lowerBound) / (leftBoundaries[i+1]-leftBoundaries[i]);
                return (int) Math.round(fraction * totalFrequencies / numBuckets);
            }

            if (!first || leftBoundaries[i] >= lowerBound){
                first = true;
                if (i > 0){
                    double leftMostBucketFraction = (leftBoundaries[i] - lowerBound) / (double)(leftBoundaries[i] - leftBoundaries[i-1]);
                    result += Math.round(leftMostBucketFraction * totalFrequencies/numBuckets);
                }
            }

            if (first && !last && leftBoundaries[i] <= upperBound){

            }
        }


        return result;
    }
}
