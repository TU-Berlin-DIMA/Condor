package Histograms;

import java.util.TreeMap;

/**
 * Classical Equi-Depth Histogram based on sorted input (relatively trivial case)
 *
 * @author joschavonhein
 */
public class EquiDepthHistogram {

    double[] leftBoundaries; // boundaries are inclusive
    int rightmostBoundary; // rightmost boundary - inclusive
    int totalFrequencies;
    int numBuckets;

    /**
     * Constructor of the EquiDepthHistogram. As it is not possible to create this kind of histogram in one pass
     * over the input stream the data has to be stored in a TreeMap during the Window.
     * @param sortedInput   the TreeMap with the key, frequency pairs - due to TreeMap properties sortedness is guaranteed
     * @param numBuckets    the number of buckets the histogram should have
     * @param total         the total amount of frequencies of the histogram (easier to compute during the window itself
     *                      instead of iterating over every single value again
     */
    public EquiDepthHistogram(TreeMap<Integer, Integer> sortedInput, int numBuckets, int total) {

        this.numBuckets = numBuckets;
        if(sortedInput.size() < numBuckets){
            this.numBuckets = sortedInput.size();   // number of buckets cannot exceed actual number of input items
        }
        this.leftBoundaries = new double[this.numBuckets];
        this.totalFrequencies = total;
        int bucketSize = total / this.numBuckets;
        int currentLeftBoundary = sortedInput.firstKey();
        leftBoundaries[0] = currentLeftBoundary;
        this.rightmostBoundary = sortedInput.lastKey();
        int tempBucketSize = 0;
        int index = 1;


        for (int i = 0; i < sortedInput.size()-1; i++) {
            tempBucketSize += sortedInput.get(currentLeftBoundary);
            currentLeftBoundary = sortedInput.higherKey(currentLeftBoundary);
            if (tempBucketSize > bucketSize){
                leftBoundaries[index] = currentLeftBoundary;
                index++;
                int extremeOverflow = (tempBucketSize-bucketSize) / bucketSize;
                for (int j = 0; j < extremeOverflow; j++) {

                }
                tempBucketSize = tempBucketSize % bucketSize;
            }
        }
    }


    /**
     * Return frequency of a range query. lower bound is inclusive, upper bound is exclusive
     * @param lowerBound
     * @param upperBound
     * @return
     */
    public int rangeQuery(int lowerBound, int upperBound){

        if (upperBound - lowerBound < 0){
            throw new IllegalArgumentException("upper Bound can't be smaller than lower Bound!");
        }

        boolean first = false;
        boolean last = false;
        int bucketsInRange = 0;
        int result = 0;

        // edge case that lower Bound is in last Bucket
        if (lowerBound >= leftBoundaries[numBuckets-1]){
            double fraction = Math.min(rightmostBoundary, upperBound)/(rightmostBoundary-leftBoundaries[numBuckets-1]);
            return (int)Math.round(fraction*totalFrequencies/numBuckets);
        }

        for (int i = 0; i < numBuckets; i++) {
            // edge case that range is contained in a single bucket
            if (lowerBound >= leftBoundaries[i] && i < numBuckets-1 && upperBound < leftBoundaries[i+1]){
                double fraction = (upperBound-lowerBound) / (leftBoundaries[i+1]-leftBoundaries[i]);
                return (int) Math.round(fraction * totalFrequencies / numBuckets);
            }

            // add leftmost bucket part to query result
            if (!first || leftBoundaries[i] >= lowerBound){
                first = true;
                if (i > 0){
                    double leftMostBucketFraction = (leftBoundaries[i] - lowerBound) / (double)(leftBoundaries[i] - leftBoundaries[i-1]);
                    result += Math.round(leftMostBucketFraction * totalFrequencies/numBuckets);
                }
            }

            // count amount of fully contained buckets in range
            if (first && !last){
                if (upperBound < leftBoundaries[i]){
                    last = true;
                    double rightmostBucketFraction = (upperBound - leftBoundaries[i-1]) / (double) (leftBoundaries[i] - leftBoundaries[i-1]);
                    result += rightmostBucketFraction * totalFrequencies/numBuckets;
                }
                bucketsInRange++;
            }
        }
        result += bucketsInRange*totalFrequencies/numBuckets;
        return result;
    }
}
