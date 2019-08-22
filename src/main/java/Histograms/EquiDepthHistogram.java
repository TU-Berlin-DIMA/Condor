package Histograms;

import com.oracle.tools.packager.mac.MacAppBundler;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;
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
        int result = 0;
        return result;
    }
}
