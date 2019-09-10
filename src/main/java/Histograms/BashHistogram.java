package Histograms;

import Synopsis.Synopsis;

import java.util.TreeMap;

public class BashHistogram implements Synopsis {

    private int p; // precision hyper parameter
    private int numBuckets; // number of final Buckets
    private int numBars; // maximum number of Bars in the sketch
    private TreeMap<Integer, Float> bars; //
    private int rightBoundary; // rightmost boundary - inclusive
    private final double MAXCOEF = 1.7;
    private int totalFrequencies; //

    public BashHistogram(int precision, int numberOfFinalBuckets) {
        p = precision;
        numBuckets = numberOfFinalBuckets;
        numBars = numBuckets * p;
        bars = new TreeMap<>();
        totalFrequencies = 0;
    }

    public BashHistogram(int numBuckets) {
        this(7, numBuckets);
    }

    @Override
    public void update(Object element) {
        int next;
        if (element instanceof Integer){
            totalFrequencies++;
            double maxSize = MAXCOEF * totalFrequencies / numBars; // maximum value a bar can have before it should split
            float binFrequency;
            next = (int) element;
            if (bars.isEmpty()){
                bars.put(next, 1f);
                rightBoundary = next;
            }else {
                int key;
                if (bars.floorKey(next) != null) {
                    key = bars.floorKey(next);
                    if (key == bars.lastKey()){
                        rightBoundary = next;
                    }
                    binFrequency = bars.get(key) + 1;
                    bars.replace(key, binFrequency);
                } else{ // element is new leftmost boundary
                    key = bars.ceilingKey(next);
                    binFrequency = bars.get(key) +1;
                    bars.remove(key);   // remove old bin
                    key = next;
                    bars.put(key, binFrequency); // create new bin with new left boundary
                }
                if (binFrequency > maxSize){
                    /**
                     * Split Bin
                     */
                    binFrequency /= 2;
                    int nextRightBound;
                    if (key == bars.lastKey()){
                        nextRightBound = rightBoundary;
                    }else{
                        nextRightBound = bars.higherKey(key);
                    }
                    int nextLeftBound = (nextRightBound+key) / 2;
                    if (nextLeftBound != key){ // edge case in which boundaries are too close to each other -> don't split
                        bars.replace(key, binFrequency);
                        bars.put(nextLeftBound, binFrequency);
                    }
                    /**
                     * Merge the two smallest adjacent bars
                     */
                    if (bars.size() > numBars){
                        // Find Bars to Merge
                        float currentMin = Float.MAX_VALUE;
                        int index = 0;
                        for (int i = 0; i < numBars - 1; i++) {
                            if (bars.get(i) + bars.get(i+1) < currentMin){
                                index = i;
                                currentMin = bars.get(i) + bars.get(i+1);
                            }
                        }
                        bars.remove(index+1);
                        bars.replace(index, currentMin);
                    }
                }
            }

        }else {
            // TODO: exception handling
        }
    }

    @Override
    public Synopsis merge(Synopsis other) throws Exception {
        // TODO: implement
        return null;
    }

}
