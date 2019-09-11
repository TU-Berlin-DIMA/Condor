package Histograms;

import Synopsis.Synopsis;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.TreeMap;

public class BashHistogram implements Synopsis, Serializable {

    private int p; // precision hyper parameter
    private int numBuckets; // number of final Buckets
    private int numBars; // maximum number of Bars in the sketch
    private TreeMap<Integer, Float> bars; //
    private int rightBoundary; // rightmost boundary - inclusive
    private final double MAXCOEF = 1.7;
    private double totalFrequencies; //

    private static final Logger logger = LoggerFactory.getLogger(BashHistogram.class);

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

    /**
     * private update method called by the public update (tuple) and merge function.
     * Adds frequencies for a certain value to the BASH Histogram.
     *
     * @param input f0: value, f1: corresponding frequency
     */
    private void update(Tuple2<Integer, Float> input){
        totalFrequencies += input.f1;
        double maxSize = MAXCOEF * totalFrequencies / numBars; // maximum value a bar can have before it should split
        float binFrequency;
        int next = input.f0;
        if (bars.isEmpty()){
            bars.put(next, input.f1);
            rightBoundary = next;
        }else {
            int key;
            if (bars.floorKey(next) != null) {
                key = bars.floorKey(next);
                if (key == bars.lastKey()){
                    rightBoundary = next;
                }
                binFrequency = bars.get(key) + input.f1;
                bars.replace(key, binFrequency);
            } else{ // element is new leftmost boundary
                key = bars.ceilingKey(next);
                binFrequency = bars.get(key) + input.f1;
                bars.remove(key);   // remove old bin
                key = next;
                bars.put(key, binFrequency); // create new bin with new left boundary
            }
            while (binFrequency > maxSize){ // split bins while
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
    }

    @Override
    public void update(Object element) {
        if (element instanceof Integer){
            update(new Tuple2<>((int)element, 1)); //standard case in which just a single element is added to the sketch
        }else {
            if(element instanceof BashHistogram){
                try {
                    this.merge((BashHistogram)element);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else {
                logger.warn("update element has to be an integer or BashHistogram!");
            }
        }
    }

    public int getP() {
        return p;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public int getNumBars() {
        return numBars;
    }

    public TreeMap<Integer, Float> getBars() {
        return bars;
    }

    public int getRightBoundary() {
        return rightBoundary;
    }

    public double getTotalFrequencies() {
        return totalFrequencies;
    }

    @Override
    public BashHistogram merge(Synopsis other) throws Exception {
        if (other instanceof BashHistogram){
            BashHistogram o = (BashHistogram) other;
            BashHistogram base;
            if (this.totalFrequencies > o.getTotalFrequencies()) {
                base = this;
            } else {
                base = o;
                o = this;
            }
            TreeMap<Integer, Float> otherBars = o.getBars();
            TreeMap<Integer, Float> baseBars = base.getBars();
            for (int i = 0; i < otherBars.size(); i++) { // add every bar of the other histogram to the base histogram using appropriate weights
                int otherLB = otherBars.firstKey();
                float frequency = otherBars.remove(otherLB);
                int otherUB;
                if (otherBars.isEmpty()) {otherUB = o.rightBoundary;}
                else {otherUB = otherBars.firstKey();}
                int baseLB = baseBars.floorKey(otherLB);
                int baseUB;
                if(baseBars.ceilingKey(otherLB) != null) {baseUB = baseBars.ceilingKey(otherLB);}
                else {baseUB = base.rightBoundary;}
                while (baseLB < otherUB){
                    int coveredBaseBar = Math.min(otherUB, baseUB) - Math.max(otherLB, baseLB);
                    int otherBarWidth = otherUB - otherLB;
                    float weightedFrequency = frequency * coveredBaseBar / otherBarWidth;
                    base.update(new Tuple2<>(baseLB, weightedFrequency)); // add weighted fraction of other bar to base bar

                    // change base boundaries to next bar
                    baseLB = baseUB;
                    if(baseBars.ceilingKey(baseUB) != null) {baseUB = baseBars.ceilingKey(baseUB);}
                    else {baseUB = base.rightBoundary;}
                }
            }

            return base;
        }else {
            throw new IllegalArgumentException("Synopsis to be merged must be of the same type!");
        }
    }

    /*
     * Methods needed for Serializability
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException{
        // TODO
    }
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        // TODO
    }
    private void readObjectNoData() throws ObjectStreamException{
        // TODO
    }

}
