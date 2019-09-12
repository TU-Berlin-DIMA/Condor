package Histograms;

import Synopsis.Synopsis;
import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.TreeMap;

/**
 * Class which sketch which can be merged with itself and updated in a streaming fashion.
 * Designed for streaming window applications in Flink.
 * Supports method to create an approximate Equi-Depth Histogram from the Sketch data.
 * Based on ideas in the paper: "Fast and Accurate Computation of Equi-Depth Histograms over Data Streams" - ACM International Conference Proceeding Series 2011
 *
 * @author joschavonhein
 */
public class BashHistogram implements Synopsis, Serializable {

    private int p; // precision hyper parameter
    private int numBuckets; // number of final Buckets
    private int maxNumBars; // maximum number of Bars in the sketch
    private TreeMap<Integer, Float> bars; //
    private int rightBoundary; // rightmost boundary - inclusive
    private final double MAXCOEF = 1.7;
    private double totalFrequencies; //

    private static final Logger logger = LoggerFactory.getLogger(BashHistogram.class);

    /**
     *
     * @param precision     precision hyperparameter - must be larger than 1 and can generally be below 10 - defaults is 7
     * @param numberOfFinalBuckets  number of buckets the final equi-depth histogram should have
     */
    public BashHistogram(int precision, int numberOfFinalBuckets) {
        p = precision;
        numBuckets = numberOfFinalBuckets;
        maxNumBars = numBuckets * p;
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
    public void update(Tuple2<Integer, Float> input){
        totalFrequencies += input.f1;
        double maxSize = MAXCOEF * totalFrequencies / maxNumBars; // maximum value a bar can have before it should split
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
                if (bars.size() > maxNumBars){
                    // Find Bars to Merge
                    float currentMin = Float.MAX_VALUE;
                    int index = 0;
                    for (int i = 0; i < maxNumBars - 1; i++) {
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
            update(new Tuple2<Integer, Float>((int)element, 1f)); //standard case in which just a single element is added to the sketch
        }else {
            if(element instanceof BashHistogram){
                try {
                    this.merge((BashHistogram)element);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else {
                logger.warn("update element has to be an integer or BashHistogram! - is: " + element.getClass());
            }
        }
    }

    public int getP() {
        return p;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public int getMaxNumBars() {
        return maxNumBars;
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

    /**
     * Function which creates the final equi-depth bucket boundaries and returns a standard equi-depths histogram
     * @return EquiDepthHistogram
     */
    public EquiDepthHistogram buildEquiDepthHistogram(){

        if (bars.isEmpty()){
            Log.error("no data yet! Bars is empty!");
            return null;
        }
        if (bars.size() < numBuckets){
            Log.warn("less bars than number of Buckets!");
        }else if (bars.size() < maxNumBars){
            Log.warn("less bars than maxNumBars!");
        }
        if (bars.size() == 1){ // in case there is only a single bar!
            double[] bound = {(double)bars.firstKey()};
            return new EquiDepthHistogram(bound, rightBoundary, totalFrequencies);
        }

        double[] boundaries = new double[numBuckets];
        boundaries[0] = bars.firstKey();
        int b = bars.firstKey();
        double count = bars.firstEntry().getValue();
        double idealBuckSize = totalFrequencies / numBuckets;

        for (int i = 1; i < numBuckets; i++) { // starting from 1 as first boundary is already known
            while (count <= idealBuckSize){
                if (bars.higherKey(b) != null){
                    b = bars.higherKey(b);
                    count += bars.get(b);
                }
            }
            double surplus = count-idealBuckSize;
            double rb;
            if (bars.higherKey(b) != null){
                rb = bars.higherKey(b);
            }else {
                rb = rightBoundary;
            }
            boundaries[i] = (b + (rb-b) * (1- (surplus / bars.get(b))));
            count = surplus;
        }

        return new EquiDepthHistogram(boundaries, rightBoundary, totalFrequencies);
    }

    /*
     * Methods needed for Serializability
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException{
        out.writeInt(p);
        out.writeInt(maxNumBars);
        out.writeObject(bars);
        out.writeInt(rightBoundary);
        out.writeDouble(totalFrequencies);
    }
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        p = in.readInt();
        maxNumBars = in.readInt();
        numBuckets = maxNumBars * p;
        bars = (TreeMap<Integer, Float>) in.readObject();
        rightBoundary = in.readInt();
        totalFrequencies = in.readDouble();
    }

    @Override
    public String toString() {
        return "BashHistogram{" +
                "p=" + p +
                ", numBuckets=" + numBuckets +
                ", maxNumBars=" + maxNumBars +
                ", bars=" + bars +
                ", rightBoundary=" + rightBoundary +
                ", totalFrequencies=" + totalFrequencies +
                '}';
    }

    private void readObjectNoData() throws ObjectStreamException{
        Log.error("method not implemented");
    }

}
