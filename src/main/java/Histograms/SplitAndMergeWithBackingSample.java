package Histograms;

import Sampling.ReservoirSampler;
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
 * Class which maintains an error-bound equi-depth histogram in a streaming environment using a backing sample.
 * The backing sample is used to periodically recompute the histogram boundaries and extract statistical properties of the data.
 * The incremental update process is done using a split and merge algorithm as proposed in the below mentioned paper.
 * The backing sample is used to compute the median of buckets which have to be split.
 *
 * Based on the paper: "Fast Incremental Maintenance of Approximate Histograms" - ACM Transactions on Database Systems, Vol. V, 2002
 *
 * @author joschavonhein
 */
public class SplitAndMergeWithBackingSample implements Synopsis, Serializable {


    private int maxNumBars; // maximum number of Bars in the sketch
    private TreeMap<Integer, Float> bars; //
    private int rightBoundary; // rightmost boundary - inclusive
    private double totalFrequencies; //
    private ReservoirSampler sample;
    private double countError;
    private int sampleSize;
    private double c;
    private final double GAMMA = 0.5; // hyper parameter which tunes the threshold - has to be greater than -1 and should realistically be smaller than 2
    private int threshold;
    private static EquiDepthHistBuilder builder = new EquiDepthHistBuilder();

    private static final Logger logger = LoggerFactory.getLogger(SplitAndMergeWithBackingSample.class);

    /**
     * initialisation method used by all constructors
     * @param countError    maximum error: standard deviation of the bucket counts from the actual number of elements
     *                     in each buckets, normalized with respect to the mean bucket count.
     * @param sampleSize    maximum size of the backing sample used to recompute the histogram and the median of buckets to be split
     */
    private void init(int numberOfFinalBuckets, double countError, int sampleSize) {
        maxNumBars = numberOfFinalBuckets;
        bars = new TreeMap<>();
        totalFrequencies = 0;
    }

    public double getErrorMinProbability() {
        return 1 - 1 / Math.pow(bars.size(), Math.sqrt(c)-1) - 1 / Math.pow(totalFrequencies / (2+GAMMA), 1/3);
    }

    public SplitAndMergeWithBackingSample(Integer numBuckets, Double countError) {
        c = 1 / (Math.pow(countError, 6) * Math.log(numBuckets));
        sampleSize = (int) (numBuckets * c * Math.pow(Math.log(numBuckets), 2));
        init( numBuckets, countError, sampleSize);
    }

    public SplitAndMergeWithBackingSample(Integer numBuckets, Integer sampleSize){
        c = sampleSize / (numBuckets * Math.pow(Math.log(numBuckets),2));
        countError = 1 / Math.pow((c * Math.log(numBuckets)), 1/6);
        init(numBuckets, countError, sampleSize);
    }


    /**
     * private update method called by the public update (tuple) and merge function.
     * Adds frequencies for a certain value to the Histogram.
     *
     * @param input f0: value, f1: corresponding frequency
     */
    public void update(Tuple2<Integer, Float> input){
        totalFrequencies += input.f1;
        float binFrequency;
        int next = input.f0;
        if (bars.isEmpty()){
            bars.put(next, input.f1);
            rightBoundary = next;
        }else {
            int key;
            if (bars.floorKey(next) != null) {
                key = bars.floorKey(next);
                if (key == bars.lastKey() && next > rightBoundary){ // if key greater than current right boundary it becomes the new boundary
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
            if (bars.size() == maxNumBars){
                /**
                 * Merge the two smallest adjacent buckets - when their sum exceeds the Threshold recompute from Sample
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
                    if (currentMin < threshold){
                        bars.remove(index+1);
                        bars.replace(index, currentMin);
                    }else {
                        // TODO: call recompute from Sample
                    }
                }
            }
            while (binFrequency >= threshold){ // split bins while frequency is greater than the threshold
                /**
                 * Split Bin
                 */
                binFrequency /= 2;
                int nextRightBound = key == bars.lastKey() ? rightBoundary : bars.higherKey(key);
                int nextLeftBound = (nextRightBound+key) / 2;
                if (nextLeftBound != key){ // edge case in which boundaries are too close to each other -> don't split
                    bars.replace(key, binFrequency);
                    bars.put(nextLeftBound, binFrequency);
                }

            }
        }
    }

    private void equiDepthSampleCompute(){
        TreeMap<Integer, Integer> map = new TreeMap();
        Integer[] s = (Integer[]) sample.getSample();
        int sampleSize = 0;
        for (int f: s) {
            map.merge(f, 1, (a,b) -> a + b);
            sampleSize += f;
        }
        if(map.size() < maxNumBars){
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

    @Override
    public void update(Object element) {
        if (element instanceof Integer){
            update(new Tuple2<Integer, Float>((int)element, 1f)); //standard case in which just a single element is added to the sketch
        }else {
            if(element instanceof SplitAndMergeWithBackingSample){
                try {
                    this.merge((SplitAndMergeWithBackingSample)element);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else {
                logger.warn("update element has to be an integer or SplitAndMergeWithBackingSample! - is: " + element.getClass());
            }
        }
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
    public SplitAndMergeWithBackingSample merge(Synopsis other) throws Exception {
        if (other instanceof SplitAndMergeWithBackingSample){
            SplitAndMergeWithBackingSample o = (SplitAndMergeWithBackingSample) other;
            SplitAndMergeWithBackingSample base;
            if (this.totalFrequencies > o.getTotalFrequencies()) {
                base = this;
            } else {
                base = o;
                o = this;
            }
            TreeMap<Integer, Float> otherBars = o.getBars();
            TreeMap<Integer, Float> baseBars = base.getBars();
            for (int i = 0; i < otherBars.size(); i++) { // add every bar of the other histogram to the base histogram using appropriate weights
                // Set base and other lower and upper bounds correctly
                int otherLB = otherBars.firstKey();
                float frequency = otherBars.remove(otherLB);
                int otherUB = otherBars.isEmpty() ? o.rightBoundary : otherBars.firstKey();
                int baseLB;
                int baseUB;
                if(baseBars.floorKey(otherLB) != null){ // case in which base bar left boundary is smaller than other left boundary
                    baseLB = baseBars.floorKey(otherLB);
                    baseUB = baseBars.higherKey(baseLB) == null ? base.rightBoundary : baseBars.higherKey(baseLB);
                }else { // case in which other bar left boundary is smaller than base left boundary
                    baseLB = otherLB; // change the leftmost boundary of the base in case the other lower bound is smaller
                    baseUB = baseBars.higherKey(baseBars.firstKey()) == null ? base.rightBoundary : baseBars.higherKey(baseBars.firstKey());
                }
                // loop through all base bars which cover area of the current other bar
                while (baseLB < otherUB){
                    int coveredBaseBar = Math.min(otherUB, baseUB) - Math.max(otherLB, baseLB);
                    int otherBarWidth = otherUB - otherLB;
                    float weightedFrequency = frequency * coveredBaseBar / otherBarWidth;

                    if (baseBars.lastKey() == baseLB){ // the rightmost base bar has to be updated with the upper bound of the other bar to facilitate changing boundaries
                        base.update(new Tuple2<>(otherUB, weightedFrequency));
                    }else {
                        base.update(new Tuple2<>(baseLB, weightedFrequency)); // standard case of adding weighted fraction of other bar to base bar
                    }

                    // change base boundaries to next bar
                    baseLB = baseUB;
                    baseUB = baseBars.higherKey(baseUB) != null ? baseBars.higherKey(baseUB) : base.rightBoundary;
                }
            }
            logger.info("merge complete");
            return base;
        }else {
            throw new IllegalArgumentException("Synopsis to be merged must be of the same type!");
        }
    }



    /*
     * Methods needed for Serializability
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException{
        out.writeInt(maxNumBars);
        out.writeObject(bars);
        out.writeInt(rightBoundary);
        out.writeDouble(totalFrequencies);
    }
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        maxNumBars = in.readInt();
        bars = (TreeMap<Integer, Float>) in.readObject();
        rightBoundary = in.readInt();
        totalFrequencies = in.readDouble();
    }

    @Override
    public String toString() {
        return "SplitAndMergeWithBackingSample{" +
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
