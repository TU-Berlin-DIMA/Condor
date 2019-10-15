package Histograms;

import Sampling.FlinkVersion.ReservoirSampler;
import Synopsis.Synopsis;
import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
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


    private int maxNumBuckets; // maximum number of Bars in the sketch
    private TreeMap<Integer, Float> buckets; //
    private int rightBoundary; // rightmost boundary - inclusive
    private double totalFrequencies; //
    private ReservoirSampler sample;
    private double countError; // with probability of at least getErrorMinProbability() this is the maximum error the counts of this approximate histogram varies from the grue equi depth histogram
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
        maxNumBuckets = numberOfFinalBuckets;
        buckets = new TreeMap<>();
        totalFrequencies = 0;
        this.countError = countError;
        this.sampleSize = sampleSize;
        this.threshold = 3; // initially set the threshold to 3 (value it should have if there is only a single valu)
    }

    public double getErrorMinProbability() {
        return 1 - 1 / Math.pow(buckets.size(), Math.sqrt(c)-1) - 1 / Math.pow(totalFrequencies / (2+GAMMA), 1/3);
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
        // 1st step: add frequency to existing bin
        if (buckets.isEmpty()){ // special case for first input
            buckets.put(next, input.f1);
            rightBoundary = next;
        }else { // usual case if a bucket already exists
            int key;
            if (buckets.floorKey(next) != null) {
                key = buckets.floorKey(next);
                if (key == buckets.lastKey() && next > rightBoundary){ // if key greater than current right boundary it becomes the new boundary
                    rightBoundary = next;
                }
                binFrequency = buckets.merge(key, input.f1, (a,b) -> a + b);
            } else{ // element is new leftmost boundary
                key = buckets.ceilingKey(next);
                binFrequency = buckets.get(key) + input.f1;
                buckets.remove(key);   // remove old bin
                buckets.put(next, binFrequency); // create new bin with new left boundary
            }

            if (binFrequency >= threshold){ // check whether the bucket frequency exceeds the threshold and has to be split
                int nextRightBound = key == buckets.lastKey() ? rightBoundary : buckets.higherKey(key); // lookup the right boundary of the bucket to be split
                int nextLeftBound = medianForBucket(key, nextRightBound); // set the median of the sample to be the left boundary of the newly created bucket

                if (buckets.size() == maxNumBuckets && nextLeftBound != key){ // check whether buckets have to be merged after split && the split can happen

                    // 2nd step: find the two adjacent buckets where the sum of frequencies is minimal
                    // if their sum exceeds the threshold recompute from sample!
                    if (buckets.size() > maxNumBuckets){
                        float currentMin = Float.MAX_VALUE;
                        int index = 0;
                        for (int i = 0; i < maxNumBuckets - 1; i++) {
                            if (buckets.get(i) + buckets.get(i+1) < currentMin){
                                index = i;
                                currentMin = buckets.get(i) + buckets.get(i+1);
                            }
                        }
                        if (currentMin < threshold){
                            buckets.remove(index+1);
                            buckets.replace(index, currentMin);
                        }else {
                            equiDepthSampleCompute();   // recompute the histogram from the backing sample
                        }
                    }
                }

                // 3rd step: split the bucket whose frequency exceeds the threshold
                binFrequency /= 2;
                if (nextLeftBound != key){ // edge case in which boundaries are too close to each other -> don't split
                    buckets.replace(key, binFrequency);
                    buckets.put(nextLeftBound, binFrequency);
                }
            }
        }
    }

    /**
     * Method to completely recompute the histogram boundaries on the basis of the backing sample
     *
     * The leftmost and rightmost boundary are an exception to this and are kept as they represent 100% accurate values.
     */
    private void equiDepthSampleCompute(){

        Integer[] sampleArray = (Integer[]) sample.getSample();
        int sampleSize = sample.getSampleSize(); // actual amount of values currently in the backing sample
        int actualNumBuckets = maxNumBuckets;
        if(sampleSize < maxNumBuckets){
            actualNumBuckets = sampleSize;   // number of buckets cannot exceed actual number of input items
        }
        float bucketSize = (float) (totalFrequencies / actualNumBuckets);
        Arrays.sort(sampleArray); // sort the sample
        sampleArray[0] = buckets.firstKey(); // replace the first entry with the leftmost boundary to make sure that the actual boundaries are kept
        buckets.clear();
        int index;

        for (int i = 0; i < actualNumBuckets; i++) {
            index = sampleSize / actualNumBuckets * i;
            buckets.put(sampleArray[index], bucketSize); // add the current bucket
        }
    }

    /**
     * computes the median for the given bucket
     * @param leftBoundary  left boundary of the given bucket - inclusive
     * @param rightBoundary right boundary of the given bucket - exclusive
     * @return  the median value of the bucket sample
     */
    private int medianForBucket(int leftBoundary, int rightBoundary){
        Integer[] sampleArray = (Integer[]) sample.getSample();
        Arrays.sort(sampleArray);
        int leftIndex = Arrays.binarySearch(sampleArray, leftBoundary);
        int rightIndex = Arrays.binarySearch(sampleArray, rightBoundary);
        int bucketCount = rightIndex - leftIndex;
        return sampleArray[rightIndex-bucketCount/2];
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

    public int getMaxNumBuckets() {
        return maxNumBuckets;
    }

    public TreeMap<Integer, Float> getBuckets() {
        return buckets;
    }

    public int getRightBoundary() {
        return rightBoundary;
    }

    public double getTotalFrequencies() {
        return totalFrequencies;
    }

    public ReservoirSampler getSample() {
        return sample;
    }

    @Override
    public SplitAndMergeWithBackingSample merge(Synopsis other) {
        if (other instanceof SplitAndMergeWithBackingSample){

            sample = sample.merge(((SplitAndMergeWithBackingSample) other).getSample());
            this.rightBoundary = this.rightBoundary < ((SplitAndMergeWithBackingSample) other).getRightBoundary()
                    ? ((SplitAndMergeWithBackingSample) other).getRightBoundary() : this.rightBoundary;
            int leftmostBoundary = buckets.firstKey() < ((SplitAndMergeWithBackingSample) other).getBuckets().firstKey()
                    ? buckets.firstKey() : ((SplitAndMergeWithBackingSample) other).getBuckets().firstKey();
            buckets.put(leftmostBoundary, 1f); // make sure the leftmost boundary is stored in the buckets, the frequency doesn't matter since it will get cleared in equiDepthSample compute anyway
            equiDepthSampleCompute();   // completely recompute the histogram based on the merged backing sample to make sure the histogram conforms to the accuracy guarantees

            logger.info("merge complete");
            return this;
        }else {
            throw new IllegalArgumentException("Synopsis to be merged must be of the same type!");
        }
    }


    /*
     * Methods needed for Serializability
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException{
        out.writeInt(maxNumBuckets);
        out.writeObject(buckets);
        out.writeInt(rightBoundary);
        out.writeDouble(totalFrequencies);
    }
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        maxNumBuckets = in.readInt();
        buckets = (TreeMap<Integer, Float>) in.readObject();
        rightBoundary = in.readInt();
        totalFrequencies = in.readDouble();
    }

    @Override
    public String toString() {
        return "SplitAndMergeWithBackingSample{" +
                ", maxNumBuckets=" + maxNumBuckets +
                ", buckets=" + buckets +
                ", rightBoundary=" + rightBoundary +
                ", totalFrequencies=" + totalFrequencies +
                '}';
    }

    private void readObjectNoData() throws ObjectStreamException{
        Log.error("method not implemented");
    }
}
