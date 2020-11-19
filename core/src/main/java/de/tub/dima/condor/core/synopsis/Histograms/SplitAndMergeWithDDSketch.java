package de.tub.dima.condor.core.synopsis.Histograms;

import de.tub.dima.condor.core.synopsis.Sketches.DDSketch;
import de.tub.dima.condor.core.synopsis.MergeableSynopsis;
import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * Class which maintains an error-bound equi-depth histogram in a streaming Environment using a dd sketch for quantile computation.
 * The sketch is used to periodically recompute the histogram boundaries and extract statistical properties of the data.
 * The incremental update process is done using a split and merge algorithm as proposed in the below mentioned paper.
 * The ddsketch is used to compute the median of buckets which have to be split.
 *
 * Based on the paper: "Fast Incremental Maintenance of Approximate MergeableSynopsis.Histograms" - ACM Transactions on Database Systems, Vol. V, 2002
 *
 * Our method differs from the algorithm proposed in the paper in the way that we trade the backing sample for a DDSketch.
 * The DDSketch wasn't available at the time the paper was written and provides quantile computation with better error bounds than a backing sample of the same size.
 *
 * Due to the nature of the algorithm and the merge this algorithm is most useful in a single threaded Environment (parallelism of 1)
 * and a continuous window with an evictor instead of the typical count / time based windows. This is due to fact that
 * with every merge the @equiDepthSampleCompute() function is called. The benefit compared to an algorithm which simply computes
 * an equiDepthHistogram from a sample lies in it's characteristic of beeing accurate within a given error bound at all (!) times
 * and not just at the end of a window.
 *
 * @author joschavonhein
 */
public class SplitAndMergeWithDDSketch implements MergeableSynopsis, Serializable {

    // TODO: check which error bounds apply using the ddsketch instead of backing sample
    // TODO: not yet debugged !!!

    private int maxNumBuckets; // maximum number of Bars in the sketch
    private TreeMap<Double, Double> buckets; // key: left boundary of bin - value: frequency of bin (rightboundary is the next key or rightMostBoundary)
    private Double rightMostBoundary; // rightmost boundary - inclusive
    private double totalFrequencies; //
    private DDSketch ddSketch;
    private double gamma; // hyper parameter which tunes the threshold - has to be greater than -1 and should realistically be smaller than 2
    private int threshold;

    private static final Logger logger = LoggerFactory.getLogger(SplitAndMergeWithDDSketch.class);

    /**
     * standard constructor. gamma is defaulted to 0.5 which
     * @param numBuckets    number of buckets the histogram is supposed to have
     * @param sketchAccuracy    accuracy bound of the DDSketch used to compute the quantiles - has to be between 0 and 1 -> the smaller it is, the more accurate the sketch is going to be
     */
    public SplitAndMergeWithDDSketch(Integer numBuckets, Double sketchAccuracy) {
        this(numBuckets, sketchAccuracy, 0.5);
    }

    /**
     * Constructer which additionally allows setting of the hyperparameter gamma. Should only be used by experience users.
     * As rule of thumb: if gamma is greater, less recomputes / bucket splits occur but the histogram will be less accurate (!)
     * @param numBuckets    number of buckets of the histogram
     * @param sketchAccuracy    accuracy bound of the DDSketch used to compute the quantiles
     * @param gamma     performance tuning parameter. should lie between -1 and 2 (defaults to 0.5)
     */
    public SplitAndMergeWithDDSketch(Integer numBuckets, Double sketchAccuracy, Double gamma) {
        if (gamma <= -1){
            throw new IllegalArgumentException("gamma has to be greater than -1 (!)");
        }else if (gamma > 2){
            Log.warn("Warning! Gamma > 2 which is usually ill advised as accuracy goes down as less splits occur.");
        }
        this.maxNumBuckets = numBuckets;
        buckets = new TreeMap<Double, Double>();
        totalFrequencies = 0;
        this.gamma = gamma;
        this.threshold = 3; // initially set the threshold to 3 (value it should have if there is only a single value)
        ddSketch = new DDSketch(sketchAccuracy, 2048); // instantiate DDSketch with given accuracy and a maximum number of Bins fo 2048 (reasonable value according to the authors of the sketch
    }

    /**
     * private update method called by the public update (tuple) and merge function.
     * Adds frequencies for a certain value to the Histogram.
     *
     * @param input attribute value
     */
    public void update(Object input){

        Double next;
        if (input instanceof Number){
            next = ((Number) input).doubleValue();
        }else {
            throw new IllegalArgumentException("input has to be a number!");
        }
        ddSketch.update(next);
        totalFrequencies ++;
        Double binFrequency;
        // 1st step: add frequency to existing bin
        if (buckets.isEmpty()){ // special case for first input
            buckets.put(next, 1d);
            rightMostBoundary = next;
        }else { // usual case if a bucket already exists
            Double key;
            if (buckets.floorKey(next) != null) {
                key = buckets.floorKey(next);
                if (key == buckets.lastKey() && next > rightMostBoundary){ // if next is greater than current right boundary it becomes the new boundary
                    rightMostBoundary = next;
                }
                binFrequency = buckets.merge(key, 1d, (a,b) -> a + b);
            } else{ // element is new leftmost boundary
                double old_key = buckets.ceilingKey(next);
                binFrequency = buckets.get(old_key) + 1;
                buckets.remove(old_key);   // remove old bin
                key = next;
                buckets.put(key, binFrequency); // create new bin with new left boundary
            }

            if (binFrequency >= threshold){ // check whether the bucket frequency exceeds the threshold and has to be split
                // 2nd step: split the bucket whose frequency exceeds the threshold
                splitBucket(key);

                while (buckets.size() > maxNumBuckets){ // check whether buckets have to be merged after split

                    // 2nd step: find the two adjacent buckets where the sum of frequencies is minimal
                    // if their sum exceeds the threshold recompute from sample!
                    if (buckets.size() > maxNumBuckets){
                        double currentMin = Double.MAX_VALUE;
                        double k = buckets.firstKey();  // key of the bucket to keep
                        double n = 0; // key of bucket to remove

                        while (buckets.higherKey(k) != null){
                            n = buckets.higherKey(k);    // key of the bucket to remove
                            currentMin = currentMin > buckets.get(k) + buckets.get(n) ? buckets.get(k) + buckets.get(n) : currentMin;
                            k = n;
                        }

                        if (currentMin < threshold){    // if sum of frequencies of buckets to be merged doesn't exceed the threshold merge can happen
                            buckets.remove(n);
                            buckets.replace(k, currentMin);
                        }else { // otherwise recompute from DDSketch
                            equiDepthSampleCompute();
                            threshold = (int)Math.round(totalFrequencies * (2+gamma));
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * private method to recursively split buckets until all buckets have sizes smaller than the threshold.
     * Only exception is when Buckets can't be split further when the computed median is the left bucket boundary.
     *
     * @param key   left boundary of bucket to split
     */
    private void splitBucket(double key){

        double nextLeftBound = medianForBucket(key);
        double newFrequency = buckets.get(key) / 2;
        if (nextLeftBound != key){
            buckets.replace(key, newFrequency);
            buckets.put(nextLeftBound, newFrequency);
            if (newFrequency >= threshold){
                splitBucket(key);
                splitBucket(nextLeftBound);
            }
        }
    }


    /**
     * Return frequency of a range query. lower bound is inclusive, upper bound is exclusive
     *
     * The intra-bucketing scheme is the continuous value assumption
     *
     * @param lowerBound    inclusive
     * @param upperBound    upper bound of the range query, exclusive
     * @return  estimated result frequency of the range query
     */
    public double rangeQuery(double lowerBound, double upperBound){

        // TODO: handle case when lowerBound and upperBound are the same (needs figuring out if returning 0 is fine or not in all cases)

        if (upperBound < lowerBound){
            throw new IllegalArgumentException("upper Bound can't be smaller than lower Bound!");
        }else if (upperBound < buckets.firstKey() || lowerBound > rightMostBoundary || buckets.size() == 0){   //
            return 0;
        }else{
            double bucketWidth; // helper variable which determines the width of a bucket containing range query bounds
            double fraction;
            double result = 0d;
            if (upperBound > rightMostBoundary){
                upperBound = rightMostBoundary;
            }
            if (lowerBound < buckets.firstKey()){ // make sure buckets.floorKey(lowerBound) returns a non-Null value
                lowerBound = buckets.firstKey();
            }
            if (buckets.floorKey(lowerBound) == buckets.floorKey(upperBound)){  // case: range is contained in a single bucket
                bucketWidth = buckets.higherKey(lowerBound) == null ? rightMostBoundary - buckets.floorKey(lowerBound) : buckets.higherKey(lowerBound) - buckets.floorKey(lowerBound);
                return (upperBound - lowerBound) / bucketWidth * buckets.floorEntry(lowerBound).getValue();
            }

            // 1st step: add left bucket part - can't be the last bucket, therefore no null-check necessary
            Map.Entry<Double, Double> lowerBoundBucket = buckets.floorEntry(lowerBound);
            bucketWidth = buckets.higherKey(lowerBound) - lowerBoundBucket.getKey();
            fraction = (buckets.higherKey(lowerBound) - lowerBound) / bucketWidth;
            result += fraction * lowerBoundBucket.getValue();

            while (buckets.higherKey(lowerBound) < buckets.floorKey(upperBound)){   // add full buckets in query range to result
                lowerBound = buckets.higherKey(lowerBound);
                result += buckets.get(lowerBound);
            }

            // 1st step: add left bucket part - can't be the last bucket, therefore no null-check necessary
            Map.Entry<Double, Double> upperBoundBucket = buckets.floorEntry(upperBound);
            bucketWidth = buckets.higherKey(upperBound) == null ? rightMostBoundary - upperBoundBucket.getKey() : buckets.higherKey(upperBound) - upperBoundBucket.getKey();
            fraction = (upperBound - upperBoundBucket.getKey()) / bucketWidth;
            result += fraction * upperBoundBucket.getValue();

            return result;
        }
    }

    /**
     * Method to completely recompute the histogram boundaries on the basis of the backing sample
     *
     * The leftmost and rightmost boundary are an exception to this and are kept as they represent 100% accurate values.
     */
    private void equiDepthSampleCompute(){

        Double bucketSize =  totalFrequencies / maxNumBuckets;
        double leftMostBoundary = buckets.firstKey();
        buckets.clear();
        buckets.put(leftMostBoundary, bucketSize);  // the first bucket keeps the leftmost boundary accurate
        for (int i = 1; i < maxNumBuckets; i++) {
            double leftBoundary = ddSketch.getValueAtQuantile((double) i / maxNumBuckets);
            buckets.merge(leftBoundary, bucketSize, (a, b) -> a + b); // add the new bucket - if two buckets have the same bucket boundary they get merged!
        }
    }

    /**
     * computes the median for the bucket with the given left boundary
     * @param leftBoundary  left boundary of the given bucket
     * @return  the median value of the bucket sample
     */
    private Double medianForBucket(Double leftBoundary){

        double freq = buckets.get(leftBoundary) / 2; // needs to add half of the bucket to the frequencies in order to be able to compute the median later
        double key = buckets.firstKey();
        while (key < leftBoundary){
            freq += buckets.get(key);
            key = buckets.higherKey(key);
        }
        double quantile = freq / totalFrequencies;
        return ddSketch.getValueAtQuantile(quantile);
    }

    public TreeMap<Double, Double> getBuckets() {
        return buckets;
    }

    public Double getRightMostBoundary() {
        return rightMostBoundary;
    }

    public double getTotalFrequencies() {
        return totalFrequencies;
    }

    public DDSketch getDdSketch() {
        return ddSketch;
    }

    @Override
    public SplitAndMergeWithDDSketch merge(MergeableSynopsis other) {
        if (other instanceof SplitAndMergeWithDDSketch){

            ddSketch = ddSketch.merge(((SplitAndMergeWithDDSketch) other).getDdSketch());
            this.rightMostBoundary = this.rightMostBoundary < ((SplitAndMergeWithDDSketch) other).getRightMostBoundary()
                    ? ((SplitAndMergeWithDDSketch) other).getRightMostBoundary() : this.rightMostBoundary;
            double leftMostBoundary = buckets.firstKey() < ((SplitAndMergeWithDDSketch) other).getBuckets().firstKey()
                    ? buckets.firstKey() : ((SplitAndMergeWithDDSketch) other).buckets.firstKey();
            buckets.put(leftMostBoundary, 1d); // keep the leftmost bucket with arbitrary frequency as those will get recomputed anyway
            equiDepthSampleCompute();   // completely recompute the histogram based on the merged backing sample to make sure the histogram conforms to the accuracy guarantees

            logger.info("merge complete");
            return this;
        }else {
            throw new IllegalArgumentException("MergeableSynopsis to be merged must be of the same type!");
        }
    }

    @Override
    public String toString() {
        return "SplitAndMergeWithDDSketch{" +
                "maxNumBuckets=" + maxNumBuckets +
                ", buckets=" + buckets +
                ", rightMostBoundary=" + rightMostBoundary +
                ", totalFrequencies=" + totalFrequencies +
                ", ddSketch=" + ddSketch +
                ", gamma=" + gamma +
                ", threshold=" + threshold +
                '}';
    }

    /*
     * Methods needed for Serializability
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException{
        out.writeInt(maxNumBuckets);
        out.writeObject(buckets);
        out.writeDouble(rightMostBoundary);
        out.writeDouble(totalFrequencies);
        out.writeObject(ddSketch);
        out.writeDouble(gamma);
        out.writeInt(threshold);
    }
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        maxNumBuckets = in.readInt();
        buckets = (TreeMap<Double, Double>) in.readObject();
        rightMostBoundary = in.readDouble();
        totalFrequencies = in.readDouble();
        ddSketch = (DDSketch) in.readObject();
        gamma = in.readDouble();
        threshold = in.readInt();
    }

    private void readObjectNoData() throws ObjectStreamException{
        Log.error("method not implemented");
    }
}
