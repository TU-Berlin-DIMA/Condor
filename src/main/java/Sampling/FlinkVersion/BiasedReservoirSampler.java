package Sampling.FlinkVersion;

import Synopsis.Synopsis;
import org.apache.flink.util.XORShiftRandom;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Implementation of the Biased Reservoir Sampling algorithm with a given sample size.
 * (@href http://charuaggarwal.net/sigreservoir.pdf)
 * The idea is to give more priority to the newest incoming elements adding them always to the sample in
 * contrast to the traditional Reservoir Sampler. The probability that this element is simply appended to
 * the sample or replace an element of the sample is given by actualSize/sampleSize. Meaning that once the sample
 * has reached the desired size the probability of an element replacing an already existing sample will be equal to 1.
 *
 * @param <T> the type of elements maintained by this sampler
 *
 * @author Rudi Poepsel Lemaitre
 */
public class BiasedReservoirSampler<T> implements Synopsis<T>, Serializable {

    private T sample[];
    private int sampleSize;
    private XORShiftRandom rand;
    private int actualSize;
    private int merged = 1;
    private LinkedList<Integer> latestPositions;

    /**
     * Construct a new empty Biased Reservoir Sampler with a bounded size.
     *
     * @param sampleSize
     */
    public BiasedReservoirSampler(Integer sampleSize) {
        this.sample = (T[]) new Object[sampleSize];
        this.sampleSize = sampleSize;
        this.rand = new XORShiftRandom();
        this.actualSize = 0;
        this.latestPositions = new LinkedList<>();
    }

    /**
     * Add the incoming element to the sample. The probability that this element is simply appended to
     * the sample or replace an element of the sample is given by actualSize/sampleSize. Meaning that once the
     * sample has reached the desired size the probability of an element replacing an already existing sample
     * will be equal to 1.
     *
     * @param element
     */
    @Override
    public void update(T element) {
        double prob = ((double) actualSize)/sampleSize;
        if(rand.nextDouble() < prob){
            Integer position = rand.nextInt(actualSize);
            sample[position] = element;
            latestPositions.remove(position);
            latestPositions.add(position);
        } else{
            sample[actualSize] = element;
            latestPositions.add(actualSize);
            actualSize++;
        }
    }

    public T[] getSample() {
        return sample;
    }

    public int getSampleSize() {
        return sampleSize;
    }


    public LinkedList<Integer> getLatestPositions() {
        return latestPositions;
    }

    public int getActualSize() {
        return actualSize;
    }

    /**
     * Function to Merge two Biased Reservoir samples. This function takes advantage of the ordering of the elements
     * given by the {@code Synopsis.BuildSynopsis} retaining only the newest elements that entered the window.
     *
     * @param other Biased Reservoir sample to be merged with
     * @return merged Biased Reservoir Sample
     * @throws Exception
     */
    @Override
    public BiasedReservoirSampler<T> merge(Synopsis other) {
        if (other instanceof BiasedReservoirSampler
                && ((BiasedReservoirSampler) other).getSampleSize() == this.sampleSize) {
            BiasedReservoirSampler<T> o = (BiasedReservoirSampler<T>) other;
            BiasedReservoirSampler<T> mergeResult = new BiasedReservoirSampler(this.sampleSize);
            mergeResult.merged = this.merged + o.merged;

            while (mergeResult.actualSize < this.sampleSize && !(this.getLatestPositions().isEmpty() && o.getLatestPositions().isEmpty())) {
                if (!o.getLatestPositions().isEmpty()){
                    int pos = o.getLatestPositions().pollLast();
                    mergeResult.sample[(mergeResult.sampleSize-1)-mergeResult.actualSize] = o.sample[pos];
                    mergeResult.getLatestPositions().addFirst((mergeResult.sampleSize-1)-mergeResult.actualSize);
                    mergeResult.actualSize++;
                    if (mergeResult.actualSize >= this.sampleSize){
                        break;
                    }
                }
                for (int i = 0; i < this.merged; i++) {
                    if (this.getLatestPositions().isEmpty()){
                        break;
                    }
                    int pos = this.getLatestPositions().pollLast();
                    mergeResult.sample[(mergeResult.sampleSize-1)-mergeResult.actualSize] = this.sample[pos];
                    mergeResult.getLatestPositions().addFirst((mergeResult.sampleSize-1)-mergeResult.actualSize);
                    mergeResult.actualSize++;
                    if (mergeResult.actualSize >= this.sampleSize){
                        break;
                    }
                }
            }
            return mergeResult;
        } else {
            throw new IllegalArgumentException("Reservoir Samplers to merge have to be the same size");
        }
    }


    @Override
    public String toString(){
        String s = new String("Biased Reservoir sample size: " + this.actualSize+"\n");
        for (int i = 0; i < actualSize; i++) {
            s += this.sample[i].toString()+", ";
        }
        s = s.substring(0,s.length()-2);
        s += "\n";
        return s;
    }
}
