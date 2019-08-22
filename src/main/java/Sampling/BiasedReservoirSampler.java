package Sampling;

import Sketches.Sketch;
import org.apache.flink.util.XORShiftRandom;

import java.io.Serializable;
import java.util.LinkedList;

public class BiasedReservoirSampler<T> implements Sketch<T>, Serializable {

    private T sample[];
    private int sampleSize;
    private XORShiftRandom rand;
    private int actualSize;
    private int merged = 1;
    private LinkedList<Integer> latestPositions;

    public BiasedReservoirSampler(Integer sampleSize) {
        this.sample = (T[]) new Object[sampleSize];
        this.sampleSize = sampleSize;
        this.rand = new XORShiftRandom();
        this.actualSize = 0;
        this.latestPositions = new LinkedList<>();
    }

    /**
     * Update the sketch with a value T
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
     * Function to Merge two Sketches
     *
     * @param other
     * @return
     * @throws Exception
     */
    @Override
    public BiasedReservoirSampler<T> merge(Sketch other) throws Exception {
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
            throw new Exception("Reservoir Samplers to merge have to be the same size");
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
