package Sampling;

import Sketches.Sketch;
import org.apache.flink.util.XORShiftRandom;

import java.io.Serializable;
import java.util.ArrayList;


public class ReservoirSampler<T> implements Sketch<T>, Serializable {
    private T sample[];
    private int sampleSize;
    private XORShiftRandom rand;
    private int processedElements;

    public ReservoirSampler(Integer sampleSize) {
        this.sample = (T[]) new Object[sampleSize];
        this.sampleSize = sampleSize;
        this.rand = new XORShiftRandom();
        this.processedElements = 0;
    }

    /**
     * Update the sketch with a value T
     *
     * @param element
     */
    @Override
    public void update(T element) {
        if (processedElements < sampleSize) {
            sample[processedElements] = element;
            processedElements++;
        } else {
            processedElements++;
            if (rand.nextDouble() < ((double) sampleSize)/processedElements){
                sample[rand.nextInt(sampleSize)] = element;
            }
        }
    }

    public T[] getSample() {
        return sample;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public int getProcessedElements() {
        return processedElements;
    }

    /**
     * Function to Merge two Sketches
     *
     * @param other
     * @return
     * @throws Exception
     */
    @Override
    public ReservoirSampler<T> merge(Sketch other) throws Exception {
        if (other instanceof ReservoirSampler
                && ((ReservoirSampler) other).getSampleSize() == this.sampleSize) {
            ReservoirSampler o = (ReservoirSampler) other;
            T[] otherSample = (T[]) o.getSample();
            T[] mergeResult = (T[]) new Object[sampleSize];
            int actual = 0;
            ArrayList<Integer> indecesList1;
            if (this.processedElements > this.sampleSize){
                indecesList1 = generateIndecesArray(this.sampleSize);
            } else{
                indecesList1 = generateIndecesArray(this.processedElements);
            }

            ArrayList<Integer> indecesList2;
            if (o.getProcessedElements() > o.getSampleSize()){
                indecesList2 = generateIndecesArray(o.getSampleSize());
            } else{
                indecesList2 = generateIndecesArray(o.getProcessedElements());
            }

            while (actual != sampleSize && !(indecesList1.isEmpty() && indecesList2.isEmpty())) {
                if (!indecesList1.isEmpty()){
                    mergeResult[actual] = this.sample[getIndexWithoutReplacement(indecesList1)];
                    actual++;
                } if (actual != sampleSize && !indecesList2.isEmpty()){
                    mergeResult[actual] = this.sample[getIndexWithoutReplacement(indecesList2)];
                    actual++;
                }
            }
            this.sample = mergeResult;
        } else {
            throw new Exception("Reservoir Samplers to merge have to be the same size");
        }
        return this;
    }

    private ArrayList<Integer> generateIndecesArray(int size){
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(i);
        }
        return list;
    }

    private int getIndexWithoutReplacement(ArrayList<Integer> list){
        return list.remove(rand.nextInt(list.size()));
    }
}
