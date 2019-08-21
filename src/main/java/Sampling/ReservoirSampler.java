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
            ReservoirSampler<T> o = (ReservoirSampler<T>) other;
            T[] otherSample = o.getSample();
            T[] mergeResult = (T[]) new Object[sampleSize];
            int actual = 0;
            ArrayList<Integer> indicesList1;
            if (this.processedElements > this.sampleSize){
                indicesList1 = generateIndicesArray(this.sampleSize);
            } else{
                indicesList1 = generateIndicesArray(this.processedElements);
            }

            ArrayList<Integer> indicesList2;
            if (o.getProcessedElements() > o.getSampleSize()){
                indicesList2 = generateIndicesArray(o.getSampleSize());
            } else{
                indicesList2 = generateIndicesArray(o.getProcessedElements());
            }

            double prob = ((double)this.processedElements)/(this.processedElements+o.getProcessedElements());
            while (actual != sampleSize && !(indicesList1.isEmpty() && indicesList2.isEmpty())) {
                if (rand.nextDouble() < prob){
                    if (!indicesList1.isEmpty()){
                        mergeResult[actual] = this.sample[getIndexWithoutReplacement(indicesList1)];
                        actual++;
                    } else {
                        mergeResult[actual] = otherSample[getIndexWithoutReplacement(indicesList2)];
                        actual++;
                    }
                } else{
                    if (!indicesList2.isEmpty()){
                        mergeResult[actual] = otherSample[getIndexWithoutReplacement(indicesList2)];
                        actual++;
                    } else {
                        mergeResult[actual] = this.sample[getIndexWithoutReplacement(indicesList1)];
                        actual++;
                    }
                }
            }
            this.sample = mergeResult;
            this.processedElements += o.getProcessedElements();
        } else {
            throw new Exception("Reservoir Samplers to merge have to be the same size");
        }
        return this;
    }

    private ArrayList<Integer> generateIndicesArray(int size){
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(i);
        }
        return list;
    }

    private int getIndexWithoutReplacement(ArrayList<Integer> list){
        return list.remove(rand.nextInt(list.size()));
    }

    public ReservoirSampler<T> nonWeightedMerge(ReservoirSampler<T> other) throws Exception {
        if (other.getSampleSize() == this.sampleSize) {
            T[] otherSample = other.getSample();
            T[] mergeResult = (T[]) new Object[sampleSize];
            int actual = 0;
            ArrayList<Integer> indicesList1;
            if (this.processedElements > this.sampleSize){
                indicesList1 = generateIndicesArray(this.sampleSize);
            } else{
                indicesList1 = generateIndicesArray(this.processedElements);
            }

            ArrayList<Integer> indicesList2;
            if (other.getProcessedElements() > other.getSampleSize()){
                indicesList2 = generateIndicesArray(other.getSampleSize());
            } else{
                indicesList2 = generateIndicesArray(other.getProcessedElements());
            }

            while (actual != sampleSize && !(indicesList1.isEmpty() && indicesList2.isEmpty())) {
                if (!indicesList1.isEmpty()){
                    mergeResult[actual] = this.sample[getIndexWithoutReplacement(indicesList1)];
                    actual++;
                } if (actual != sampleSize && !indicesList2.isEmpty()){
                    mergeResult[actual] = otherSample[getIndexWithoutReplacement(indicesList2)];
                    actual++;
                }
            }
            this.sample = mergeResult;
            this.processedElements += other.getProcessedElements();
        } else {
            throw new Exception("Reservoir Samplers to merge have to be the same size");
        }
        return this;
    }

    @Override
    public String toString(){
        String s = new String("Reservoir sample size: " + this.sampleSize+"\n");
        for (int i = 0; i < processedElements; i++) {
            if (i == sampleSize){
                break;
            }
            s += this.sample[i].toString()+", ";
        }
        s = s.substring(0,s.length()-2);
        s += "\n";
        return s;
    }
}
