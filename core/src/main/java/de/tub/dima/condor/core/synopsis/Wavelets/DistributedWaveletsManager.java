package de.tub.dima.condor.core.synopsis.Wavelets;

import de.tub.dima.condor.core.synopsis.NonMergeableSynopsisManager;

import java.util.ArrayList;

public class DistributedWaveletsManager<Input> extends NonMergeableSynopsisManager<Input,WaveletSynopsis<Input>> {

    int parallelism;

    public DistributedWaveletsManager(int parallelism, ArrayList<WaveletSynopsis<Input>> unifiedSynopses) {
        this.parallelism = parallelism;
        this.unifiedSynopses = unifiedSynopses;
    }

    public DistributedWaveletsManager(){
        super();
    }

    @Override
    public int getSynopsisIndex(int streamIndex) {
        return streamIndex % parallelism;
    }

    @Override
    public void update(Object element) {
        elementsProcessed++;
        unifiedSynopses.get(getSynopsisIndex(elementsProcessed)).update((Input) element);
    }

    @Override
    public void addSynopsis(WaveletSynopsis<Input> synopsis) {
        parallelism++;
        elementsProcessed += synopsis.getStreamElementCounter();
        super.addSynopsis(synopsis);
    }

    public int getLocalIndex(int index){
        return index / parallelism;
    }

    public double pointQuery(int index){
        WaveletSynopsis<Input> wavelet = unifiedSynopses.get(getSynopsisIndex(index));
        int localIndex = getLocalIndex(index);
        if (localIndex > wavelet.getStreamElementCounter()){
            System.out.println("ups");
            localIndex = getLocalIndex(index);
            return -1;
        }
        return wavelet.pointQuery(localIndex);
//        return unifiedSynopses.get(getSynopsisIndex(index)).pointQuery(getLocalIndex(index));
    }

    public double rangeSumQuery(int leftIndex, int rightIndex){
        double rangeSum = 0;

        int leftLocalIndex = getLocalIndex(leftIndex);
        int rightLocalIndex = getLocalIndex(rightIndex);

        for (int i = 0; i < parallelism; i++) {

            int partitionLeftIndex = leftLocalIndex;
            if (getGlobalIndex(leftLocalIndex, i) < leftIndex){
                partitionLeftIndex += 1;
            }

            int partitionRightIndex = rightLocalIndex;
            if (getGlobalIndex(rightLocalIndex, i) > rightIndex){
                partitionRightIndex -=1;
            }

            rangeSum += unifiedSynopses.get(i).rangeSumQuery(partitionLeftIndex, partitionRightIndex);
        }

        return rangeSum;
    }

    private int getGlobalIndex(int localIndex, int partition){
        return partition + (localIndex * parallelism);
    }
}
