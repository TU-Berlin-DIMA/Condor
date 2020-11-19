package de.tub.dima.condor.core.synopsis.Wavelets;

import de.tub.dima.condor.core.synopsis.NonMergeableSynopsisManager;

import java.util.ArrayList;

public class DistributedSliceWaveletsManager<Input> extends NonMergeableSynopsisManager<Input,SliceWaveletsManager<Input>> {

    int parallelism = 0;

    public DistributedSliceWaveletsManager(ArrayList<SliceWaveletsManager<Input>> unifiedSynopses) {
        this.unifiedSynopses = unifiedSynopses;
        this.parallelism = unifiedSynopses.size();
        for (int i = 0; i < unifiedSynopses.size(); i++) {
            elementsProcessed += unifiedSynopses.get(i).getElementsProcessed();
        }
    }

    public DistributedSliceWaveletsManager() {
        super();
    }

    @Override
    public int getSynopsisIndex(int streamIndex) {
        return streamIndex % parallelism;
    }

    @Override
    public void update(Object element) {
        elementsProcessed++;
        unifiedSynopses.get(getSynopsisIndex(elementsProcessed)).update(element);
    }

    @Override
    public void addSynopsis(SliceWaveletsManager<Input> synopsis) {
        parallelism++;
        elementsProcessed += synopsis.getElementsProcessed();
        super.addSynopsis(synopsis);
    }

    public int getLocalIndex(int index) {
        return index / parallelism;
    }

    public double pointQuery(int index) {
        return unifiedSynopses.get(getSynopsisIndex(index)).pointQuery(getLocalIndex(index));
    }

    public double rangeSumQuery(int leftIndex, int rightIndex) {
        double rangeSum = 0;

        int leftLocalIndex = getLocalIndex(leftIndex);
        int rightLocalIndex = getLocalIndex(rightIndex);

        for (int i = 0; i < parallelism; i++) {

            int partitionLeftIndex = leftLocalIndex;
            if (getGlobalIndex(leftLocalIndex, i) < leftIndex) {
                partitionLeftIndex += 1;
            }

            int partitionRightIndex = rightLocalIndex;
            if (getGlobalIndex(rightLocalIndex, i) > rightIndex) {
                partitionRightIndex -= 1;
            }

            rangeSum += unifiedSynopses.get(i).rangeSumQuery(partitionLeftIndex, partitionRightIndex);
        }

        return rangeSum;
    }

    private int getGlobalIndex(int localIndex, int partition) {
        return partition + (localIndex * parallelism);
    }
}
