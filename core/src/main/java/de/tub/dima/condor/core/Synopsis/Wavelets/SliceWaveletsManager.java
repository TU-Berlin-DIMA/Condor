package de.tub.dima.condor.core.Synopsis.Wavelets;

import de.tub.dima.condor.core.Synopsis.NonMergeableSynopsisManager;
import de.tub.dima.condor.core.Synopsis.StratifiedSynopsis;

import java.util.ArrayList;


public class SliceWaveletsManager<Input> extends NonMergeableSynopsisManager<Input,WaveletSynopsis<Input>> {

    int slicesPerWindow;
    ArrayList<Integer> sliceStartIndices;


    public SliceWaveletsManager(ArrayList<WaveletSynopsis<Input>> unifiedSynopses) {
        this.unifiedSynopses = unifiedSynopses;
        this.slicesPerWindow = unifiedSynopses.size();
        sliceStartIndices = new ArrayList<>(slicesPerWindow);

        elementsProcessed = 0;
        for (int i = 0; i < slicesPerWindow; i++) {
            sliceStartIndices.add(i, elementsProcessed);

            elementsProcessed += unifiedSynopses.get(i).getStreamElementCounter();
        }
    }

    public SliceWaveletsManager() {
        super();
        sliceStartIndices = new ArrayList<>();
    }

    @Override
    public void update(Object element) {
        if (!unifiedSynopses.isEmpty()) {
            unifiedSynopses.get(unifiedSynopses.size() - 1).update((Input) element);
        }
    }

    @Override
    public int getSynopsisIndex(int streamIndex) {
        int index = -1;
        for (int i = 0; i < sliceStartIndices.size(); i++) {
            if (sliceStartIndices.get(i) > streamIndex) {
                return index;
            }
            index++;
        }
        return index;
    }

    @Override
    public void addSynopsis(WaveletSynopsis<Input> synopsis) {
        if (sliceStartIndices == null){
            sliceStartIndices = new ArrayList<>();
        }
        slicesPerWindow++;
        elementsProcessed += synopsis.getStreamElementCounter();
        if (unifiedSynopses.isEmpty()) {
            sliceStartIndices.add(0);
        } else {
            sliceStartIndices.add(sliceStartIndices.get(sliceStartIndices.size() - 1) + unifiedSynopses.get(unifiedSynopses.size() - 1).getStreamElementCounter());
        }
        super.addSynopsis(synopsis);
    }

    @Override
    public void unify(NonMergeableSynopsisManager other) {
//        if (other instanceof SliceWaveletsManager){
            SliceWaveletsManager o = (SliceWaveletsManager) other;
            for (int i = 0; i < o.getUnifiedSynopses().size(); i++) {
                this.addSynopsis((WaveletSynopsis<Input>) o.getUnifiedSynopses().get(i));
            }
//        }
//        Environment.out.println(other.getClass());
//        throw new IllegalArgumentException("It is only possible to unify two objects of type NonMergeableSynopsisManager with each other.");
    }

    public double pointQuery(int index) {
        int managerIndex = getSynopsisIndex(index);
        int previousSliceElements = sliceStartIndices.get(managerIndex);
        return unifiedSynopses.get(managerIndex).pointQuery(index - previousSliceElements);
    }


    public double rangeSumQuery(int leftIndex, int rightIndex) {
        int leftManagerIndex = getSynopsisIndex(leftIndex);
        int rightManagerIndex = getSynopsisIndex(rightIndex);

        double rangeSum = 0;

        for (int i = leftManagerIndex; i <= rightManagerIndex; i++) {
            int previousSliceElements = sliceStartIndices.get(i);
            int localLeftIndex = i == leftManagerIndex ? leftIndex - previousSliceElements : 0;
            int localRightIndex = i == rightManagerIndex ? rightIndex - previousSliceElements : sliceStartIndices.get(i + 1) - previousSliceElements - 1;
            rangeSum += unifiedSynopses.get(i).rangeSumQuery(localLeftIndex, localRightIndex);
        }
        return rangeSum;
    }
}
