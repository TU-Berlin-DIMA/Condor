package de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils;

import de.tub.dima.condor.core.synopsis.NonMergeableSynopsisManager;
import de.tub.dima.condor.core.synopsis.Wavelets.WaveletSynopsis;

import java.util.ArrayList;


public class SliceCointMinSketchManager<Input> extends NonMergeableSynopsisManager<Input, CountMinSketchOrderBased<Input>> {

    int slicesPerWindow;
    ArrayList<Integer> sliceStartIndices;


    public SliceCointMinSketchManager(ArrayList<CountMinSketchOrderBased<Input>> unifiedSynopses) {
        this.unifiedSynopses = unifiedSynopses;
        this.slicesPerWindow = unifiedSynopses.size();
        sliceStartIndices = new ArrayList<>(slicesPerWindow);

        elementsProcessed = 0;
        for (int i = 0; i < slicesPerWindow; i++) {
            sliceStartIndices.add(i, elementsProcessed);
        }
    }

    public SliceCointMinSketchManager() {
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
    public void addSynopsis(CountMinSketchOrderBased<Input> synopsis) {
        if (sliceStartIndices == null){
            sliceStartIndices = new ArrayList<>();
        }
        slicesPerWindow++;
        elementsProcessed += synopsis.getElementsProcessed();
        if (unifiedSynopses.isEmpty()) {
            sliceStartIndices.add(0);
        } else {
            sliceStartIndices.add(sliceStartIndices.get(sliceStartIndices.size() - 1) + unifiedSynopses.get(unifiedSynopses.size() - 1).getElementsProcessed());
        }
        super.addSynopsis(synopsis);
    }

    @Override
    public void unify(NonMergeableSynopsisManager other) {
//        if (other instanceof SliceWaveletsManager){
            SliceCointMinSketchManager o = (SliceCointMinSketchManager) other;
            for (int i = 0; i < o.getUnifiedSynopses().size(); i++) {
                this.addSynopsis((CountMinSketchOrderBased<Input>) o.getUnifiedSynopses().get(i));
            }
//        }
//        Environment.out.println(other.getClass());
//        throw new IllegalArgumentException("It is only possible to unify two objects of type NonMergeableSynopsisManager with each other.");
    }
}
