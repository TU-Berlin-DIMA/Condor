package de.tub.dima.condor.benchmark.efficiency.oneOffImplementation.utils;

import de.tub.dima.condor.core.synopsis.NonMergeableSynopsisManager;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.Wavelets.WaveletSynopsis;

import java.util.ArrayList;

public class CountMinSketchesManager<Input> extends NonMergeableSynopsisManager<Input, CountMinSketch<Input>> {

    int parallelism;

    public CountMinSketchesManager(int parallelism, ArrayList<CountMinSketch<Input>> unifiedSynopses) {
        this.parallelism = parallelism;
        this.unifiedSynopses = unifiedSynopses;
    }

    public CountMinSketchesManager(){
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
    public void addSynopsis(CountMinSketch<Input> synopsis) {
        parallelism++;
        elementsProcessed += synopsis.getElementsProcessed();
        super.addSynopsis(synopsis);
    }

    private int getGlobalIndex(int localIndex, int partition){
        return partition + (localIndex * parallelism);
    }
}
