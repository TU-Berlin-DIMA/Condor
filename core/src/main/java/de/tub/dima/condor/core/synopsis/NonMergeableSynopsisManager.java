package de.tub.dima.condor.core.synopsis;

import java.util.ArrayList;

public abstract class NonMergeableSynopsisManager<Input, S extends Synopsis<Input>> extends StratifiedSynopsis implements Synopsis<Input>{
    protected ArrayList<S> unifiedSynopses;
    protected int elementsProcessed = 0;

    public NonMergeableSynopsisManager(){
        unifiedSynopses = new ArrayList<>();
    }

    public abstract int getSynopsisIndex(int streamIndex);

    public void addSynopsis(S synopsis){
        unifiedSynopses.add(synopsis);
    }

    public ArrayList<S> getUnifiedSynopses() {
        return unifiedSynopses;
    }

    public int getElementsProcessed() {
        return elementsProcessed;
    }

    public void unify(NonMergeableSynopsisManager other){
        elementsProcessed += other.getElementsProcessed();
        unifiedSynopses.addAll(other.getUnifiedSynopses());
    }

    public void cleanManager(){
        unifiedSynopses = new ArrayList<>();
        elementsProcessed = 0;
    }
}
