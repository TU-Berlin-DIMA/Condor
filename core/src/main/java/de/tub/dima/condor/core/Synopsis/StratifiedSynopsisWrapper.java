package de.tub.dima.condor.core.Synopsis;

import java.io.Serializable;

public class StratifiedSynopsisWrapper<K extends Serializable, S extends Synopsis> implements Serializable{
    final K key;
    final S synopsis;


    public StratifiedSynopsisWrapper(K key, S synopsis) {
        this.synopsis = synopsis;
        this.key = key;
    }

    public S getSynopsis() {
        return synopsis;
    }

    public K getKey() {
        return key;
    }
}
