package de.tub.dima.condor.core.Synopsis;

import java.io.Serializable;
import java.util.TreeMap;

public interface InvertibleSynopsis<T> extends CommutativeSynopsis<T>, Serializable {
    InvertibleSynopsis<T> invert(InvertibleSynopsis<T> toRemove);

    void decrement(T toDecrement);

    @Override
    InvertibleSynopsis<T> merge(MergeableSynopsis<T> other);

}
