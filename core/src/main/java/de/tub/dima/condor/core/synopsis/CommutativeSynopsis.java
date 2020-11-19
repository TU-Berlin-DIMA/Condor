package de.tub.dima.condor.core.synopsis;

import java.io.Serializable;

public interface CommutativeSynopsis<T> extends MergeableSynopsis<T>, Serializable {
    @Override
    CommutativeSynopsis<T> merge(MergeableSynopsis<T> other);
}
