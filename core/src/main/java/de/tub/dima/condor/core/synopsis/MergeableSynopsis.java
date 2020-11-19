package de.tub.dima.condor.core.synopsis;

import org.apache.flink.annotation.Internal;

/**
 * Generic interface for a MergeableSynopsis.
 *
 * @param <T> the Input Type
 * @author Joscha von Hein
 * @author Rudi Poepsel Lemaitre
 */
@Internal
public interface MergeableSynopsis<T> extends Synopsis<T>, java.io.Serializable, Cloneable{

	/**
	 * Function to Merge two Synopses.
	 *
	 * @param other synopsis to be merged with
	 * @return merged synopsis
	 * @throws Exception
	 */
	MergeableSynopsis<T> merge(MergeableSynopsis<T> other);

}
