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
public interface Synopsis<T> extends java.io.Serializable, Cloneable{
	/**
	 * Update the MergeableSynopsis structure with a new incoming element.
	 *
	 * @param element new incoming element
	 */
	void update(T element);

}
