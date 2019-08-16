package Sketches;

import org.apache.flink.annotation.Internal;

/**
 * Generic interface for a Sketch.
 *
 * @param <T> the Input Type
 */
@Internal
public interface Sketch<T> extends java.io.Serializable{

	/**
	 * Update the sketch with a value T
	 * @param t
	 */
	void update(T t);

	/**
	 * Function to Merge two Sketches
	 * @param other
	 * @return
	 * @throws Exception
	 */
	Sketch merge(Sketch other) throws Exception;

}
