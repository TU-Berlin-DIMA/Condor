package Sketches;

import org.apache.flink.annotation.Internal;

/**
 * Generic interface for a Sketch.
 *
 * @param <T> the Input Type
 */
public interface Sketch<T> extends java.io.Serializable{

	/**
	 * Update the sketch with a value T
	 * @param t
	 */
	void update(T t);

	Sketch merge(Sketch other) throws Exception;

}
