package Sketches;

import org.apache.flink.annotation.Internal;

/**
 * Generic interface for a Sketch.
 *
 * @param <T> the Input Type
 * @param <R> the result type of the query
 */
@Internal
public interface Sketch<T, R> extends java.io.Serializable{

	/**
	 * Update the sketch with a value T
	 * @param t
	 */
	void update(T t);

	/**
	 * Query the sketch and get a Return Value
	 * @param t
	 * @return Value
	 */
	R query(T t);

	Sketch merge(Sketch other) throws Exception;



}
