package Synopsis;

import org.apache.flink.annotation.Internal;

/**
 * Generic interface for a Synopsis.
 *
 * @param <T> the Input Type
 * @author Joscha von Hein
 * @author Rudi Poepsel Lemaitre
 */
@Internal
public interface Synopsis<T> extends java.io.Serializable{

	/**
	 * Update the Synopsis structure with a new incoming element.
	 *
	 * @param element new incoming element
	 */
	void update(T element);

	/**
	 * Function to Merge two Synopses.
	 *
	 * @param other synopsis to be merged with
	 * @return merged synopsis
	 * @throws Exception
	 */
	Synopsis<T> merge(Synopsis<T> other);

}
