package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountMinSketchAggregator2<T> implements AggregateFunction<T , Integer, Integer> {

	private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

	private int height;
	private int width;
	private int seed;
	private PairwiseIndependentHashFunctions hashFunctions;

	public CountMinSketchAggregator2(int height, int weight, int seed){
		this.height = height;
		this.width = weight;
		this.seed = seed;
	}
	/**
	 * Creates a new accumulator, starting a new aggregate.
	 *
	 * <p>The new accumulator is typically meaningless unless a value is added
	 * via {@link #add(Object, Object)}.
	 *
	 * <p>The accumulator is the state of a running aggregation. When a program has multiple
	 * aggregates in progress (such as per key and window), the state (per key and window)
	 * is the size of the accumulator.
	 *
	 * @return A new accumulator, corresponding to an empty aggregate.
	 */
	@Override
	public Integer createAccumulator() {
		LOG.info("Accumulator starts");
		hashFunctions = new PairwiseIndependentHashFunctions(height, seed);

		return new Integer(0);
	}

	/**
	 * Adds the given input value to the given accumulator, returning the
	 * new accumulator value.
	 *
	 * <p>For efficiency, the input accumulator may be modified and returned.
	 *
	 * @param value       The value to add
	 * @param accumulator The accumulator to add the value to
	 */
	@Override
	public Integer add(T value, Integer accumulator) {

		accumulator += 1;
		return accumulator;
	}

	/**
	 * Gets the result of the aggregation from the accumulator.
	 *
	 * @param accumulator The accumulator of the aggregation
	 * @return The final aggregation result.
	 */
	@Override
	public Integer getResult(Integer accumulator) {
		return accumulator;
	}

	/**
	 * Merges two accumulators, returning an accumulator with the merged state.
	 *
	 * <p>This function may reuse any of the given accumulators as the target for the merge
	 * and return that. The assumption is that the given accumulators will not be used any
	 * more after having been passed to this function.
	 *
	 * @param a An accumulator to merge
	 * @param b Another accumulator to merge
	 * @return The accumulator with the merged state
	 */
	@Override
	public Integer merge(Integer a, Integer b) {

		return new Integer(a+b);
	}
}
