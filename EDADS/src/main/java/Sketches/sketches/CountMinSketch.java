package Sketches.sketches;

import org.apache.flink.api.java.HashFunctions.PairwiseIndependentHashFunctions;

public class CountMinSketch<T> implements Sketch<T, Integer> {

	private int width;
	private int height;
	private int[][] array;
	private PairwiseIndependentHashFunctions hashFunctions;

	public CountMinSketch(int width, int height, PairwiseIndependentHashFunctions hashFunctions){
		this.width = width;
		this.height = height;
		array = new int[height][width];
		this.hashFunctions = hashFunctions;
	}

	/**
	 * Update the sketch with a value T by increasing the count by 1.
	 *
	 * @param tuple
	 */
	@Override
	public void update(T tuple) {

		int[] indices = hashFunctions.hash(tuple);
		for (int i = 0; i < height; i++){
			array[i][indices[i] % width] ++;
		}
	}

	/**
	 * Update the sketch with incoming tuple by a width.
	 * @param tuple
	 * @param weight must be positive
	 */
	public void weightedUpdate(T tuple, int weight) throws Exception {
		if (weight < 0){
			throw new Exception("Count Min sketch only accepts positive weights!");
		}
		int[] indices = hashFunctions.hash(tuple);
		for (int i = 0; i < height; i++){
			array[i][indices[i] % width] += weight;
		}
	}

	/**
	 * Query the sketch and get a Return Value
	 *
	 * @param tuple
	 * @return The approximate count of tuple so far
	 */
	@Override
	public Integer query(T tuple) {
		int[] indices = hashFunctions.hash(tuple);
		int min = -1;
		for (int i = 0; i < height; i++){
			if (min == -1)
				min = array[i][indices[i] % width];
			else if (array[i][indices[i] % width] < min){
				min = array[i][indices[i] % width];
			}
		}
		return min;
	}

	public int getWidth() {
		return width;
	}

	public int getHeight() {
		return height;
	}

	public int[][] getArray() {
		return array;
	}

	public PairwiseIndependentHashFunctions getHashFunctions() {
		return hashFunctions;
	}

	protected CountMinSketch merge(CountMinSketch other) throws Exception {
		if (other.getWidth() == width && other.getHeight() == height && hashFunctions.equals(other.hashFunctions)){
			int[][] a2 = other.getArray();
			for (int i = 0; i < height; i++){
				for (int j = 0; j < width; j++){
					array[i][j] += a2[i][j];
				}
			}
		}else {
			throw new Exception("Sketches to merge have to be the same size and hash Functions");
		}
		return this;
	}
}
