package Sketches;


import Sketches.HashFunctions.PairwiseIndependentHashFunctions;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

public class CountMinSketch<T> implements Sketch<T, Integer>, Serializable {


	private int width;
	private int height;
	private int[][] array;
	private PairwiseIndependentHashFunctions hashFunctions;
	private int elementsProcessed;
	private HashSet<Object> Elements;

	public CountMinSketch(int width, int height, PairwiseIndependentHashFunctions hashFunctions){
		this.width = width;
		this.height = height;
		array = new int[height][width];
		this.hashFunctions = hashFunctions;
		this.elementsProcessed = 0;
		this.Elements = new HashSet<>();
	}

	/**
	 * Update the sketch with a value T by increasing the count by 1.
	 *
	 * @param tuple
	 */
	@Override
	public void update(Object tuple) {

		int[] indices = hashFunctions.hash(tuple);
		for (int i = 0; i < height; i++){

			int pos = indices[i] % width;
			//System.out.println(this + " "+ tuple+" "+pos+" "+indices[i]);
			array[i][indices[i] % width] ++;
		}
		Elements.add(tuple);
		elementsProcessed++;
	}

	/**
	 * Update the sketch with incoming tuple by a width.
	 * @param tuple
	 * @param weight must be positive
	 */
	public void weightedUpdate(Object tuple, int weight) throws Exception {
		if (weight < 0){
			throw new Exception("Count Min sketch only accepts positive weights!");
		}
		int[] indices = hashFunctions.hash(tuple);
		for (int i = 0; i < height; i++){
			array[i][indices[i] % width] += weight;
		}
		elementsProcessed++;
	}

	/**
	 * Query the sketch and get a Return Value
	 *
	 * @param tuple
	 * @return The approximate count of tuple so far
	 */
	@Override
	public Integer query(Object tuple) {
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

	public int getElementsProcessed() {
		return elementsProcessed;
	}

	public HashSet<Object> getElements() {
		return Elements;
	}

	public CountMinSketch merge(CountMinSketch other) throws Exception {
		if (other.getWidth() == width && other.getHeight() == height && hashFunctions.equals(other.hashFunctions)){
			int[][] a2 = other.getArray();
			for (int i = 0; i < height; i++){
				for (int j = 0; j < width; j++){
					array[i][j] += a2[i][j];
				}
			}
			elementsProcessed += other.getElementsProcessed();
			Elements.addAll(other.getElements());
		}else {
			throw new Exception("Sketches to merge have to be the same size and hash Functions");
		}
		return this;
	}

	@Override
	public String toString(){
		String sketch = new String();
//		sketch += "Functions\n";
//		for (int i = 0; i < height; i++) {
//			sketch += i + ":  A: " + hashFunctions.getA()[i] + "  ";
//			sketch += "B: " + hashFunctions.getB()[i] + "\n";
//		}
//		for (int i = 0; i < height; i++) {
//			for (int j = 0; j < width; j++) {
//
//				sketch += array[i][j] + " | ";
//			}
//			sketch += "\n";
//		}
		sketch += /*"Elements processed: " + */elementsProcessed/*+ "\n"*/;
//		for (Object elem:
//				Elements) {
//			sketch += elem.toString() + ", ";
//		}
//		sketch += "\n";
		return sketch;
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(width);
		out.writeInt(height);
		for (int i = 0; i < height; i++){
			for (int j = 0; j < width; j++){
				out.writeInt(array[i][j]);
			}
		}
		out.writeObject(hashFunctions);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
		width = in.readInt();
		height = in.readInt();
		for (int i = 0; i < height; i++){
			for (int j = 0; j < width; j++){
				array[i][j] = in.readInt();
			}
		}
		hashFunctions = (PairwiseIndependentHashFunctions) in.readObject();
	}

	private void readObjectNoData() throws ObjectStreamException {
		System.out.println("readObjectNoData() called - should give an exception");
	}
}
