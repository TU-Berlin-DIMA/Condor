package Sketches.HashFunctions;

import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.io.ObjectStreamException;

import java.io.Serializable;
import java.util.Random;

/**
 * Implementation of a family of pairwise independent hash functions
 *
 * @author Rudi Poepsel Lemaitre
 */
public class PairwiseIndependentHashFunctions implements Serializable {


	private final int[] a;
	private final int[] b;
	private int numFunctions;
	private Random rand;
	private final int p = 1610612741; // prime


	/**
	 * Create a new family of pairwise independent hash functions.
	 *
	 * @param numFunctions number of functions
	 * @param seed for the randomness of the hash functions
	 */
	public PairwiseIndependentHashFunctions(int numFunctions, long seed) {
		this.rand = new XORShiftRandom(seed);
		this.numFunctions = numFunctions;
		a = new int[numFunctions];
		b = new int[numFunctions];

		for (int i = 0; i < numFunctions; i++){
			a[i] = rand.nextInt(p);
			b[i] = rand.nextInt(p);
		}
	}

	/**
	 * Create a new family of pairwise independent hash functions.
	 *
	 * @param numFunctions number of functions
	 */
	public PairwiseIndependentHashFunctions(int numFunctions) {
		this.rand = new XORShiftRandom();
		this.numFunctions = numFunctions;
		a = new int[numFunctions];
		b = new int[numFunctions];

		for (int i = 0; i < numFunctions; i++){
			a[i] = rand.nextInt(p);
			b[i] = rand.nextInt(p);
		}

	}


	/**
	 * Calculate the hash values of the incoming object for all the functions of the family.
	 *
	 * @param o object to calculate the hash values
	 * @return an array containing the hash values given this family of functions
	 */
	public int[] hash(Object o) {
		int[] result = new int[numFunctions];

		for (int i = 0; i < numFunctions; i++){
			int temp = (a[i]* o.hashCode() + b[i])%p;
			if (temp < 0){
				temp *= -1;
			}
			result[i] = temp;
		}

		return result;
	}

	public int[] getA() {
		return a;
	}

	public int[] getB() {
		return b;
	}

	public boolean equals(PairwiseIndependentHashFunctions other){
		if (other.numFunctions == numFunctions){
			for (int i = 0; i < numFunctions; i++){
				if (a[i] != other.getA()[i] || b[i] != other.getB()[i]){
					return false;
				}
			}
		}else return false;
		return true;
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(numFunctions);
		for (int i = 0; i < numFunctions; i++){
			out.writeInt(a[i]);
			out.writeInt(b[i]);
		}
		out.writeObject(rand);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
		numFunctions = in.readInt();
		for (int i = 0; i < numFunctions; i++){
			a[i] = in.readInt();
			b[i] = in.readInt();
		}
		rand = (Random) in.readObject();
	}

	private void readObjectNoData() throws ObjectStreamException{
		System.out.println("readObjectNoData() called - should give an exception");
	}

}
