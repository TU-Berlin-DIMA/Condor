package Sketches.HashFunctions;

import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.io.ObjectStreamException;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Random;

/**
 * Implementation of a family of pairwise independent hash functions
 *
 * @author Rudi Poepsel Lemaitre
 */
public class PairwiseIndependentHashFunctions implements Serializable {


	private final BigInteger[] a;
	private final BigInteger[] b;
	private int numFunctions;
	private Random rand;
	private final BigInteger p = new BigInteger("1610612741"); // prime

	/**
	 * Create a new family of pairwise independent hash functions.
	 *
	 * @param numFunctions number of functions
	 * @param seed for the randomness of the hash functions
	 */
	public PairwiseIndependentHashFunctions(int numFunctions, long seed) {
		this.rand = new XORShiftRandom(seed);
		this.numFunctions = numFunctions;
		a = new BigInteger[numFunctions];
		b = new BigInteger[numFunctions];

		for (int i = 0; i < numFunctions; i++){
			a[i] = new BigInteger(32, rand);
			b[i] = new BigInteger(32, rand);
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
		a = new BigInteger[numFunctions];
		b = new BigInteger[numFunctions];

		for (int i = 0; i < numFunctions; i++){
			a[i] = new BigInteger(32, rand);
			b[i] = new BigInteger(32, rand);
		}
	}

	/**
	 * Calculate the hash values of the incoming object for all the functions of the family.
	 *
	 * @param o object to calculate the hash values - equals method has to be implemented correctly (!)
	 * @return an array containing the hash values given this family of functions
	 */
	public int[] hash(Object o) {
		int[] result = new int[numFunctions];
		for (int i = 0; i < numFunctions; i++){
			BigInteger hashCode = new BigInteger(Integer.toString(o.hashCode()));
			result[i]  = (((a[i].multiply(hashCode)).add(b[i])).mod(p)).intValue();
		}
		return result;
	}

	public BigInteger[] getA() {
		return a;
	}

	public BigInteger[] getB() {
		return b;
	}

	public boolean equals(PairwiseIndependentHashFunctions other){
		if (other.numFunctions == numFunctions){
			for (int i = 0; i < numFunctions; i++){
				if (!a[i].equals(other.getA()[i]) || !b[i].equals(other.getB()[i])){
					return false;
				}
			}
		}else return false;
		return true;
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(numFunctions);
		for (int i = 0; i < numFunctions; i++){
			out.writeObject(a[i]);
			out.writeObject(b[i]);
		}
		out.writeObject(rand);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
		numFunctions = in.readInt();
		for (int i = 0; i < numFunctions; i++){
			a[i] = (BigInteger) in.readObject();
			b[i] = (BigInteger) in.readObject();
		}
		rand = (Random) in.readObject();
	}

	private void readObjectNoData() throws ObjectStreamException{
		System.out.println("readObjectNoData() called - should give an exception");
	}
}
