package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import Synopsis.Synopsis;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.BitSet;

/**
 * Implementation of classical Bloom Filter sketch to estimate the the elements that were contained in a
 * datastream.
 * Tis implementation uses a family of pairwise independent hash functions to update the hash map of the
 * sketch.
 *
 * @param <T> the type of elements maintained by this sketch
 *
 * @author Rudi Poepsel Lemaitre
 */
public class BloomFilter<T> implements Synopsis<T>, Serializable {
    private BitSet hashmap;
    private int nHashFunctions;
    private int numberBits;
    private int elementsProcessed;
    private static final double LN2 = 0.6931471805599453; // ln(2)
    private PairwiseIndependentHashFunctions hashFunctions;

    /**
     * Create a new Bloom Filter.
     *
     * @param maxNumElements Expected number of elements
     * @param numberBits     Desired size of the container in bits
     **/
    public BloomFilter(Integer maxNumElements, Integer numberBits, Integer seed) {
        this.numberBits = numberBits;
        this.nHashFunctions = (int) Math.round(LN2 * numberBits / maxNumElements);
        if (nHashFunctions <= 0) nHashFunctions = 1;
        this.hashmap = new BitSet(numberBits);
        this.hashFunctions = new PairwiseIndependentHashFunctions(nHashFunctions, seed);
        this.elementsProcessed = 0;
    }

    /**
     * Update the hash map of the Bloom Filter by setting to 1 all the positions calculated by the family
     * of hash functions with respect the new incoming element.
     *
     * @param element new incoming element
     */
    @Override
    public void update(T element) {
        int[] indices = hashFunctions.hash(element);
        for (int i = 0; i < nHashFunctions; i++) {
            hashmap.set(indices[i] % numberBits);
        }
        elementsProcessed++;
    }

    public boolean query(T element){
        int[] indices = hashFunctions.hash(element);
        for (int i = 0; i < nHashFunctions; i++) {
            if(!hashmap.get(indices[i] % numberBits)){
                return false;
            }
        }
        return true;
    }

    public BitSet getHashmap() {
        return hashmap;
    }

    public int getnHashFunctions() {
        return nHashFunctions;
    }

    public int getNumberBits() {
        return numberBits;
    }

    public int getElementsProcessed() {
        return elementsProcessed;
    }

    public PairwiseIndependentHashFunctions getHashFunctions() {
        return hashFunctions;
    }

    /**
     * Function to Merge two Bloom Filters of the same size with the same family of Hash functions
     * by a simple logical-and operation between the hash maps.
     *
     * @param other synopsis to be merged with
     * @return merged Bloom Filter
     * @throws Exception
     */
    @Override
    public BloomFilter merge(Synopsis other) {
        if (other instanceof BloomFilter) {
            BloomFilter otherBF = (BloomFilter) other;
            if (otherBF.getnHashFunctions() == nHashFunctions && otherBF.getNumberBits() == numberBits && hashFunctions.equals(otherBF.hashFunctions))
            {
                hashmap.and(otherBF.getHashmap());
                elementsProcessed += otherBF.getElementsProcessed();
            }
        } else {
            throw new IllegalArgumentException("Sketches to merge have to be the same size and hash Functions");
        }
        return this;
    }

    @Override
    public String toString() {
        String sketch = new String();
        sketch += "Functions\n";
        for (int i = 0; i < nHashFunctions; i++) {
            sketch += i + ":  A: " + hashFunctions.getA()[i] + "  ";
            sketch += "B: " + hashFunctions.getB()[i] + "\n";
        }
        sketch += hashmap.toString() + "\n";
        sketch += "Elements processed: " + elementsProcessed + "\n";
        sketch += "\n";
        return sketch;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(nHashFunctions);
        out.writeInt(numberBits);
        out.writeInt(elementsProcessed);
        out.writeObject(hashFunctions);
        out.writeObject(hashmap);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        nHashFunctions = in.readInt();
        numberBits = in.readInt();
        elementsProcessed = in.readInt();
        hashFunctions = (PairwiseIndependentHashFunctions) in.readObject();
        hashmap = (BitSet) in.readObject();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
