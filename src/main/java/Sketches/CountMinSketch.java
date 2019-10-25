package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import Synopsis.Synopsis;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashSet;

/**
 * Implementation of classical Count-Min sketch to estimate the frequencies of the elements in a datastream.
 * This implementation uses a family of pairwise independent hash functions to update the counters of the
 * sketch.
 *
 * @param <T> the type of elements maintained by this sketch
 *
 * @author Rudi Poepsel Lemaitre
 */
public class CountMinSketch<T> implements Synopsis<T>, Serializable {

    private int width;
    private int height;
    private int[][] array;
    private PairwiseIndependentHashFunctions hashFunctions;
    private int elementsProcessed;
    private HashSet<T> Elements;


    /**
     * Construct a Count-Min sketch
     *
     * @param width value range of the hash functions
     * @param height number of hash functions
     * @param seed for the randomness of the hash functions
     */
    public CountMinSketch(Integer width, Integer height, Long seed) {
        this.width = width;
        this.height = height;
        array = new int[height][width];
        this.hashFunctions = new PairwiseIndependentHashFunctions(height, seed);
        this.elementsProcessed = 0;
        this.Elements = new HashSet<>();
    }



    /**
     * Update each row of the sketch with a new element by increasing the count of the hash position by 1.
     *
     * @param element new incoming element
     */
    @Override
    public void update(T element) {
        int[] indices = hashFunctions.hash(element);
        for (int i = 0; i < height; i++) {
            array[i][indices[i] % width]++;
        }
        Elements.add(element);
        elementsProcessed++;
    }

//    /**
//     * Update the sketch with incoming positive weighted tuple.
//     *
//     * @param tuple
//     * @param weight must be positive
//     */
//    public void weightedUpdate(Object tuple, int weight) throws Exception {
//        if (weight < 0) {
//            throw new Exception("Count Min sketch only accepts positive weights!");
//        }
//        int[] indices = hashFunctions.hash(tuple);
//        for (int i = 0; i < height; i++) {
//            array[i][indices[i] % width] += weight;
//        }
//        elementsProcessed++;
//    }

    /**
     * Query the sketch and get an estimate of the count of this value.
     *
     * @param element to query the frequency
     * @return The approximate count of tuple so far
     */
    public Integer query(T element) {
        int[] indices = hashFunctions.hash(element);
        int min = -1;
        for (int i = 0; i < height; i++) {
            if (min == -1)
                min = array[i][indices[i] % width];
            else if (array[i][indices[i] % width] < min) {
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

    public int getElementsProcessed() {
        return elementsProcessed;
    }

    public HashSet<T> getElements() {
        return Elements;
    }

    /**
     * Function to Merge two Count-Min sketches by adding their counters.
     *
     * @param other Count-Min sketch to be merged with
     * @return merged Count-Min sketch
     * @throws Exception
     */
    @Override
    public CountMinSketch merge(Synopsis other) throws Exception {
        if (other instanceof CountMinSketch) {
            CountMinSketch otherCM = (CountMinSketch) other;
            if (otherCM.getWidth() == width && otherCM.getHeight() == height && hashFunctions.equals(otherCM.hashFunctions))
            {
                int[][] a2 = otherCM.getArray();
                for (int i = 0; i < height; i++) {
                    for (int j = 0; j < width; j++) {
                        array[i][j] += a2[i][j];
                    }
                }
                elementsProcessed += otherCM.getElementsProcessed();
                Elements.addAll(otherCM.getElements());
                return this;
            }
        }
        throw new Exception("Sketches to merge have to be the same size and hash Functions");
    }

    @Override
    public String toString() {
        String sketch = new String();
        sketch += "Functions\n";
        for (int i = 0; i < height; i++) {
            sketch += i + ":  A: " + hashFunctions.getA()[i] + "  ";
            sketch += "B: " + hashFunctions.getB()[i] + "\n";
        }
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {

                sketch += array[i][j] + " | ";
            }
            sketch += "\n";
        }
        sketch += "Elements processed: " + elementsProcessed + "\n";
        for (Object elem :
                Elements) {
            sketch += elem.toString() + ", ";
        }
        sketch += "\n";
        return sketch;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(width);
        out.writeInt(height);
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                out.writeInt(array[i][j]);
            }
        }
        out.writeObject(hashFunctions);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        width = in.readInt();
        height = in.readInt();
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                array[i][j] = in.readInt();
            }
        }
        hashFunctions = (PairwiseIndependentHashFunctions) in.readObject();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
