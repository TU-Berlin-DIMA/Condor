package Sketches;

import Sketches.HashFunctions.EH3_HashFunction;
import Sketches.HashFunctions.EfficientH3Functions;
import Synopsis.Synopsis;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;

/**
 * implementation of the Fast AMS Sketch using H3 and EH3_HashFunction Hash Functions.
 *
 * This sketch is used to estimate the F2 norm and can be updated in a streaming fashion.
 * The sketch supports updates, deletions and can be merged, given the seeds of the hash functions are the same.
 * The sketch gives estimates which are errorBound with a given probability (see methods @errorBound() and @errorProbability())
 *
 * The sketch can handle insertions and removals. It doesn't work anymore if more removals than insertions for a specific attribute value occur!
 *
 * @author joschavonhein
 */
public class FastAMS implements Synopsis, Serializable {

    private int[][] array;
    private int width;
    private int height;
    private EfficientH3Functions hashFunctions;
    private EH3_HashFunction[] eh3_array;
    private final byte n = 32;
    private BitSet[] seeds; // always have size n+1

    @Override
    public String toString() {
        String arrayString = "";
        for (int i = 0; i < height; i++) {
            arrayString += Arrays.toString(array[i]) + "\n";
        }
        return "FastAMS{" +
                ", width=" + width +
                ", height=" + height +
                ", n=" + n +
                ", seeds=" + Arrays.toString(seeds) +
                "\n Array: " + arrayString +
                '}';
    }

    /**
     * Constructs a FastAMS Sketch Object.
     *
     * @param width     amount of buckets in each row - it is recommended to use powers of 2
     * @param height    amount of hash functions / rows in the sketch array
     * @param seed      seed for the RandomNumber Generator
     */
    public FastAMS(Integer width, Integer height, Long seed) {
        if (n > 64){
            throw new IllegalArgumentException("n can't be larger than 64 (amount of bits of a Long)!");
        }
        this.width = width;
        this.height = height;
        this.seeds = new BitSet[height];
        hashFunctions = new EfficientH3Functions(height, seed);
        eh3_array = new EH3_HashFunction[height];
        array = new int[height][width];

        // initialize the EH3 HashFunctions
        computeSeeds(seed);
        for (int i = 0; i < height; i++) {
            eh3_array[i] = new EH3_HashFunction(seeds[i], n);
        }
    }

    /**
     * private function which computes the seeds for the hash functions
     * @param seed
     */
    private void computeSeeds(long seed){
        XORShiftRandom random = new XORShiftRandom(seed);
        int length = (int)Math.ceil(n/8d);
        byte[] byteArray = new byte[length];
        for (int i = 0; i < height; i++) {
            random.nextBytes(byteArray);
            seeds[i] = BitSet.valueOf(byteArray);
            seeds[i].clear(n+1, n+8); // make sure the seeds are of size n+1 by clearing the overflow bits
        }
    }



    /**
     * Updates the sketch with the given element.
     * This means the it is assumed that the frequency of the given value is increased by 1.
     * @param element new incoming element
     */
    @Override
    public void update(Object element) {
        update(element, true);
    }

    /**
     * Either increments or decrements the given frequency value by 1.
     * This corresponds to either adding a value or removing a tuple of the given attribute value.
     * @param element       attribute value which should either be increased or decreased
     *                      needs to be a Number of Type BitSet, Integer (n == 32), Double (n == 64)
     *                      - otherwise the hashcode of arbitrary objects is taken (n == 32)
     * @param increment     true if sketch should be incremented by 1 for given element, false if frequency should be decreased by 1
     */
    public void update(Object element, boolean increment) {
        // make sure input element is converted to BitSet of correct length - otherwise throw exception
        BitSet input = new BitSet(n);
        int f;
        if(element instanceof Integer){
            f = (int) element;
            long[] a = {(long)f};
            input = BitSet.valueOf(a);

        }else {
            f = element.hashCode();
            long[] a = {(long)f};
            input = BitSet.valueOf(a);
        }

        int position;
        int[] hashValues = hashFunctions.generateHash(f);
        for (int i = 0; i < height; i++) {
            position = Math.abs(hashValues[i] % width); // compute the bucket position
            boolean b = eh3_array[i].rand(input);
            int addition = (increment && b) || (!increment && !b) ? 1 : -1;
            array[i][position] += addition;
        }
    }

    public int[][] getArray() {
        return array;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public byte getN() {
        return n;
    }

    public BitSet[] getSeeds() {
        return seeds;
    }

    @Override
    public FastAMS merge(Synopsis other) throws IllegalArgumentException {
        if (other instanceof FastAMS){
            FastAMS o = (FastAMS) other;
            if (width == o.getWidth() && height == o.getHeight() && n == o.getN() && Arrays.equals(seeds, o.getSeeds())){
                int[][] o_array = ((FastAMS) other).getArray();
                for (int i = 0; i < height; i++) {
                    for (int j = 0; j < width; j++) {
                        array[i][j] += o_array[i][j];
                    }
                }
                return this;
            }else {
                throw new IllegalArgumentException("Sketches have to be of same height, width, n and seeds / hash_functions to be merged");
            }
        }else {
            throw new IllegalArgumentException("sketch can only be merged with other sketches of the same class");
        }
    }

    /**
     * Gives the maximum error of the sketch in its current state with a bounded probability (see @errorProbability()).
     * The error is bounded by the F2 value and the sketch width.
     * @return  the maximum error of the F2 estimate with a bounded probability
     */
    public double errorBound(){
        return estimateF2() / Math.sqrt(width);
    }

    /**
     * Method which computes the probability of the error Bound in it's current state.
     * The sketch gives with probability 1-return a bad estimate (!)
     * @return  the probability in which the sketch estimate lies within the error bound!
     */
    public double errorProbability(){
        return 1d/Math.pow(2, height);
    }

    /**
     * Method which returns an estimate of the value of the F2 of the frequency vector which equals the F2 norm.
     * This is the sum of the squares of the frequency vector.
     * In a database context this equates to the self-join size of the relation whose frequency distribution on the join attribute is f.
     * @return  the F2 norm
     */
    public long estimateF2(){

        long[] f2_array = new long[height];

        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                f2_array[i] += (long)Math.pow(array[i][j],2);
            }
        }

        Arrays.sort(f2_array);

        return f2_array[height/2];  // median of the sorted array of estimated F2 values
    }


    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(array);
        out.writeInt(width);
        out.writeInt(height);
        out.writeObject(hashFunctions);
        out.writeObject(eh3_array);
        out.writeObject(seeds);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        array = (int[][])in.readObject();
        width = in.readInt();
        height = in.readInt();
        hashFunctions = (EfficientH3Functions) in.readObject();
        eh3_array = (EH3_HashFunction[]) in.readObject();
        seeds = (BitSet[]) in.readObject();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
