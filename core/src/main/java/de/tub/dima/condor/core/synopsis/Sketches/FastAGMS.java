package de.tub.dima.condor.core.synopsis.Sketches;

import de.tub.dima.condor.core.synopsis.Sketches.HashFunctions.EH3_HashFunction;
import de.tub.dima.condor.core.synopsis.Sketches.HashFunctions.EfficientH3Functions;
import de.tub.dima.condor.core.synopsis.InvertibleSynopsis;
import de.tub.dima.condor.core.synopsis.MergeableSynopsis;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsis;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.io.NotSerializableException;
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
public class FastAGMS<T> extends StratifiedSynopsis implements InvertibleSynopsis<T>, Serializable {

    private int[][] array;
    private int width;
    private int height;
    private EfficientH3Functions hashFunctions;
    private EH3_HashFunction eh3_boolean_hashfunctions;
    private final byte n = 32;
    long seed;


    @Override
    public String toString() {
        String arrayString = "";
        for (int i = 0; i < height; i++) {
            arrayString += Arrays.toString(array[i]) + "\n";
        }
        return "FastAGMS{" +
                ", width=" + width +
                ", height=" + height +
                ", n=" + n +
                "\n Array: " + arrayString +
                '}';
    }

    /**
     * Constructs a FastAGMS Sketch Object.
     *
     * @param width     amount of buckets in each row - it is recommended to use powers of 2
     * @param height    amount of hash functions / rows in the sketch array
     * @param seed      seed for the RandomNumber Generator
     */
    public FastAGMS(Integer width, Integer height, Long seed) {
        this.seed = seed;
        this.width = width;
        this.height = height;
        hashFunctions = new EfficientH3Functions(height, seed);
        eh3_boolean_hashfunctions = new EH3_HashFunction(seed, height);
        array = new int[height][width];
    }

    /**
     * Constructer which uses a random seed - otherwise equal
     * @param width
     * @param height
     */
    public FastAGMS(Integer width, Integer height){
        XORShiftRandom random = new XORShiftRandom();
        long seed = random.nextLong();
        this.seed = seed;
        this.width = width;
        this.height = height;
        hashFunctions = new EfficientH3Functions(height, seed);
        eh3_boolean_hashfunctions = new EH3_HashFunction(seed, height);
        array = new int[height][width];
    }

    /**
     * Either increments or decrements the given frequency value by 1.
     * This corresponds to either adding a value or removing a tuple of the given attribute value.
     * @param element       attribute value which should either be increased or decreased
     *                      needs to be a Number of Type BitSet, Integer (n == 32), Double (n == 64)
     *                      - otherwise the hashcode of arbitrary objects is taken (n == 32)
     * @param increment     true if sketch should be incremented by 1 for given element, false if frequency should be decreased by 1
     */
    private void update(Object element, boolean increment) {
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
        int[] hashValues = hashFunctions.hash(f);
        boolean[] booleans = eh3_boolean_hashfunctions.rand(f);
        for (int i = 0; i < height; i++) {
            position = Math.abs(hashValues[i] % width); // compute the bucket position
            boolean b = booleans[i];
            int addition = (increment && b) || (!increment && !b) ? 1 : -1;
            array[i][position] += addition;
        }
    }

    @Override
    public void update(T element){
        update(element, true);
    }

    @Override
    public void decrement(T toDecrement) {
        update(toDecrement, false);
    }


    @Override
    public InvertibleSynopsis<T> invert(InvertibleSynopsis<T> toRemove) {
        if (toRemove instanceof FastAGMS){
            if (seed == ((FastAGMS<Object>) toRemove).seed && height == ((FastAGMS<Object>) toRemove).getHeight() && width == ((FastAGMS<Object>) toRemove).getWidth()){
                int[][] toRemoveArray = ((FastAGMS<Object>) toRemove).getArray();
                for (int i = 0; i < height; i++) {
                    for (int j = 0; j < width; j++) {
                        array[i][j] -= toRemoveArray[i][j]; // subtract the value in the array of the sketch to be removed
                    }
                }
                return this;
            }
            throw new IllegalArgumentException("Fast AMS Sketch to be removed needs to be the same size and use the same hash functions (seed has to be equal)!");
        }
        throw new IllegalArgumentException("MergeableSynopsis.Sketches to merge have to be the same size and hash Functions");
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

    public EfficientH3Functions getHashFunctions() {
        return hashFunctions;
    }

    public EH3_HashFunction getEh3_boolean_hashfunctions() {
        return eh3_boolean_hashfunctions;
    }

    public long getSeed() {
        return seed;
    }

    @Override
    public FastAGMS<T> merge(MergeableSynopsis other){
        if (other instanceof FastAGMS){
            FastAGMS o = (FastAGMS) other;
            if (width == o.getWidth() && height == o.getHeight() && seed == o.getSeed()){
                int[][] o_array = ((FastAGMS) other).getArray();
                for (int i = 0; i < height; i++) {
                    for (int j = 0; j < width; j++) {
                        array[i][j] += o_array[i][j];
                    }
                }
                return this;
            }else {
                throw new IllegalArgumentException("MergeableSynopsis.Sketches have to be of same height, width, n and seeds / hash_functions to be merged");
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
        out.writeObject(eh3_boolean_hashfunctions);
        out.writeLong(seed);
        out.writeObject(this.getPartitionValue());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        array = (int[][])in.readObject();
        width = in.readInt();
        height = in.readInt();
        hashFunctions = (EfficientH3Functions) in.readObject();
        eh3_boolean_hashfunctions = (EH3_HashFunction) in.readObject();
        seed = in.readLong();
        this.setPartitionValue(in.readObject());
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }
}
