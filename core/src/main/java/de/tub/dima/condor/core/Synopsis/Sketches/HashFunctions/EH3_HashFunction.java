package de.tub.dima.condor.core.Synopsis.Sketches.HashFunctions;

import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.BitSet;

/**
 * Implementation of the EH3_HashFunction random bit generator
 *
 * @author joschavonhein
 */
public class EH3_HashFunction implements Serializable {
    private long[] seeds;
    private int height;

    /**
     * @param seed      seed for the random number generator
     * @param height    the amount of eh3 hash_functions
     */
    public EH3_HashFunction(Long seed, int height) {
        this.height = height;
        seeds= new long[height];
        // initialize seeds
        XORShiftRandom random = new XORShiftRandom(seed);
        for (int i = 0; i < height; i++) {
            seeds[i] = random.nextLong();
            seeds[i] >>>= 31; // make sure the seeds are exactly 32 + 1 bits long
        }
    }

    /**
     * Random number generator. Returns either true or false, depending on input and seed.
     * Based on the EH3_HashFunction scheme.
     * @param input bits
     * @return  true =1 , false = 0
     */
    public boolean[] rand(int input){
//TODO: Wrong indices for the h function.........
        long longInput = input + (1 << 32); // [1,input] concatenation -> longInput has exactly 32 + 1 bits
        boolean[] results = new boolean[height];

        for (int i = 0; i < height; i++) {
            long temp = longInput;
            long h = 1 & temp;
            for (int j = 1; j < 33; j++) {
                if (i % 2 == 0){
                    h = h ^ (1 & (temp >> 1));
                }else {
                    h = h | (1 & (temp >> 1));
                }
            }

            long second = longInput & seeds[i];
            int result = Long.bitCount(second) % 2;    // equal to the sequential XOR of the (longInput & seeds) bits
            results[i] = (result ^ (int) h) == 1;   // XOR between S*[1,i] and h(i)
        }
        return results;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(seeds);
        out.writeInt(height);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        seeds = (long[]) in.readObject();
        height = in.readInt();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
