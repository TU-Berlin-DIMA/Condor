package Sketches.HashFunctions;

import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.BitSet;

/**
 * implementation of the H3 hash function which delivers a highly even distribution of hash keys to hash values.
 * Based on the explanation in the paper "Sketch Acceleration on FPGA and its Applications in Network Anomaly Detection.
 *
 * @author joschavonhein
 */
public class H3_HashFunctions implements Serializable {
    private BitSet[][] q_matrices;
    private final byte H = 64;  // length in bits of the generated HashValues
    private byte n;
    private int numFunctions;
    private BitSet[] seeds; // seeds for the EH3_HashFunction Hash Functions used to fill the q_matrices - 17 bits length

    Logger logger = LoggerFactory.getLogger(H3_HashFunctions.class);



    public H3_HashFunctions(int numFunctions, byte numInputBits, long seed) {
        if (n > 64){
            throw new IllegalArgumentException("input size n can't be larger than 64 bits (#bits of a Long)!");
        }

        this.n = numInputBits;
        this.numFunctions = numFunctions;
        q_matrices = new BitSet[numFunctions][n]; // initialize numFunctions, n * H matrices
        seeds = new BitSet[numFunctions];
        computeSeeds(seed);

        EH3_HashFunction eh3;
        for (int a = 0; a < numFunctions; a++) {
            eh3 = new EH3_HashFunction(seeds[a], (byte) 16);

            for (int i = 0; i < n; i++) {
                for (int j = 0; j < H; j++) {
                    byte[] array = new byte[2];
                    array[0] = (byte)i;
                    array[1] = (byte)j;
                    q_matrices[a][i].set(j, eh3.rand(BitSet.valueOf(array)));    // initiate the q-matrices randomly with either 0 or 1
                }
            }
        }
    }

    /**
     * private function which computes the seeds for the hash functions
     * @param seed
     */
    private void computeSeeds(long seed){
        XORShiftRandom random = new XORShiftRandom(seed);
        byte[] byteArray = new byte[3];
        for (int i = 0; i < numFunctions; i++) {
            random.nextBytes(byteArray);
            seeds[i] = BitSet.valueOf(byteArray);
            seeds[i].clear(17, 24); // make sure the seeds are 17 bits long
        }
    }

    /**
     * Generates Hash values with size H
     * @param input value which is used to generate the hashes
     * @return  Hash values as Long Array
     */
    public long[] generateHash(BitSet input){
        BitSet[] sets = new BitSet[numFunctions];
        long[] result = new long[numFunctions];
        for (int i = 0; i < numFunctions; i++) {
            sets[i] = new BitSet(H);
            for (int j = 0; j < n; j++) {
                if (input.get(j)){
                    sets[i].xor(q_matrices[i][j]);    // XOR the row j of q-matrix the input bit is set to 1 at position j for each q-matrix
                }
            }
            result[i] = sets[i].toLongArray()[0];
        }

        return result;
    }
}
