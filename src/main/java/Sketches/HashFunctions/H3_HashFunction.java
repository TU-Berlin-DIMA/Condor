package Sketches.HashFunctions;

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
public class H3_HashFunction implements Serializable {
    private BitSet[] q_matrix;
    private final byte H = 64;  // length in bits of the generated HashValues
    private byte n;

    Logger logger = LoggerFactory.getLogger(H3_HashFunction.class);



    public H3_HashFunction(byte n, BitSet seed) {
        if (n > 64){
            throw new IllegalArgumentException("input size n can't be larger than 64 bits (#bits of a Long)!");
        }

        this.n = n;
        q_matrix = new BitSet[n];

        BitSet eh3_seed = (BitSet) seed.clone();
        eh3_seed.clear(17, 64); // make sure the seed is length 17 by clearing the bits which are possibly set
        EH3 eh3 = new EH3(eh3_seed, (byte) 16);

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < H; j++) {
                byte[] array = new byte[2];
                array[0] = (byte)i;
                array[1] = (byte)j;
                q_matrix[i].set(j, eh3.rand(BitSet.valueOf(array)));    // initiate the matrix randomly either with 0 or 1 depending on the seed
            }
        }
    }

    /**
     * Generates a Hash value with size H
     * @param input value which is used to generate the hash
     * @return  Hash value as Long
     */
    public long generateHash(BitSet input){
        BitSet result = new BitSet(H);
        for (int i = 0; i < n; i++) {
            if (input.get(i)){
                result.xor(q_matrix[i]);    // XOR the row i of matrix q when the input bit is set to 1 at position i
            }
        }
        return result.toLongArray()[0];
    }
}
