package Sketches.HashFunctions;

import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;
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
    private final byte H = 32;  // length in bits of the generated HashValues
    private byte n;
    private int numFunctions;

    Logger logger = LoggerFactory.getLogger(H3_HashFunctions.class);


    public H3_HashFunctions(int numFunctions, byte numInputBits, long seed) {
        this.n = numInputBits;
        if (n > 64){
            throw new IllegalArgumentException("input size n can't be larger than 64 bits (#bits of a Long)!");
        }

        this.numFunctions = numFunctions;
        q_matrices = new BitSet[numFunctions][n]; // initialize numFunctions, n * H matrices
        XORShiftRandom random = new XORShiftRandom(seed);

        for (int a = 0; a < numFunctions; a++) {
            for (int i = 0; i < n; i++) {
                byte[] byteArray = new byte[4];
                random.nextBytes(byteArray);
                q_matrices[a][i] = BitSet.valueOf(byteArray);
            }
        }
    }

    /**
     * Generates Hash values with size H
     * @param input value which is used to generate the hashes
     * @return  Hash values as Long Array
     */
    public int[] generateHash(BitSet input){
        if (input.isEmpty()){
            input.set(0); // if input is zero set it to an arbitrary value in order to not have it mapped to zero
        }
        BitSet[] sets = new BitSet[numFunctions];
        int[] result = new int[numFunctions];
        for (int i = 0; i < numFunctions; i++) {
            sets[i] = new BitSet(H);
            for (int j = 0; j < n; j++) {
                if (input.get(j)){
                    sets[i].xor(q_matrices[i][j]);    // XOR the row j of q-matrix the input bit is set to 1 at position j for each q-matrix
                }
            }
            int current;
            if (sets[i].toLongArray().length == 0){
                logger.warn("very unlikely that this event occurs!!! -> all 32 bits of the hash value are 0");
                current = 0;
            }else {
                current = (int)(sets[i].toLongArray()[0]);
            }
            result[i] = current;
        }

        return result;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(q_matrices);
        out.writeByte(n);
        out.writeInt(numFunctions);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        q_matrices = (BitSet[][]) in.readObject();
        n = in.readByte();
        numFunctions = in.readInt();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
