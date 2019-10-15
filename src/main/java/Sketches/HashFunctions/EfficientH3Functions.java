package Sketches.HashFunctions;

import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;

public class EfficientH3Functions {
    private int[][] q_matrices;
    private int numFunctions;

    Logger logger = LoggerFactory.getLogger(EfficientH3Functions.class);


    public EfficientH3Functions(int numFunctions, long seed) {
        this.numFunctions = numFunctions;
        q_matrices = new int[numFunctions][32]; // initialize numFunctions, n * H matrices
        XORShiftRandom random = new XORShiftRandom(seed);

        for (int a = 0; a < numFunctions; a++) {
            for (int i = 0; i < 32; i++) {
                q_matrices[a][i] = random.nextInt();   // set the q_matrices
            }
        }
    }

    /**
     * Generates Hash values with size H
     * @param input value which is used to generate the hashes - should not be 0!
     * @return  Hash values as Long Array
     */
    public int[] generateHash(int input){
        input = input == 0 ? 1 : input; // input must not be 0
        int[] result = new int[numFunctions];
        for (int i = 0; i < numFunctions; i++) {
            int current = 0;
            for (int j = 0; j < 32; j++) {
                current = ((1 & input) * current) ^ q_matrices[i][j];
                input >>>= 1;
            }
            result[i] = current;
        }
        return result;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(q_matrices);
        out.writeInt(numFunctions);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        q_matrices = (int[][]) in.readObject();
        numFunctions = in.readInt();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
