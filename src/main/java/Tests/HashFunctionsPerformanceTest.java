package Tests;

import Sketches.HashFunctions.H3_HashFunctions;
import Sketches.HashFunctions.PairwiseIndependentHashFunctions;

import java.util.BitSet;

public class HashFunctionsPerformanceTest {

    public static void main(String[] args){

        H3_HashFunctions h3_hashFunctions = new H3_HashFunctions(32, (byte)32, 1000l);
        PairwiseIndependentHashFunctions pairwiseIndependentHashFunctions = new PairwiseIndependentHashFunctions(32, 1000l);
        final int ITERATIONS = 10000000;
        int[] hashes = new int[32];
        BitSet[] inputs = new BitSet[ITERATIONS];
        for (int i = 0; i < ITERATIONS; i++) {
            long[] l = {i};
            inputs[i] = BitSet.valueOf(l);
        }

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            hashes = h3_hashFunctions.generateHash(inputs[i]);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("H3 HashFunctions: "+ (endTime - startTime) + " hashes[0] = " + hashes[0]);



        startTime = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            hashes = pairwiseIndependentHashFunctions.hash(i);
        }
        endTime = System.currentTimeMillis();
        System.out.println("PairwiseIndependent HashFunctions: "+ (endTime - startTime) + " hashes[0] = " + hashes[0]);
    }
}
