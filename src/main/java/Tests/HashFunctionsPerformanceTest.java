package Tests;

import Sketches.HashFunctions.EfficientH3Functions;
import Sketches.HashFunctions.PairwiseIndependentHashFunctions;


public class HashFunctionsPerformanceTest {

    public static void main(String[] args){

        EfficientH3Functions temp = new EfficientH3Functions(1, 2);

        String s = "";
        for (int i = 0; i < 100; i++) {
            s += temp.generateHash(i)[0] + "\n";
        }
        System.out.println(s);



        EfficientH3Functions h3_hashFunctions = new EfficientH3Functions(32, 1000l);
        PairwiseIndependentHashFunctions pairwiseIndependentHashFunctions = new PairwiseIndependentHashFunctions(32, 1000l);
        final int ITERATIONS = 1000000;
        int[] hashes = new int[32];

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            hashes = pairwiseIndependentHashFunctions.hash(i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("PairwiseIndependent HashFunctions: "+ (endTime - startTime) + " hashes[0] = " + hashes[0]);

        startTime = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            hashes = h3_hashFunctions.generateHash(i);
        }
        endTime = System.currentTimeMillis();
        System.out.println("H3 HashFunctions: "+ (endTime - startTime) + " hashes[0] = " + hashes[0]);

    }
}
